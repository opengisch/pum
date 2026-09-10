from pathlib import Path
import psycopg
import yaml
import packaging
import packaging.version
from pydantic import ValidationError
import logging
from typing import TYPE_CHECKING

from .dependency_handler import DependencyHandler, prefix_site_packages
from .exceptions import PumConfigError, PumException, PumHookError, PumInvalidChangelog, PumSqlError
from .parameter import ParameterDefinition
from .role_manager import RoleManager
from .config_model import ConfigModel
from .hook import HookHandler
from ._version import VERSION as PUM_VERSION  # re-exported for backward compatibility
import hashlib
import importlib
import os
import re
import sys
import sysconfig
from collections import Counter


if TYPE_CHECKING:
    from .changelog import Changelog


logger = logging.getLogger(__name__)

# sys.path entries added for dependencies, counted per live PumConfig so that one
# config being garbage collected does not pull the path from under another.
_dependency_path_users: Counter = Counter()


def _user_cache_dir() -> Path:
    """Return the per-user cache directory for pum.

    `PUM_CACHE_DIR` overrides it, which keeps test runs and sandboxed
    environments out of the real user cache.
    """
    override = os.environ.get("PUM_CACHE_DIR")
    if override:
        return Path(override)
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local"
        return Path(base) / "pum" / "Cache"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "pum"
    return Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "pum"


def _path_safe(name: str) -> str:
    """Reduce `name` to a single, harmless path component.

    The module name comes from the configuration file, so it may hold separators
    or `..` that would place the cache outside its directory.
    """
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name).strip("._-")
    return safe or "module"


def _exception_chain_text(exc: BaseException) -> str:
    """Concatenate str() of all exceptions in the cause chain."""
    parts = []
    while exc:
        parts.append(str(exc))
        exc = exc.__cause__
    return " ".join(parts)


class PumConfig:
    """A class to hold configuration settings.

    Version Added:
        1.0.0
    """

    def __init__(
        self,
        base_path: str | Path,
        *,
        validate: bool = True,
        install_dependencies: bool = False,
        **kwargs: dict,
    ) -> None:
        """Initialize the configuration with key-value pairs.

        Args:
            base_path: The directory where the changelogs are located.
            validate: Whether to validate the changelogs and hooks and resolve dependencies. Defaults to True.
            install_dependencies: Whether to install missing dependencies into a cache directory.
            **kwargs: Key-value pairs representing configuration settings.

        Raises:
            PumConfigError: If the configuration is invalid.

        """

        if not isinstance(base_path, Path):
            base_path = Path(base_path)
        if not base_path.is_dir():
            raise PumConfigError(f"Directory `{base_path}` does not exist.")
        self._base_path = base_path

        self.dependency_path = None
        self._dependency_sys_paths = []  # sys.path entries added for dependencies
        self._cached_handlers = []  # Cache handlers for cleanup

        try:
            self.config = ConfigModel(**kwargs)
        except ValidationError as e:
            logger.error("Config validation error: %s", e)
            raise PumConfigError(e) from e

        if validate:
            if self.config.pum.minimum_version and PUM_VERSION < self.config.pum.minimum_version:
                raise PumConfigError(
                    f"Minimum required version of pum is {self.config.pum.minimum_version}, but the current version is {PUM_VERSION}. Please upgrade pum."
                )
            try:
                self.validate(install_dependencies=install_dependencies)
            except (PumInvalidChangelog, PumHookError) as e:
                raise PumConfigError(
                    f"Configuration is invalid: {e}. You can disable the validation when constructing the config."
                ) from e

    @classmethod
    def from_yaml(
        cls,
        file_path: str | Path,
        *,
        validate: bool = True,
        install_dependencies: bool = False,
    ) -> "PumConfig":
        """Create a PumConfig instance from a YAML file.

        Args:
            file_path: The path to the YAML file.
            validate: Whether to validate the changelogs and hooks.
            install_dependencies: Whether to install missing dependencies into a cache directory.

        Returns:
            PumConfig: An instance of the PumConfig class.

        Raises:
            FileNotFoundError: If the file does not exist.
            yaml.YAMLError: If there is an error parsing the YAML file.

        """
        with Path.open(file_path) as file:
            data = yaml.safe_load(file)

        if "base_path" in data:
            raise PumConfigError("base_path not allowed in configuration instead.")

        base_path = Path(file_path).parent
        return cls(
            base_path=base_path,
            validate=validate,
            install_dependencies=install_dependencies,
            **data,
        )

    @property
    def base_path(self) -> Path:
        """Return the base path used for configuration and changelogs."""
        return self._base_path

    def cleanup_hook_imports(self) -> None:
        """Clean up imported modules and sys.path entries from hooks.

        This should be called when switching to a different module version to ensure
        that cached imports from the previous version don't cause conflicts.
        """
        # First, clean up sys.path additions from all cached handlers
        for handler in self._cached_handlers:
            handler.cleanup_sys_path()

        # Clear all modules that were loaded from this base_path
        base_path_str = str(self._base_path.resolve())
        modules_to_remove = []

        for module_name, module in list(sys.modules.items()):
            if module is None:
                continue
            module_file = getattr(module, "__file__", None)
            if module_file and module_file.startswith(base_path_str):
                modules_to_remove.append(module_name)

        for module_name in modules_to_remove:
            if module_name in sys.modules:
                logger.debug(f"Removing cached module: {module_name}")
                del sys.modules[module_name]

        self._cached_handlers.clear()

    def parameters(self) -> list[ParameterDefinition]:
        """Return a list of migration parameters.

        Returns:
            list[ParameterDefinition]: A list of migration parameter definitions.

        """
        return [
            ParameterDefinition(**parameter.model_dump(mode="python"))
            for parameter in self.config.parameters
        ]

    def parameter(self, name: str) -> ParameterDefinition:
        """Get a specific migration parameter by name.

        Args:
            name: The name of the parameter.

        Returns:
            ParameterDefintion: The migration parameter definition.

        Raises:
            PumConfigError: If the parameter name does not exist.

        """
        for parameter in self.config.parameters:
            if parameter.name == name:
                return ParameterDefinition(**parameter.model_dump(mode="python"))
        raise PumConfigError(f"Parameter '{name}' not found in configuration.") from None

    def last_version(
        self, min_version: str | None = None, max_version: str | None = None
    ) -> str | None:
        """Return the last version of the changelogs.
        The changelogs are sorted by version.

        Args:
            min_version (str | None): The version to start from (inclusive).
            max_version (str | None): The version to end at (inclusive).

        Returns:
            str | None: The last version of the changelogs. If no changelogs are found, None is returned.

        """
        changelogs = self.changelogs(min_version, max_version)
        if not changelogs:
            return None
        return changelogs[-1].version

    def changelogs(
        self,
        min_version: str | packaging.version.Version | None = None,
        max_version: str | packaging.version.Version | None = None,
    ) -> "list[Changelog]":
        """Return a list of changelogs.
        The changelogs are sorted by version.

        Args:
            min_version (str | None): The version to start from (inclusive).
            max_version (str | None): The version to end at (inclusive).

        Returns:
            list: A list of changelogs. Each changelog is represented by a Changelog object.

        """
        path = self._base_path / self.config.changelogs_directory
        if not path.is_dir():
            raise PumException(f"Changelogs directory `{path}` does not exist.")
        if not any(path.iterdir()):
            raise PumException(f"Changelogs directory `{path}` is empty.")

        # Local import avoids circular imports at module import time.
        from .changelog import Changelog

        changelogs = [Changelog(d) for d in path.iterdir() if d.is_dir()]

        if min_version:
            changelogs = [
                c for c in changelogs if c.version >= packaging.version.parse(min_version)
            ]
        if max_version:
            changelogs = [
                c for c in changelogs if c.version <= packaging.version.parse(max_version)
            ]

        changelogs.sort(key=lambda c: c.version)
        return changelogs

    def role_manager(self) -> RoleManager:
        """Return a RoleManager instance based on the roles defined in the configuration."""
        if not self.config.roles:
            logger.warning("No roles defined in the configuration. Returning an empty RoleManager.")
            return RoleManager([])
        return RoleManager([role.model_dump() for role in self.config.roles])

    def drop_app_handlers(self) -> list[HookHandler]:
        """Return the list of drop app hook handlers."""
        handlers = (
            [
                HookHandler(base_path=self._base_path, **hook.model_dump())
                for hook in self.config.application.drop
            ]
            if self.config.application.drop
            else []
        )
        # Cache handlers for cleanup
        self._cached_handlers.extend(handlers)
        return handlers

    def create_app_handlers(self) -> list[HookHandler]:
        """Return the list of create app hook handlers."""
        handlers = (
            [
                HookHandler(base_path=self._base_path, **hook.model_dump())
                for hook in self.config.application.create
            ]
            if self.config.application.create
            else []
        )
        # Cache handlers for cleanup
        self._cached_handlers.extend(handlers)
        return handlers

    def uninstall_handlers(self) -> list[HookHandler]:
        """Return the list of uninstall hook handlers."""
        return (
            [
                HookHandler(base_path=self._base_path, **hook.model_dump())
                for hook in self.config.uninstall
            ]
            if self.config.uninstall
            else []
        )

    def demo_data(self) -> dict[str, list[str]]:
        """Return a dictionary of demo data files defined in the configuration."""
        demo_data_files = {}
        for dm in self.config.demo_data:
            demo_data_files[dm.name] = dm.files or [dm.file]
        return demo_data_files

    def _dependency_cache_path(self) -> Path:
        """Return the pip prefix caching this configuration's dependencies.

        The key covers everything the installed content depends on, so a change of
        dependency, interpreter or platform gets its own prefix. The directory is
        kept across runs: reinstalling on every configuration load is slow, and it
        would download the dependency again on each module switch.
        """
        key = "\n".join(
            sorted(
                f"{d.name}|{d.minimum_version or ''}|{d.maximum_version or ''}"
                for d in self.config.dependencies
            )
            + [
                f"python|{sys.version_info.major}.{sys.version_info.minor}",
                f"platform|{sysconfig.get_platform()}",
            ]
        )
        digest = hashlib.sha256(key.encode()).hexdigest()[:16]
        name = _path_safe(self.config.pum.module)
        return _user_cache_dir() / "dependencies" / f"{name}-{digest}"

    def _add_dependency_sys_paths(self, prefix: Path) -> None:
        """Make the dependencies installed under `prefix` importable.

        Idempotent, and meant to be called again after every install: which
        site-packages directories pip creates depends on the install scheme, so
        they can only be discovered once they exist on disk.
        """
        for path in reversed(prefix_site_packages(prefix)):
            if path in self._dependency_sys_paths:
                continue
            self._dependency_sys_paths.append(path)
            _dependency_path_users[path] += 1
            if path not in sys.path:
                sys.path.insert(0, path)
        # A sys.path entry that did not exist when it was first searched is
        # negatively cached in sys.path_importer_cache until the caches are
        # invalidated; this also lets importlib.metadata see a fresh install.
        importlib.invalidate_caches()

    def __del__(self):
        # Cleanup sys.path modifications. The cache directory itself is kept.
        # Guarded: at interpreter shutdown the module globals may already be gone.
        try:
            for path in getattr(self, "_dependency_sys_paths", ()):
                _dependency_path_users[path] -= 1
                if _dependency_path_users[path] <= 0:
                    del _dependency_path_users[path]
                    if sys.path:
                        sys.path = [p for p in sys.path if p != path]
            self._dependency_sys_paths = []
        except Exception:
            pass

    def validate(self, install_dependencies: bool = False) -> None:
        """Validate the changelogs and hooks.

        Args:
            install_dependencies (bool): Whether to install missing dependencies into a cache directory.
        """

        if install_dependencies and self.config.dependencies:
            self.dependency_path = self._dependency_cache_path()
            self.dependency_path.mkdir(parents=True, exist_ok=True)
            # Added before resolving, so that a dependency already in the cache
            # is found and not installed again.
            self._add_dependency_sys_paths(self.dependency_path)

        parameter_defaults = {}
        app_only_parameter_names = set()
        for parameter in self.config.parameters:
            parameter_defaults[parameter.name] = psycopg.sql.Literal(parameter.default)
            if parameter.app_only:
                app_only_parameter_names.add(parameter.name)

        for dependency in self.config.dependencies:
            DependencyHandler(**dependency.model_dump()).resolve(
                install_dependencies=install_dependencies, install_path=self.dependency_path
            )
            if self.dependency_path:
                # pip has only now created the site-packages directories, and the
                # next dependency must be able to see what this one pulled in.
                self._add_dependency_sys_paths(self.dependency_path)

        # Validate changelogs with only non-app_only parameters.
        # app_only parameters must not be used in changelogs (migrations),
        # they are only allowed in application hooks.
        changelog_parameters = {
            k: v for k, v in parameter_defaults.items() if k not in app_only_parameter_names
        }
        for changelog in self.changelogs():
            try:
                changelog.validate(parameters=changelog_parameters)
            except (PumInvalidChangelog, PumSqlError) as e:
                # Check if the error is due to an app_only parameter being used
                error_text = _exception_chain_text(e)
                for name in app_only_parameter_names:
                    if name in error_text:
                        raise PumInvalidChangelog(
                            f"Changelog `{changelog}` uses app_only parameter `{name}`. "
                            f"App-only parameters cannot be used in changelogs (migrations), "
                            f"they are only allowed in application hooks (create/drop)."
                        ) from e
                raise PumInvalidChangelog(f"Changelog `{changelog}` is invalid.") from e

        hook_handlers = []
        if self.config.application.drop:
            hook_handlers.extend(self.drop_app_handlers())
        if self.config.application.create:
            hook_handlers.extend(self.create_app_handlers())
        for hook_handler in hook_handlers:
            try:
                hook_handler.validate(parameter_defaults)
            except PumHookError as e:
                raise PumHookError(f"Hook `{hook_handler}` is invalid.") from e
