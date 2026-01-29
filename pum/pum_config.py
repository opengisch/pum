from pathlib import Path
import psycopg
import yaml
import packaging
import packaging.version
from pydantic import ValidationError
import logging
import importlib.metadata
import glob
import os
import subprocess
from typing import TYPE_CHECKING

from .dependency_handler import DependencyHandler
from .exceptions import PumConfigError, PumException, PumHookError, PumInvalidChangelog, PumSqlError
from .parameter import ParameterDefinition
from .role_manager import RoleManager
from .config_model import ConfigModel
from .hook import HookHandler
import tempfile
import sys


if TYPE_CHECKING:
    from .changelog import Changelog


try:
    PUM_VERSION = packaging.version.Version(importlib.metadata.version("pum"))
except importlib.metadata.PackageNotFoundError:
    # Fallback: try to read from pum-*.dist-info/METADATA
    dist_info_dirs = glob.glob(os.path.join(os.path.dirname(__file__), "..", "pum-*.dist-info"))
    versions = []
    for dist_info in dist_info_dirs:
        metadata_path = os.path.join(dist_info, "METADATA")
        if os.path.isfile(metadata_path):
            with open(metadata_path) as f:
                for line in f:
                    if line.startswith("Version:"):
                        version = line.split(":", 1)[1].strip()
                        versions.append(version)
                        break
    if versions:
        # Pick the highest version
        PUM_VERSION = max(packaging.version.Version(v) for v in versions)
    else:
        # Fallback: try to get version from git (for development from source)
        try:
            git_dir = Path(__file__).parent.parent / ".git"
            if git_dir.exists():
                result = subprocess.run(
                    ["git", "describe", "--tags", "--always", "--dirty"],
                    cwd=Path(__file__).parent.parent,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode == 0 and result.stdout.strip():
                    git_version = result.stdout.strip()
                    # Clean up git version to be PEP 440 compatible
                    # e.g., "0.9.2-10-g1234567" -> "0.9.2.post10"
                    # e.g., "0.9.2" -> "0.9.2"
                    # e.g., "1234567" (no tags) -> "0.0.0+1234567"
                    if "-" in git_version:
                        parts = git_version.split("-")
                        if len(parts) >= 3 and parts[0][0].isdigit():
                            # Tagged version with commits after: "0.9.2-10-g1234567"
                            base_version = parts[0]
                            commits_after = parts[1]
                            PUM_VERSION = packaging.version.Version(
                                f"{base_version}.post{commits_after}"
                            )
                        else:
                            # Untagged: just use the commit hash
                            PUM_VERSION = packaging.version.Version(f"0.0.0+{parts[0]}")
                    elif git_version[0].isdigit():
                        # Clean tag version
                        PUM_VERSION = packaging.version.Version(git_version)
                    else:
                        # Just a commit hash (no tags)
                        PUM_VERSION = packaging.version.Version(f"0.0.0+{git_version}")
                else:
                    PUM_VERSION = packaging.version.Version("0.0.0")
            else:
                PUM_VERSION = packaging.version.Version("0.0.0")
        except Exception:
            PUM_VERSION = packaging.version.Version("0.0.0")


logger = logging.getLogger(__name__)


class PumConfig:
    """A class to hold configuration settings."""

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
            install_dependencies: Whether to temporarily install dependencies.
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
            install_dependencies: Wheter to temporarily install dependencies.

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
        """Clean up imported modules from hooks to prevent conflicts when switching versions.

        This should be called when switching to a different module version to ensure
        that cached imports from the previous version don't cause conflicts.
        """
        for handler in self._cached_handlers:
            if hasattr(handler, "cleanup_imports"):
                handler.cleanup_imports()
        # Clear the cache after cleanup
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
        raise PumConfigError(f"Parameter '{name}' not found in configuration.") from KeyError

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
        if min_version:
            changelogs = [
                c for c in changelogs if c.version >= packaging.version.parse(min_version)
            ]
        if max_version:
            changelogs = [
                c for c in changelogs if c.version <= packaging.version.parse(max_version)
            ]
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
        if not path.iterdir():
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

    def __del__(self):
        # Cleanup temporary directories and sys.path modifications
        if self.dependency_path and sys.path:
            # Remove from sys.path if present
            sys.path = [p for p in sys.path if p != str(self.dependency_path)]
            # Remove the directory if it exists and is a TemporaryDirectory
            if hasattr(self, "_temp_dir") and self._temp_dir:
                self.dependency_tmp.cleanup()

    def validate(self, install_dependencies: bool = False) -> None:
        """Validate the changelogs and hooks.

        Args:
            install_dependencies (bool): Whether to temporarily install dependencies.
        """

        if install_dependencies and self.config.dependencies:
            self.dependency_tmp = tempfile.TemporaryDirectory()
            self.dependency_path = Path(self.dependency_tmp.name)
            sys.path.insert(0, str(self.dependency_path))

        parameter_defaults = {}
        for parameter in self.config.parameters:
            parameter_defaults[parameter.name] = psycopg.sql.Literal(parameter.default)

        for dependency in self.config.dependencies:
            DependencyHandler(**dependency.model_dump()).resolve(
                install_dependencies=install_dependencies, install_path=self.dependency_path
            )

        for changelog in self.changelogs():
            try:
                changelog.validate(parameters=parameter_defaults)
            except (PumInvalidChangelog, PumSqlError) as e:
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
