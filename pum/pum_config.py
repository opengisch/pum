from pathlib import Path
import psycopg
import yaml
import packaging
from pydantic import ValidationError
import logging
import importlib.metadata
import glob
import os


from .changelog import Changelog
from .dependency_handler import DependencyHandler
from .exceptions import PumConfigError, PumException, PumHookError, PumInvalidChangelog, PumSqlError
from .parameter import ParameterDefinition
from .role_manager import RoleManager
from .config_model import ConfigModel
from .hook import HookHandler
import tempfile
import sys


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
        PUM_VERSION = max((packaging.version.Version(v) for v in versions))
    else:
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

    def parameters(self) -> list[ParameterDefinition]:
        """Return a list of migration parameters.

        Returns:
            list[ParameterDefinition]: A list of migration parameter definitions.

        """
        return [
            ParameterDefinition(**parameter.model_dump()) for parameter in self.config.parameters
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
                return ParameterDefinition(**parameter.model_dump())
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

    def changelogs(self, min_version: str | None = None, max_version: str | None = None) -> list:
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
            return RoleManager()
        return RoleManager([role.model_dump() for role in self.config.roles])

    def pre_hook_handlers(self) -> list[HookHandler]:
        """Return the list of pre-migration hook handlers."""
        return (
            [
                HookHandler(base_path=self._base_path, **hook.model_dump())
                for hook in self.config.migration_hooks.pre
            ]
            if self.config.migration_hooks.pre
            else []
        )

    def post_hook_handlers(self) -> list[HookHandler]:
        """Return the list of post-migration hook handlers."""
        return (
            [
                HookHandler(base_path=self._base_path, **hook.model_dump())
                for hook in self.config.migration_hooks.post
            ]
            if self.config.migration_hooks.post
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
        if self.config.migration_hooks.pre:
            hook_handlers.extend(self.pre_hook_handlers())
        if self.config.migration_hooks.post:
            hook_handlers.extend(self.post_hook_handlers())
        for hook_handler in hook_handlers:
            try:
                hook_handler.validate(parameter_defaults)
            except PumHookError as e:
                raise PumHookError(f"Hook `{hook_handler}` is invalid.") from e
