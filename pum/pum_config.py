from pathlib import Path
import psycopg
import yaml
import packaging
from pydantic import ValidationError
import logging
import importlib.metadata


from .changelog import Changelog
from .exceptions import PumConfigError, PumException, PumHookError, PumInvalidChangelog, PumSqlError
from .parameter import ParameterDefinition
from .role_manager import RoleManager
from .config_model import ConfigModel
from .hook import HookHandler


try:
    PUM_VERSION = packaging.version.Version(importlib.metadata.version("pum"))
except importlib.metadata.PackageNotFoundError:
    PUM_VERSION = packaging.version.Version("0.0.0")


logger = logging.getLogger(__name__)


class PumConfig(ConfigModel):
    """A class to hold configuration settings."""

    def __init__(self, base_path: str | Path, validate: bool = True, **kwargs: dict) -> None:
        """Initialize the configuration with key-value pairs.

        Args:
            base_path: The directory where the changelogs are located.
            validate: Whether to validate the changelogs and hooks.
            **kwargs: Key-value pairs representing configuration settings.

        Raises:
            PumConfigError: If the configuration is invalid.

        """
        # self.pg_restore_exe: str | None = kwargs.get("pg_restore_exe") or os.getenv(
        #     "PG_RESTORE_EXE"
        # )
        # self.pg_dump_exe: str | None = kwargs.get("pg_dump_exe") or os.getenv("PG_DUMP_EXE")

        if not isinstance(base_path, Path):
            base_path = Path(base_path)
        if not base_path.is_dir():
            raise PumConfigError(f"Directory `{base_path}` does not exist.")

        try:
            super().__init__(**kwargs)
            self.set_base_path(base_path=base_path)
        except ValidationError as e:
            logger.error("Config validation error: %s", e)
            raise PumConfigError(e)

        if validate:
            if self.pum.minimum_version and PUM_VERSION < self.pum.minimum_version:
                raise PumConfigError(
                    f"Minimum required version of pum is {self.pum.minimum_version}, but the current version is {PUM_VERSION}. Please upgrade pum."
                )
            try:
                self.validate()
            except (PumInvalidChangelog, PumHookError) as e:
                raise PumConfigError(
                    f"Configuration is invalid: {e}. You can disable the validation when constructing the config."
                ) from e

    @classmethod
    def from_yaml(cls, file_path: str | Path, *, validate: bool = True) -> "PumConfig":
        """Create a PumConfig instance from a YAML file.

        Args:
            file_path: The path to the YAML file.
            validate: Whether to validate the changelogs and hooks.

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
        return cls(base_path=base_path, validate=validate, **data)

    def parameter(self, name: str) -> ParameterDefinition:
        """Get a specific migration parameter by name.

        Args:
            name: The name of the parameter.

        Returns:
            ParameterDefintion: The migration parameter definition.

        Raises:
            PumConfigError: If the parameter name does not exist.

        """
        for parameter in self.parameters:
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
        changelogs = self.list_changelogs(min_version, max_version)
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

    def list_changelogs(
        self, min_version: str | None = None, max_version: str | None = None
    ) -> list:
        """Return a list of changelogs.
        The changelogs are sorted by version.

        Args:
            min_version (str | None): The version to start from (inclusive).
            max_version (str | None): The version to end at (inclusive).

        Returns:
            list: A list of changelogs. Each changelog is represented by a Changelog object.

        """
        path = self._base_path / self.changelogs_directory
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
        if not self.roles:
            logger.warning("No roles defined in the configuration. Returning an empty RoleManager.")
            return RoleManager()
        return RoleManager([role.model_dump() for role in self.roles])

    def pre_hook_handlers(self) -> list[HookHandler]:
        """Return the list of pre-migration hook handlers."""
        return (
            [
                HookHandler(base_path=self._base_path, **hook.model_dump())
                for hook in self.migration_hooks.pre
            ]
            if self.migration_hooks.pre
            else []
        )

    def post_hook_handlers(self) -> list[HookHandler]:
        """Return the list of post-migration hook handlers."""
        return (
            [
                HookHandler(base_path=self._base_path, **hook.model_dump())
                for hook in self.migration_hooks.post
            ]
            if self.migration_hooks.post
            else []
        )

    def validate(self) -> None:
        """Validate the chanbgelogs and hooks."""

        parameter_defaults = {}
        for parameter in self.parameters:
            parameter_defaults[parameter.name] = psycopg.sql.Literal(parameter.default)

        for changelog in self.list_changelogs():
            try:
                changelog.validate(parameters=parameter_defaults)
            except (PumInvalidChangelog, PumSqlError) as e:
                raise PumInvalidChangelog(f"Changelog `{changelog}` is invalid.") from e

        hook_handlers = []
        if self.migration_hooks.pre:
            hook_handlers.extend(self.pre_hook_handlers())
        if self.migration_hooks.post:
            hook_handlers.extend(self.post_hook_handlers())
        for hook_handler in hook_handlers:
            try:
                hook_handler.validate(parameter_defaults)
            except PumHookError as e:
                raise PumHookError(f"Hook `{hook_handler}` is invalid.") from e
