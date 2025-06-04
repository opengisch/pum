from pathlib import Path
import psycopg
import yaml
import packaging
from pydantic import BaseModel, Field, ValidationError, model_validator
from typing import List, Optional, Any, Literal
import logging
import importlib.metadata


from .changelog import Changelog
from .exceptions import PumConfigError, PumException, PumHookError, PumInvalidChangelog, PumSqlError
from .hook import HookHandler
from .parameter import ParameterDefinition, ParameterType

DIR = "."

try:
    PUM_VERSION = packaging.version.Version(importlib.metadata.version("pum"))
except importlib.metadata.PackageNotFoundError:
    PUM_VERSION = packaging.version.Version("0.0.0")


class ParameterDefinitionModel(BaseModel):
    name: str
    type: ParameterType = Field(default=ParameterType.TEXT, description="Type of the parameter")
    default: Optional[Any] = None
    description: Optional[str] = None


class HookModel(BaseModel):
    file: Optional[str] = None
    code: Optional[str] = None
    hook_handler: HookHandler = None

    model_config = {"arbitrary_types_allowed": True}

    @model_validator(mode="after")
    def build_handler(self):
        file, code = self.file, self.code
        if (file and code) or (not file and not code):
            raise ValueError("Exactly one of 'file' or 'code' must be set in a hook.")
        if file:
            path = Path(file)
            if not path.is_absolute():
                path = DIR / path
            if not path.exists():
                raise PumConfigError(f"hook file {path} does not exist")
            self.hook_handler = HookHandler(file=path)
        elif code:
            self.hook_handler = HookHandler(code=code)
        return self


class MigrationHooksModel(BaseModel):
    pre: Optional[List[HookModel]] = []
    post: Optional[List[HookModel]] = []


class PumModel(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    migration_table_schema: Optional[str] = Field(
        default="public", description="Name of schema for the migration table"
    )
    migration_table_name: Literal["pum_migrations"] = Field(default="pum_migrations")
    minimum_version: Optional[packaging.version.Version] = Field(
        default=None,
        description="Minimum required version of pum.",
    )

    @model_validator(mode="before")
    def parse_minimum_version(cls, values):
        min_ver = values.get("minimum_version")
        if isinstance(min_ver, str):
            values["minimum_version"] = packaging.version.Version(min_ver)
        return values


class PermissionModel(BaseModel):
    type: Literal["read", "write"] = Field(..., description="Permission type ('read' or 'write').")
    schemas: List[str] = Field(
        default_factory=list, description="List of schemas this permission applies to."
    )


class RoleModel(BaseModel):
    name: str = Field(..., description="Name of the role.")
    permissions: List[PermissionModel] = Field(..., description="List of permissions for the role.")
    inherit: Optional[str] = Field(None, description="Name of the role to inherit from.")
    description: Optional[str] = Field(None, description="Description of the role.")


class ConfigModel(BaseModel):
    pum: Optional[PumModel] = Field(default_factory=PumModel)
    parameters: Optional[List[ParameterDefinitionModel]] = []
    migration_hooks: Optional[MigrationHooksModel] = Field(default_factory=MigrationHooksModel)
    pum_migrations_schema: Optional[str] = None
    changelogs_directory: Optional[str] = "changelogs"
    roles: Optional[List[RoleModel]] = None  # You can make this more specific


logger = logging.getLogger(__name__)


class PumConfig(ConfigModel):
    """A class to hold configuration settings."""

    def __init__(self, dir: str | Path, validate: bool = True, **kwargs: dict) -> None:
        """Initialize the configuration with key-value pairs.

        Args:
            dir: The directory where the changelogs are located.
            validate: Whether to validate the changelogs and hooks.
            **kwargs: Key-value pairs representing configuration settings.

        Raises:
            PumConfigError: If the configuration is invalid.

        """
        # self.pg_restore_exe: str | None = kwargs.get("pg_restore_exe") or os.getenv(
        #     "PG_RESTORE_EXE"
        # )
        # self.pg_dump_exe: str | None = kwargs.get("pg_dump_exe") or os.getenv("PG_DUMP_EXE")

        global DIR
        DIR = dir if isinstance(dir, Path) else Path(dir)
        if not DIR.is_dir():
            raise PumConfigError(f"Directory `{DIR}` does not exist.")

        try:
            super().__init__(**kwargs)  # Initialize the base model
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

        if "dir" in data:
            raise PumConfigError("dir not allowed in configuration instead.")

        dir_ = Path(file_path).parent
        return cls(dir=dir_, validate=validate, **data)

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
        path = DIR / self.changelogs_directory
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

        migration_hooks = []
        if self.migration_hooks.pre:
            migration_hooks.extend(self.migration_hooks.pre)
        if self.migration_hooks.post:
            migration_hooks.extend(self.migration_hooks.post)
        for hook in migration_hooks:
            try:
                hook.hook_handler.validate(parameter_defaults)
            except PumHookError as e:
                raise PumHookError(f"Hook `{hook}` is invalid.") from e
