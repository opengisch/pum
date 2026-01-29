from packaging.version import Version
from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Any, Literal


from .exceptions import PumConfigError
from .parameter import ParameterType


class PumCustomBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ParameterDefinitionModel(PumCustomBaseModel):
    """ParameterDefinitionModel represents a parameter definition in the configuration.

    Attributes:
        name: Name of the parameter.
        type: Type of the parameter (default is TEXT).
        default: Optional default value for the parameter.
        description: Optional description of the parameter.
    """

    name: str
    type: ParameterType = Field(default=ParameterType.TEXT, description="Type of the parameter")
    default: Any | None = None
    description: str | None = None

    @model_validator(mode="before")
    def validate_default(cls, values):
        if values.get("type") == ParameterType.BOOLEAN:
            values["default"] = values.get("default", False) in (1, "1", "true", "TRUE", True)
        return values


class HookModel(PumCustomBaseModel):
    """
    HookModel represents a migration hook configuration.
    It can either execute a file (SQL or Python script) or run inline SQL code.

    Attributes:
        file: Optional path to a SQL file or a Python script to execute as a hook.
        code: Optional SQL code to execute as a hook.
    """

    file: str | None = None
    code: str | None = None

    @model_validator(mode="after")
    def validate_args(self):
        file, code = self.file, self.code
        if (file and code) or (not file and not code):
            raise PumConfigError("Exactly one of 'file' or 'code' must be set in a hook.")
        return self


class ApplicationModel(PumCustomBaseModel):
    """
    ApplicationModel holds the configuration for application hooks.

    Attributes:
        drop: Hooks to drop the application before applying migrations.
        create: Hooks to create the application after applying migrations.
    """

    drop: list[HookModel] | None = Field(default=[], alias="pre")
    create: list[HookModel] | None = Field(default=[], alias="post")

    @model_validator(mode="before")
    def handle_legacy_names(cls, values):
        """Support legacy field names for backward compatibility."""
        # If new names don't exist but old names do, use old names
        if "drop" not in values and "pre" in values:
            values["drop"] = values.pop("pre")
        if "create" not in values and "post" in values:
            values["create"] = values.pop("post")
        return values


class PumModel(PumCustomBaseModel):
    """
    PumModel holds some PUM specifics.

    Attributes:
        module: Name of the module being managed.
        migration_table_schema: Name of schema for the migration table. The table will always be named `pum_migrations`.
        minimum_version: Minimum required version of PUM.
    """

    model_config = {"arbitrary_types_allowed": True}
    module: str = Field(..., description="Name of the module being managed")
    migration_table_schema: str | None = Field(
        default="public", description="Name of schema for the migration table"
    )

    minimum_version: Version | None = Field(
        default=None,
        description="Minimum required version of pum.",
    )

    @model_validator(mode="before")
    def parse_minimum_version(cls, values):
        min_ver = values.get("minimum_version")
        if isinstance(min_ver, str):
            values["minimum_version"] = Version(min_ver)
        return values


class PermissionModel(PumCustomBaseModel):
    """
    PermissionModel represents a permission for a database role.

    Attributes:
        type: Type of permission ('read' or 'write').
        schemas: List of schemas this permission applies to.
    """

    type: Literal["read", "write"] = Field(..., description="Permission type ('read' or 'write').")
    schemas: list[str] = Field(
        default_factory=list, description="List of schemas this permission applies to."
    )


class RoleModel(PumCustomBaseModel):
    """
    RoleModel represents a database role with associated permissions.
    Attributes:
        name: Name of the role.
        permissions: List of permissions associated with the role.
        inherit: Optional name of another role to inherit permissions from.
        description: Optional description of the role.
    """

    name: str = Field(..., description="Name of the role.")
    permissions: list[PermissionModel] = Field(
        default_factory=list, description="List of permissions for the role."
    )
    inherit: str | None = Field(None, description="Name of the role to inherit from.")
    description: str | None = Field(None, description="Description of the role.")


class DemoDataModel(PumCustomBaseModel):
    """
    DemoDataModel represents a configuration for demo data.

    Attributes:
        name: Name of the demo data.
        file: Optional path to a single demo data file.
        files: Optional list of paths to multiple demo data files.
    """

    name: str = Field(..., description="Name of the demo data.")

    file: str | None = None
    files: list[str] | None = None

    @model_validator(mode="after")
    def validate_args(self):
        file, files = self.file, self.files
        if (file and files) or (not file and not files):
            raise PumConfigError("Exactly one of 'file' or 'files' must be set in a demo data set.")
        return self


class DependencyModel(PumCustomBaseModel):
    """
    DependencyModel represents a Python dependency for PUM.

    Attributes:
        name: Name of the Python dependency.
        version: Version of the dependency.
    """

    model_config = {"arbitrary_types_allowed": True}

    name: str = Field(..., description="Name of the Python dependency.")
    minimum_version: Version | None = Field(
        default=None,
        description="Specific minimum required version of the package.",
    )
    maximum_version: Version | None = Field(
        default=None,
        description="Specific maximum required version of the package.",
    )

    @model_validator(mode="before")
    def parse_version(cls, values):
        for value in ("minimum_version", "maximum_version"):
            ver = values.get(value)
            if isinstance(ver, str):
                values[value] = Version(ver)
            return values


class ConfigModel(PumCustomBaseModel):
    """
    ConfigModel represents the main configuration schema for the application.

    Attributes:
        pum: The PUM (Project Update Manager) configuration. Defaults to a new PumModel instance.
        parameters: List of parameter definitions. Defaults to an empty list.
        application: Configuration for application hooks. Defaults to a new ApplicationModel instance.
        changelogs_directory: Directory path for changelogs. Defaults to "changelogs".
        roles: List of role definitions. Defaults to None.
    """

    pum: PumModel | None = Field(default_factory=PumModel)
    parameters: list[ParameterDefinitionModel] | None = []
    application: ApplicationModel | None = Field(
        default_factory=ApplicationModel, alias="migration_hooks"
    )
    changelogs_directory: str | None = "changelogs"
    roles: list[RoleModel] | None = []
    demo_data: list[DemoDataModel] | None = []
    dependencies: list[DependencyModel] | None = []
    uninstall: list[HookModel] | None = []

    @model_validator(mode="before")
    def handle_legacy_field_names(cls, values):
        """Support legacy field names for backward compatibility."""
        # If new name doesn't exist but old name does, use old name
        if "application" not in values and "migration_hooks" in values:
            values["application"] = values.pop("migration_hooks")
        return values
