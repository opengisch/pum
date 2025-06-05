from pathlib import Path
import packaging
from pydantic import BaseModel, Field, model_validator, PrivateAttr
from typing import List, Optional, Any, Literal


from .exceptions import PumConfigError
from .parameter import ParameterType


class ParameterDefinitionModel(BaseModel):
    name: str
    type: ParameterType = Field(default=ParameterType.TEXT, description="Type of the parameter")
    default: Optional[Any] = None
    description: Optional[str] = None


class HookModel(BaseModel):
    file: Optional[str] = None
    code: Optional[str] = None

    @model_validator(mode="after")
    def validate_args(self):
        file, code = self.file, self.code
        if (file and code) or (not file and not code):
            raise PumConfigError("Exactly one of 'file' or 'code' must be set in a hook.")
        return self


class MigrationHooksModel(BaseModel):
    pre: Optional[List[HookModel]] = []
    post: Optional[List[HookModel]] = []

    def set_base_path(self, base_path: Path):
        if self.pre:
            for hook in self.pre:
                hook._base_path = base_path
        if self.post:
            for hook in self.post:
                hook._base_path = base_path


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
    permissions: List[PermissionModel] = Field(
        default_factory=list, description="List of permissions for the role."
    )
    inherit: Optional[str] = Field(None, description="Name of the role to inherit from.")
    description: Optional[str] = Field(None, description="Description of the role.")


class ConfigModel(BaseModel):
    pum: Optional[PumModel] = Field(default_factory=PumModel)
    parameters: Optional[List[ParameterDefinitionModel]] = []
    migration_hooks: Optional[MigrationHooksModel] = Field(default_factory=MigrationHooksModel)
    changelogs_directory: Optional[str] = "changelogs"
    roles: Optional[List[RoleModel]] = None
    _base_path: Path = PrivateAttr(default=None)

    def set_base_path(self, base_path: Path):
        """Set the base path for the configuration."""
        self._base_path = base_path
