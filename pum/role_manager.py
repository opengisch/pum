import enum
from typing import Optional
import copy


class PermissionType(enum.Enum):
    """Enum for permission types.

    Attributes:
        READ (str): Read permission.
        WRITE (str): Write permission.
        EXECUTE (str): Execute permission.
    """

    READ = "read"
    WRITE = "write"


class Permission:
    def __init__(self, type: PermissionType | str, schemas: list[str] = None) -> None:
        if not isinstance(type, PermissionType):
            type = PermissionType(type)


class Role:
    def __init__(
        self,
        name: str,
        permissions: list[Permission] | list[str],
        *,
        inherit: Optional["Role"] = None,
        description: str | None = None,
    ) -> None:
        """Initialize the Role class.
        Args:
            name: Name of the role.
            permissions: List of permissions associated with the role.
            inherit: Optional role to inherit permissions from.
            description: Optional description of the role.
        """
        self.name = name
        if isinstance(permissions, list) and all(isinstance(p, dict) for p in permissions):
            self._permissions = [Permission(**p) for p in permissions]
        elif isinstance(permissions, list) and all(isinstance(p, Permission) for p in permissions):
            self._permissions = permissions
        else:
            raise TypeError("Permissions must be a list of dictionnaries or Permission instances.")

        if inherit is not None and not isinstance(inherit, Role):
            raise TypeError("Inherit must be a Role instance or None.")
        self.inherit = inherit
        self.description = description

    def permissions(self):
        return self._permissions


class RoleManager:
    def __init__(self, roles=list[Role] | list[dict]) -> None:
        """Initialize the RoleManager class.:
        Args:
            roles: List of roles or dictionaries defining roles.
            Each role can be a dictionary with keys 'name', 'permissions', and optional 'description' and 'inherit'.
        """
        if isinstance(roles, list) and all(isinstance(role, dict) for role in roles):
            for role in roles:
                _inherit = role.get("inherit")
                if _inherit is not None:
                    if _inherit not in roles:
                        raise ValueError(
                            f"Inherited role {_inherit} does not exist in the already defined roles. Pay attention to the order of the roles in the list."
                        )
                    role["inherit"] = self.roles[_inherit]
            _roles = [Role(**role) for role in roles]
        elif isinstance(roles, list) and all(isinstance(role, Role) for role in roles):
            _roles = copy.deepcopy(roles)
        else:
            raise TypeError("Roles must be a list of dictionaries or Role instances.")

        self.roles = {role.name: role for role in _roles}

        for role in self.roles.values():
            if role.inherit is not None and role.inherit not in self.roles:
                raise ValueError(
                    f"Inherited role {role.inherit.name} does not exist in the defined roles."
                )
