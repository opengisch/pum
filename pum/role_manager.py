import enum
from typing import Optional, TYPE_CHECKING
import copy
import psycopg
import logging

from .sql_content import SqlContent
from .exceptions import PumException

if TYPE_CHECKING:
    from .feedback import Feedback

logger = logging.getLogger(__name__)


class PermissionType(enum.Enum):
    """Enum for permission types.

    Attributes:
        READ (str): Read permission.
        WRITE (str): Write permission.
    """

    READ = "read"
    WRITE = "write"


class Permission:
    """Class to represent a permission for a database role.

    Attributes:
        type: Type of permission (read or write).
        schemas: List of schemas this permission applies to.
    """

    def __init__(self, type: PermissionType | str, schemas: list[str] = None) -> None:
        if not isinstance(type, PermissionType):
            type = PermissionType(type)
        self.type = type
        self.schemas = schemas

    def grant(
        self,
        role: str,
        connection: psycopg.Connection,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Grant the permission to the specified role.
        Args:
            role: The name of the role to grant the permission to.
            connection: The database connection to execute the SQL statements.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.
        """
        if not isinstance(role, str):
            raise TypeError("Role must be a string.")

        if not self.schemas:
            raise ValueError("Schemas must be defined for the permission.")

        for schema in self.schemas:
            if feedback and feedback.is_cancelled():
                raise PumException("Permission grant cancelled by user")

            # Detect if schema exists; if not, warn and continue
            cursor = SqlContent("SELECT 1 FROM pg_namespace WHERE nspname = {schema}").execute(
                connection=connection,
                commit=False,
                parameters={"schema": psycopg.sql.Literal(schema)},
            )
            if not cursor._pum_results or not cursor._pum_results[0]:
                logger.warning(
                    f"Schema {schema} does not exist; skipping grant of {self.type.value} "
                    f"permission to role {role}."
                )
                continue

            logger.debug(
                f"Granting {self.type.value} permission on schema {schema} to role {role}."
            )
            if self.type == PermissionType.READ:
                SqlContent("""
                        GRANT USAGE ON SCHEMA {schema} TO {role};
                        GRANT SELECT, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA {schema} TO {role};
                        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {role};
                        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {role};
                        GRANT EXECUTE ON ALL ROUTINES IN SCHEMA {schema} TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT, REFERENCES, TRIGGER ON TABLES TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON SEQUENCES TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT EXECUTE ON ROUTINES TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT USAGE ON TYPES TO {role};
                           """).execute(
                    connection=connection,
                    commit=False,
                    parameters={
                        "schema": psycopg.sql.Identifier(schema),
                        "role": psycopg.sql.Identifier(role),
                    },
                )
                # Grant permissions on existing types
                self._grant_existing_types(connection, schema, role, "USAGE")
            elif self.type == PermissionType.WRITE:
                SqlContent("""
                        GRANT ALL ON SCHEMA {schema} TO {role};
                        GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO {role};
                        GRANT ALL ON ALL SEQUENCES IN SCHEMA {schema} TO {role};
                        GRANT ALL ON ALL FUNCTIONS IN SCHEMA {schema} TO {role};
                        GRANT ALL ON ALL ROUTINES IN SCHEMA {schema} TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON SEQUENCES TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON FUNCTIONS TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON ROUTINES TO {role};
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TYPES TO {role};
                           """).execute(
                    connection=connection,
                    commit=False,
                    parameters={
                        "schema": psycopg.sql.Identifier(schema),
                        "role": psycopg.sql.Identifier(role),
                    },
                )
                # Grant permissions on existing types
                self._grant_existing_types(connection, schema, role, "ALL")
            else:
                raise ValueError(f"Unknown permission type: {self.type}")

        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()

    def _grant_existing_types(
        self, connection: psycopg.Connection, schema: str, role: str, privilege: str
    ) -> None:
        """Grant permissions on all existing types in a schema.

        Args:
            connection: The database connection.
            schema: The schema name.
            role: The role name.
            privilege: The privilege to grant (USAGE or ALL).

        """
        # Query for all types in the schema (excluding array types and internal types)
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT t.typname
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = %s
              AND t.typtype IN ('e', 'c', 'd', 'b', 'r')  -- enum, composite, domain, base, range
              AND t.typname NOT LIKE '_%%'  -- exclude array types
            """,
            (schema,),
        )
        types = cursor.fetchall()

        # Grant permissions on each type
        for (type_name,) in types:
            grant_sql = psycopg.sql.SQL(
                "GRANT {privilege} ON TYPE {schema}.{type_name} TO {role}"
            ).format(
                privilege=psycopg.sql.SQL(privilege),
                schema=psycopg.sql.Identifier(schema),
                type_name=psycopg.sql.Identifier(type_name),
                role=psycopg.sql.Identifier(role),
            )
            cursor.execute(grant_sql)

    def __repr__(self) -> str:
        """Return a string representation of the Permission object."""
        return f"<Permission: {self.type.value} on {self.schemas}>"


class Role:
    """
    Represents a database role with associated permissions and optional inheritance.
    The Role class encapsulates the concept of a database role, including its name,
    permissions, optional inheritance from another role, and an optional description.
    """

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

    def permissions(self) -> list[Permission]:
        """
        Returns the list of permissions associated with the role.
        """
        return self._permissions

    def exists(self, connection: psycopg.Connection) -> bool:
        """Check if the role exists in the database.
        Args:
            connection: The database connection to execute the SQL statements.
        Returns:
            bool: True if the role exists, False otherwise.
        """
        cursor = SqlContent("SELECT 1 FROM pg_roles WHERE rolname = {name}").execute(
            connection=connection,
            commit=False,
            parameters={"name": psycopg.sql.Literal(self.name)},
        )
        return bool(cursor._pum_results)

    def create(
        self,
        connection: psycopg.Connection,
        grant: bool = False,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Create the role in the database.
        Args:
            connection: The database connection to execute the SQL statements.
            grant: Whether to grant permissions to the role. Defaults to False.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.
        """
        if feedback and feedback.is_cancelled():
            from .exceptions import PumException

            raise PumException("Role creation cancelled by user")

        if self.exists(connection):
            logger.debug(f"Role {self.name} already exists, skipping creation.")
        else:
            logger.debug(f"Creating role {self.name}.")
            SqlContent(
                "CREATE ROLE {name} NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION"
            ).execute(
                connection=connection,
                commit=False,
                parameters={"name": psycopg.sql.Identifier(self.name)},
            )
            if self.description:
                SqlContent("COMMENT ON ROLE {name} IS {description}").execute(
                    connection=connection,
                    commit=False,
                    parameters={
                        "name": psycopg.sql.Identifier(self.name),
                        "description": psycopg.sql.Literal(self.description),
                    },
                )
            if self.inherit:
                SqlContent("GRANT {inherit} TO {role}").execute(
                    connection=connection,
                    commit=False,
                    parameters={
                        "inherit": psycopg.sql.Identifier(self.inherit.name),
                        "role": psycopg.sql.Identifier(self.name),
                    },
                )
        if grant:
            for permission in self.permissions():
                permission.grant(
                    role=self.name, connection=connection, commit=False, feedback=feedback
                )

        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()


class RoleManager:
    """
    RoleManager manages a collection of Role objects,
    allowing creation and permission management
    for multiple roles in the PostgreSQL database.
    """

    def __init__(self, roles=list[Role] | list[dict]) -> None:
        """Initialize the RoleManager class.:
        Args:
            roles: List of roles or dictionaries defining roles.
            Each role can be a dictionary with keys 'name', 'permissions', and optional 'description' and 'inherit'.
        """
        if isinstance(roles, list) and all(isinstance(role, dict) for role in roles):
            self.roles = {}
            for role in roles:
                _inherit = role.get("inherit")
                if _inherit is not None:
                    if _inherit not in self.roles:
                        raise ValueError(
                            f"Inherited role {_inherit} does not exist in the already defined roles. Pay attention to the order of the roles in the list."
                        )
                    role["inherit"] = self.roles[_inherit]
                self.roles[role["name"]] = Role(**role)
        elif isinstance(roles, list) and all(isinstance(role, Role) for role in roles):
            _roles = copy.deepcopy(roles)
            self.roles = {role.name: role for role in _roles}
        else:
            raise TypeError("Roles must be a list of dictionaries or Role instances.")

        for role in self.roles.values():
            if role.inherit is not None and role.inherit not in self.roles.values():
                raise ValueError(
                    f"Inherited role {role.inherit.name} does not exist in the defined roles."
                )

    def create_roles(
        self,
        connection: psycopg.Connection,
        grant: bool = False,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Create roles in the database.
        Args:
            connection: The database connection to execute the SQL statements.
            grant: Whether to grant permissions to the roles. Defaults to False.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.
        """
        roles_list = list(self.roles.values())
        for role in roles_list:
            if feedback and feedback.is_cancelled():
                from .exceptions import PumException

                raise PumException("Role creation cancelled by user")
            if feedback:
                feedback.increment_step()
                feedback.report_progress(f"Creating role: {role.name}")
            role.create(connection=connection, commit=False, grant=grant, feedback=feedback)
        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()

    def grant_permissions(
        self,
        connection: psycopg.Connection,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Grant permissions to the roles in the database.
        Args:
            connection: The database connection to execute the SQL statements.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.
        """
        roles_list = list(self.roles.values())
        for role in roles_list:
            if feedback and feedback.is_cancelled():
                from .exceptions import PumException

                raise PumException("Permission grant cancelled by user")
            if feedback:
                feedback.increment_step()
                feedback.report_progress(f"Granting permissions to role: {role.name}")
            for permission in role.permissions():
                permission.grant(
                    role=role.name, connection=connection, commit=False, feedback=feedback
                )
        logger.info("All permissions granted to roles.")
        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()
