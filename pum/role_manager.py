import enum
from dataclasses import dataclass, field
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

    Version Added:
        1.3.0
    """

    READ = "read"
    WRITE = "write"


class Permission:
    """Class to represent a permission for a database role.

    Attributes:
        type: Type of permission (read or write).
        schemas: List of schemas this permission applies to.

    Version Added:
        1.3.0
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

    Version Added:
        1.3.0
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

    Version Added:
        1.3.0
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
        *,
        suffix: str | None = None,
        create_generic: bool = True,
        grant: bool = False,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Create roles in the database.

        When *suffix* is provided, DB-specific roles are created by appending
        the suffix to each configured role name (e.g. ``tww_user_lausanne``
        for suffix ``lausanne``). The *create_generic* flag controls whether
        the base roles are also created and granted membership of the specific
        roles (so that the generic role inherits the specific one's permissions).

        When *suffix* is ``None`` (default), only the generic roles defined in
        the configuration are created.

        Args:
            connection: The database connection to execute the SQL statements.
            suffix: Optional suffix to append to role names for DB-specific
                roles.
            create_generic: Whether to also create the generic (config-defined)
                roles and grant them membership of the specific roles.
                When *suffix* is ``None`` this is always ``True``.
                Defaults to True.
            grant: Whether to grant permissions to the roles. Defaults to False.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.

        Version Changed:
            1.5.0: Added *suffix* and *create_generic* parameters for DB-specific roles.
        """
        roles_list = list(self.roles.values())

        if suffix:
            for role in roles_list:
                if feedback and feedback.is_cancelled():
                    raise PumException("Role creation cancelled by user")

                specific_name = f"{role.name}_{suffix}"

                # Build a specific role with the suffixed name and same permissions
                specific_role = Role(
                    name=specific_name,
                    permissions=[
                        Permission(type=p.type, schemas=p.schemas) for p in role.permissions()
                    ],
                    description=(
                        f"{role.description} (specific to {suffix})" if role.description else None
                    ),
                )

                if feedback:
                    feedback.increment_step()
                    feedback.report_progress(f"Creating specific role: {specific_name}")

                specific_role.create(
                    connection=connection, commit=False, grant=grant, feedback=feedback
                )

                if create_generic:
                    if feedback:
                        feedback.increment_step()
                        feedback.report_progress(f"Creating generic role: {role.name}")
                    role.create(connection=connection, commit=False, grant=False, feedback=feedback)

                    logger.debug(
                        f"Granting specific role {specific_name} to generic role {role.name}."
                    )
                    SqlContent("GRANT {specific} TO {generic}").execute(
                        connection=connection,
                        commit=False,
                        parameters={
                            "specific": psycopg.sql.Identifier(specific_name),
                            "generic": psycopg.sql.Identifier(role.name),
                        },
                    )
        else:
            for role in roles_list:
                if feedback and feedback.is_cancelled():
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

    def check_roles(
        self,
        connection: psycopg.Connection,
    ) -> "RoleCheckResult":
        """Check that the database roles match the configuration.

        For each configured role, checks the generic role and automatically
        discovers any DB-specific (suffixed) variants (e.g. ``tww_user_lausanne``,
        ``tww_user_zurich``).  For every discovered role it verifies that the
        expected permissions are present.  Roles that have access to configured
        schemas but are not in the configuration are reported as unknown.

        Args:
            connection: The database connection to use.

        Returns:
            A ``RoleCheckResult`` summarising the findings.

        Version Added:
            1.5.0
        """
        configured_schemas = set()
        for role in self.roles.values():
            for perm in role.permissions():
                if perm.schemas:
                    configured_schemas.update(perm.schemas)

        cursor = connection.cursor()

        # Discover all roles matching each configured name or <name>_*
        role_statuses: list[RoleStatus] = []
        known_names: set[str] = set()
        for role in self.roles.values():
            # Find the generic role and any suffixed variants
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = %s OR rolname LIKE %s ORDER BY rolname",
                (role.name, f"{role.name}\\_%"),
            )
            found_names = [row[0] for row in cursor.fetchall()]

            if not found_names:
                # Role is completely missing
                role_statuses.append(
                    _check_single_role(connection, role.name, role, configured_schemas)
                )
                known_names.add(role.name)
            else:
                for name in found_names:
                    role_statuses.append(
                        _check_single_role(connection, name, role, configured_schemas)
                    )
                    known_names.add(name)

        # Discover unknown roles with privileges on the configured schemas
        unknown_roles = _find_unknown_roles(
            connection,
            configured_schemas,
            known_names=known_names,
        )

        return RoleCheckResult(roles=role_statuses, unknown_roles=unknown_roles)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class SchemaPermissionStatus:
    """Result of checking a single schema permission for a role.

    Version Added:
        1.5.0
    """

    schema: str
    """Name of the schema."""
    expected: PermissionType | None
    """Expected permission type from the configuration, or ``None``."""
    has_read: bool = False
    """Whether the role currently has read-level access."""
    has_write: bool = False
    """Whether the role currently has write-level access."""

    @property
    def ok(self) -> bool:
        """``True`` when the actual privileges match the expected ones."""
        if self.expected == PermissionType.READ:
            return self.has_read
        if self.expected == PermissionType.WRITE:
            return self.has_write
        # No expectation – anything is fine
        return True


@dataclass
class RoleStatus:
    """Result of checking a single role against the database.

    Version Added:
        1.5.0
    """

    name: str
    """Role name that was checked."""
    exists: bool
    """Whether the role exists in the database."""
    schema_permissions: list[SchemaPermissionStatus] = field(default_factory=list)
    """Per-schema permission details."""

    @property
    def ok(self) -> bool:
        """``True`` when the role exists and all permissions match."""
        return self.exists and all(sp.ok for sp in self.schema_permissions)


@dataclass
class UnknownRole:
    """A role not defined in the configuration that has privileges on a
    configured schema.

    Version Added:
        1.5.0
    """

    name: str
    """Role name."""
    schemas: list[str]
    """Schemas on which the role has privileges."""


@dataclass
class RoleCheckResult:
    """Aggregated result of ``RoleManager.check_roles``.

    Version Added:
        1.5.0
    """

    roles: list[RoleStatus] = field(default_factory=list)
    """Status of each expected role."""
    unknown_roles: list[UnknownRole] = field(default_factory=list)
    """Roles not in the configuration that have schema privileges."""

    @property
    def ok(self) -> bool:
        """``True`` when every expected role is OK and there are no unknown roles."""
        return all(r.ok for r in self.roles) and not self.unknown_roles


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _check_single_role(
    connection: psycopg.Connection,
    name: str,
    role: Role,
    configured_schemas: set[str],
) -> RoleStatus:
    """Check whether *name* exists and has the expected permissions."""
    cursor = connection.cursor()
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (name,))
    exists = cursor.fetchone() is not None

    if not exists:
        # Build permission entries even though nothing can match
        perms = []
        for perm in role.permissions():
            for schema in perm.schemas or []:
                perms.append(SchemaPermissionStatus(schema=schema, expected=perm.type))
        return RoleStatus(name=name, exists=False, schema_permissions=perms)

    # Check every configured schema
    schema_statuses: list[SchemaPermissionStatus] = []
    for perm in role.permissions():
        for schema in perm.schemas or []:
            has_read = _has_read(connection, name, schema)
            has_write = _has_write(connection, name, schema)
            schema_statuses.append(
                SchemaPermissionStatus(
                    schema=schema,
                    expected=perm.type,
                    has_read=has_read,
                    has_write=has_write,
                )
            )

    return RoleStatus(name=name, exists=exists, schema_permissions=schema_statuses)


def _has_read(connection: psycopg.Connection, role: str, schema: str) -> bool:
    """Return ``True`` if *role* has read-level access to *schema*.

    Checks USAGE on the schema plus SELECT on all tables (if any exist).
    """
    cursor = connection.cursor()
    cursor.execute("SELECT has_schema_privilege(%s, %s, 'USAGE')", (role, schema))
    row = cursor.fetchone()
    if not row or not row[0]:
        return False

    # Check SELECT on all tables/views
    cursor.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'v', 'm')
        LIMIT 1
        """,
        (schema,),
    )
    if cursor.fetchone() is not None:
        cursor.execute(
            """
            SELECT bool_and(has_table_privilege(%s, n.nspname || '.' || c.relname, 'SELECT'))
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind IN ('r', 'v', 'm')
            """,
            (role, schema),
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            return False

    return True


def _has_write(connection: psycopg.Connection, role: str, schema: str) -> bool:
    """Return ``True`` if *role* has write-level access to *schema*.

    Checks CREATE on the schema plus INSERT/UPDATE/DELETE on all tables
    (if any exist).
    """
    cursor = connection.cursor()
    cursor.execute("SELECT has_schema_privilege(%s, %s, 'CREATE')", (role, schema))
    row = cursor.fetchone()
    if not row or not row[0]:
        return False

    cursor.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind = 'r'
        LIMIT 1
        """,
        (schema,),
    )
    if cursor.fetchone() is not None:
        cursor.execute(
            """
            SELECT bool_and(
                has_table_privilege(%s, n.nspname || '.' || c.relname, 'INSERT')
                AND has_table_privilege(%s, n.nspname || '.' || c.relname, 'UPDATE')
                AND has_table_privilege(%s, n.nspname || '.' || c.relname, 'DELETE')
            )
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind = 'r'
            """,
            (role, role, role, schema),
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            return False

    return True


def _find_unknown_roles(
    connection: psycopg.Connection,
    schemas: set[str],
    known_names: set[str],
) -> list[UnknownRole]:
    """Return roles not in *known_names* that have privileges on *schemas*.

    Excludes PostgreSQL system roles and the current superuser.
    """
    if not schemas:
        return []

    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT r.rolname, n.nspname
        FROM pg_namespace n
        CROSS JOIN pg_roles r
        WHERE n.nspname = ANY(%s)
          AND has_schema_privilege(r.rolname, n.nspname, 'USAGE')
          AND NOT r.rolsuper
          AND r.rolname NOT LIKE 'pg_%%'
          AND r.rolname != current_user
        ORDER BY r.rolname, n.nspname
        """,
        (list(schemas),),
    )

    role_schemas: dict[str, list[str]] = {}
    for rolname, nspname in cursor.fetchall():
        if rolname not in known_names:
            role_schemas.setdefault(rolname, []).append(nspname)

    return [UnknownRole(name=name, schemas=schemalist) for name, schemalist in role_schemas.items()]
