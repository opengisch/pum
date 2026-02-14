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
        grant: bool = False,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Create roles in the database.

        When *suffix* is provided, DB-specific roles are created by appending
        the suffix to each configured role name (e.g. ``tww_user_lausanne``
        for suffix ``lausanne``). The generic (base) roles are also created
        and granted membership of the specific roles, so that the generic role
        inherits the specific one's permissions.

        When *suffix* is ``None`` (default), only the generic roles defined in
        the configuration are created.

        Args:
            connection: The database connection to execute the SQL statements.
            suffix: Optional suffix to append to role names for DB-specific
                roles. When provided, both the suffixed and generic roles are
                created, and inheritance is granted.
            grant: Whether to grant permissions to the roles. Defaults to False.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.

        Version Changed:
            1.5.0: Added *suffix* parameter for DB-specific roles.
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

                if feedback:
                    feedback.increment_step()
                    feedback.report_progress(f"Creating generic role: {role.name}")
                role.create(connection=connection, commit=False, grant=False, feedback=feedback)

                logger.debug(f"Granting specific role {specific_name} to generic role {role.name}.")
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
        role_names = ", ".join(r.name for r in roles_list)
        logger.info(f"Permissions granted to roles: {role_names}.")
        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()

    def _resolve_roles(
        self,
        roles: list[str] | None = None,
    ) -> list[Role]:
        """Return the list of Role objects to act on.

        When *roles* is ``None`` all configured roles are returned.
        Otherwise only the roles whose names appear in the list are
        returned (in configuration order).  Raises ``PumException``
        if any requested name is not in the configuration.

        Args:
            roles: Optional role names to filter on.

        Returns:
            Filtered list of ``Role`` objects.
        """
        if roles is None:
            return list(self.roles.values())
        unknown = set(roles) - set(self.roles)
        if unknown:
            raise PumException(
                f"Unknown role(s): {', '.join(sorted(unknown))}. "
                f"Configured roles: {', '.join(self.roles)}"
            )
        return [self.roles[name] for name in roles]

    def revoke_permissions(
        self,
        connection: psycopg.Connection,
        *,
        roles: list[str] | None = None,
        suffix: str | None = None,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Revoke previously granted permissions from roles.

        When *suffix* is provided, permissions are revoked from the
        DB-specific (suffixed) roles only.  Otherwise they are revoked
        from the generic roles.

        When *roles* is provided only those configured roles are acted
        on; otherwise all configured roles are affected.

        Args:
            connection: The database connection to execute the SQL statements.
            roles: Optional list of configured role names to revoke.
                When ``None`` (default), all configured roles are revoked.
            suffix: Optional suffix identifying DB-specific roles.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.

        Version Added:
            1.5.0
        """
        target_roles = self._resolve_roles(roles)
        for role in target_roles:
            if feedback and feedback.is_cancelled():
                raise PumException("Permission revoke cancelled by user")

            role_name = f"{role.name}_{suffix}" if suffix else role.name

            if feedback:
                feedback.increment_step()
                feedback.report_progress(f"Revoking permissions from role: {role_name}")

            for perm in role.permissions():
                for schema in perm.schemas or []:
                    # Check if schema exists before revoking
                    cursor = SqlContent(
                        "SELECT 1 FROM pg_namespace WHERE nspname = {schema}"
                    ).execute(
                        connection=connection,
                        commit=False,
                        parameters={"schema": psycopg.sql.Literal(schema)},
                    )
                    if not cursor._pum_results or not cursor._pum_results[0]:
                        logger.warning(
                            f"Schema {schema} does not exist; skipping revoke for role {role_name}."
                        )
                        continue

                    logger.debug(
                        f"Revoking {perm.type.value} permission on schema {schema} from role {role_name}."
                    )
                    if perm.type == PermissionType.READ:
                        SqlContent("""
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE SELECT, REFERENCES, TRIGGER ON TABLES FROM {role};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE SELECT ON SEQUENCES FROM {role};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE EXECUTE ON FUNCTIONS FROM {role};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE EXECUTE ON ROUTINES FROM {role};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE USAGE ON TYPES FROM {role};
                            REVOKE EXECUTE ON ALL ROUTINES IN SCHEMA {schema} FROM {role};
                            REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} FROM {role};
                            REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {schema} FROM {role};
                            REVOKE SELECT, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA {schema} FROM {role};
                            REVOKE USAGE ON SCHEMA {schema} FROM {role};
                        """).execute(
                            connection=connection,
                            commit=False,
                            parameters={
                                "schema": psycopg.sql.Identifier(schema),
                                "role": psycopg.sql.Identifier(role_name),
                            },
                        )
                    elif perm.type == PermissionType.WRITE:
                        SqlContent("""
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE ALL ON TABLES FROM {role};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE ALL ON SEQUENCES FROM {role};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE ALL ON FUNCTIONS FROM {role};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE ALL ON ROUTINES FROM {role};
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE ALL ON TYPES FROM {role};
                            REVOKE ALL ON ALL ROUTINES IN SCHEMA {schema} FROM {role};
                            REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {schema} FROM {role};
                            REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM {role};
                            REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {role};
                            REVOKE ALL ON SCHEMA {schema} FROM {role};
                        """).execute(
                            connection=connection,
                            commit=False,
                            parameters={
                                "schema": psycopg.sql.Identifier(schema),
                                "role": psycopg.sql.Identifier(role_name),
                            },
                        )

        # Revoke role memberships so inherited privileges are removed too.
        # E.g. if generic is a member of specific (GRANT specific TO generic),
        # we need to REVOKE specific FROM generic for the revoke to be effective.
        for role in target_roles:
            role_name = f"{role.name}_{suffix}" if suffix else role.name

            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT r.rolname
                FROM pg_auth_members m
                JOIN pg_roles r ON r.oid = m.roleid
                JOIN pg_roles mr ON mr.oid = m.member
                WHERE mr.rolname = %s
                """,
                (role_name,),
            )
            for (parent_role,) in cursor.fetchall():
                logger.debug(f"Revoking membership: REVOKE {parent_role} FROM {role_name}")
                SqlContent("REVOKE {parent} FROM {member}").execute(
                    connection=connection,
                    commit=False,
                    parameters={
                        "parent": psycopg.sql.Identifier(parent_role),
                        "member": psycopg.sql.Identifier(role_name),
                    },
                )

        role_names = ", ".join(f"{r.name}_{suffix}" if suffix else r.name for r in target_roles)
        logger.info(f"Permissions revoked from roles: {role_names}.")
        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()

    def drop_roles(
        self,
        connection: psycopg.Connection,
        *,
        roles: list[str] | None = None,
        suffix: str | None = None,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Drop roles from the database.

        Permissions are revoked first (via ``revoke_permissions``), then roles
        are dropped.  When *suffix* is provided only the DB-specific roles are
        dropped; the generic roles are left untouched.  Without *suffix* only
        the generic roles are dropped.

        When *roles* is provided only those configured roles are acted
        on; otherwise all configured roles are affected.

        Args:
            connection: The database connection to execute the SQL statements.
            roles: Optional list of configured role names to drop.
                When ``None`` (default), all configured roles are dropped.
            suffix: Optional suffix identifying DB-specific roles.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.

        Version Added:
            1.5.0

        Version Changed:
            1.5.0: Added *roles* parameter for per-role operations.
        """
        target_roles = self._resolve_roles(roles)

        # Revoke permissions first so the roles own nothing
        self.revoke_permissions(
            connection=connection, roles=roles, suffix=suffix, commit=False, feedback=feedback
        )

        for role in target_roles:
            if feedback and feedback.is_cancelled():
                raise PumException("Role drop cancelled by user")

            role_name = f"{role.name}_{suffix}" if suffix else role.name

            if feedback:
                feedback.increment_step()
                feedback.report_progress(f"Dropping role: {role_name}")

            logger.debug(f"Dropping role {role_name}.")
            SqlContent("DROP ROLE IF EXISTS {name}").execute(
                connection=connection,
                commit=False,
                parameters={"name": psycopg.sql.Identifier(role_name)},
            )

        role_names = ", ".join(f"{r.name}_{suffix}" if suffix else r.name for r in target_roles)
        logger.info(f"Roles dropped: {role_names}.")
        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()

    def grant_to(
        self,
        connection: psycopg.Connection,
        *,
        to: str,
        roles: list[str] | None = None,
        suffix: str | None = None,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Grant configured roles to a database user.

        Executes ``GRANT <role> TO <to>`` for each selected role, making
        *to* a member of those roles so it inherits their permissions.

        When *suffix* is provided the suffixed role names are used
        (e.g. ``tww_viewer_lausanne``).  Otherwise the generic names are
        used.

        Args:
            connection: The database connection to execute the SQL statements.
            to: The target database role that will receive membership.
            roles: Optional list of configured role names to grant.
                When ``None`` (default), all configured roles are granted.
            suffix: Optional suffix identifying DB-specific roles.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.

        Version Added:
            1.5.0
        """
        target_roles = self._resolve_roles(roles)

        for role in target_roles:
            if feedback and feedback.is_cancelled():
                raise PumException("Grant-to cancelled by user")

            role_name = f"{role.name}_{suffix}" if suffix else role.name

            if feedback:
                feedback.increment_step()
                feedback.report_progress(f"Granting {role_name} to {to}")

            logger.debug(f"Granting role {role_name} to {to}.")
            SqlContent("GRANT {role} TO {target}").execute(
                connection=connection,
                commit=False,
                parameters={
                    "role": psycopg.sql.Identifier(role_name),
                    "target": psycopg.sql.Identifier(to),
                },
            )

        role_names = ", ".join(f"{r.name}_{suffix}" if suffix else r.name for r in target_roles)
        logger.info(f"Roles granted to {to}: {role_names}.")
        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()

    def revoke_from(
        self,
        connection: psycopg.Connection,
        *,
        from_role: str,
        roles: list[str] | None = None,
        suffix: str | None = None,
        commit: bool = False,
        feedback: Optional["Feedback"] = None,
    ) -> None:
        """Revoke configured roles from a database user.

        Executes ``REVOKE <role> FROM <from_role>`` for each selected
        role, removing *from_role*'s membership.

        When *suffix* is provided the suffixed role names are used.
        Otherwise the generic names are used.

        Args:
            connection: The database connection to execute the SQL statements.
            from_role: The target database role to revoke membership from.
            roles: Optional list of configured role names to revoke.
                When ``None`` (default), all configured roles are revoked.
            suffix: Optional suffix identifying DB-specific roles.
            commit: Whether to commit the transaction. Defaults to False.
            feedback: Optional feedback object for progress reporting.

        Version Added:
            1.5.0
        """
        target_roles = self._resolve_roles(roles)

        for role in target_roles:
            if feedback and feedback.is_cancelled():
                raise PumException("Revoke-from cancelled by user")

            role_name = f"{role.name}_{suffix}" if suffix else role.name

            if feedback:
                feedback.increment_step()
                feedback.report_progress(f"Revoking {role_name} from {from_role}")

            logger.debug(f"Revoking role {role_name} from {from_role}.")
            SqlContent("REVOKE {role} FROM {target}").execute(
                connection=connection,
                commit=False,
                parameters={
                    "role": psycopg.sql.Identifier(role_name),
                    "target": psycopg.sql.Identifier(from_role),
                },
            )

        role_names = ", ".join(f"{r.name}_{suffix}" if suffix else r.name for r in target_roles)
        logger.info(f"Roles revoked from {from_role}: {role_names}.")
        if commit:
            if feedback:
                feedback.lock_cancellation()
            connection.commit()

    @staticmethod
    def create_login_role(
        connection: psycopg.Connection,
        name: str,
        *,
        password: str | None = None,
        commit: bool = False,
    ) -> None:
        """Create a PostgreSQL role with the LOGIN attribute.

        Args:
            connection: The database connection.
            name: The name of the role to create.
            password: Optional password for the role.
            commit: Whether to commit the transaction. Defaults to False.

        Version Added:
            1.5.0
        """
        if password:
            SqlContent("CREATE ROLE {role} LOGIN PASSWORD {pwd}").execute(
                connection=connection,
                commit=commit,
                parameters={
                    "role": psycopg.sql.Identifier(name),
                    "pwd": psycopg.sql.Literal(password),
                },
            )
        else:
            SqlContent("CREATE ROLE {role} LOGIN").execute(
                connection=connection,
                commit=commit,
                parameters={"role": psycopg.sql.Identifier(name)},
            )
        logger.info(f"Login role '{name}' created.")

    @staticmethod
    def drop_login_role(
        connection: psycopg.Connection,
        name: str,
        *,
        commit: bool = False,
    ) -> None:
        """Drop a PostgreSQL login role.

        Args:
            connection: The database connection.
            name: The name of the role to drop.
            commit: Whether to commit the transaction. Defaults to False.

        Version Added:
            1.5.0
        """
        SqlContent("DROP ROLE IF EXISTS {role}").execute(
            connection=connection,
            commit=commit,
            parameters={"role": psycopg.sql.Identifier(name)},
        )
        logger.info(f"Login role '{name}' dropped.")

    @staticmethod
    def login_roles(connection: psycopg.Connection) -> list[str]:
        """Return the names of all login roles that are not superusers.

        This is useful for listing roles that can be granted membership
        in module roles.  System roles (``pg_*``) are excluded.

        Args:
            connection: The database connection.

        Returns:
            Sorted list of login role names.

        Version Added:
            1.5.0
        """
        with connection.transaction():
            cursor = connection.cursor()
            cursor.execute(
                "SELECT rolname FROM pg_roles "
                "WHERE rolcanlogin AND NOT rolsuper AND rolname NOT LIKE 'pg\\_%' "
                "ORDER BY rolname"
            )
            return [row[0] for row in cursor.fetchall()]

    @staticmethod
    def members_of(connection: psycopg.Connection, role_name: str) -> list[str]:
        """Return the login role names that are members of *role_name*.

        Args:
            connection: The database connection.
            role_name: The group role whose members to look up.

        Returns:
            Sorted list of member login role names.

        Version Added:
            1.5.0
        """
        with connection.transaction():
            cursor = connection.cursor()
            cursor.execute(
                "SELECT m.rolname "
                "FROM pg_auth_members am "
                "JOIN pg_roles r ON r.oid = am.roleid "
                "JOIN pg_roles m ON m.oid = am.member "
                "WHERE r.rolname = %s AND m.rolcanlogin "
                "ORDER BY m.rolname",
                (role_name,),
            )
            return [row[0] for row in cursor.fetchall()]

    def roles_inventory(
        self,
        connection: psycopg.Connection,
        *,
        include_superusers: bool = False,
    ) -> "RoleInventory":
        """List all database roles related to the module's configured schemas.

        Returns the module's generic roles, any DB-specific (suffixed)
        variants discovered via naming convention, and any other
        database roles that have access to the configured schemas.
        For every role the method reports which schemas it can read or
        write, whether it is a superuser, and whether it can log in.

        Args:
            connection: The database connection to use.
            include_superusers: When ``True``, superusers are included in
                the results.  Defaults to ``False`` because superusers
                implicitly have access to everything.

        Returns:
            A ``RoleInventory`` containing the discovered roles.

        Version Added:
            1.5.0
        """
        configured_schemas = set()
        for role in self.roles.values():
            for perm in role.permissions():
                if perm.schemas:
                    configured_schemas.update(perm.schemas)

        with connection.transaction():
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

                for name in found_names:
                    role_statuses.append(
                        _build_role_status(connection, name, role, configured_schemas)
                    )
                    known_names.add(name)

            # Discover unknown roles with privileges on the configured schemas
            unknown_roles = _find_unknown_roles(
                connection,
                configured_schemas,
                known_names=known_names,
                include_superusers=include_superusers,
            )

            # Discover login roles that have no access to the configured schemas
            all_known = known_names | {r.name for r in unknown_roles}
            other_login = _find_other_login_roles(connection, configured_schemas, all_known)

            return RoleInventory(
                roles=role_statuses + unknown_roles,
                expected_roles=list(self.roles.keys()),
                other_login_roles=other_login,
            )


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
    def satisfied(self) -> bool:
        """``True`` when the actual privileges match the expected ones."""
        if self.expected == PermissionType.READ:
            return self.has_read
        if self.expected == PermissionType.WRITE:
            return self.has_write
        # No expectation – anything is fine
        return True


@dataclass
class RoleStatus:
    """A database role discovered during ``RoleManager.check_roles``.

    Only roles that actually exist in PostgreSQL are represented.

    Version Added:
        1.5.0
    """

    name: str
    """Role name in PostgreSQL."""
    role: Role | None = None
    """Configured role this maps to, or ``None`` for unknown roles."""
    suffix: str = ""
    """DB-specific suffix (e.g. ``"lausanne"``), empty for generic roles."""
    schema_permissions: list[SchemaPermissionStatus] = field(default_factory=list)
    """Per-schema permission details."""
    granted_to: list[str] = field(default_factory=list)
    """Roles that this role is a member of (i.e. ``GRANT parent TO this``)."""
    superuser: bool = False
    """Whether the role is a PostgreSQL superuser."""
    login: bool = False
    """Whether the role has the LOGIN attribute."""

    @property
    def is_unknown(self) -> bool:
        """``True`` when the role is not mapped to a configuration entry."""
        return self.role is None

    @property
    def is_suffixed(self) -> bool:
        """``True`` when the role is a DB-specific (suffixed) variant."""
        return self.suffix != ""

    @property
    def schemas(self) -> list[str]:
        """List of schema names from the permission details."""
        return [sp.schema for sp in self.schema_permissions]


@dataclass
class RoleInventory:
    """Result of ``RoleManager.roles_inventory``.

    Contains all discovered database roles related to the module's
    configured schemas: configured roles (generic and suffixed),
    other roles with schema access, and the list of expected role
    names from the configuration.

    Version Added:
        1.5.0
    """

    roles: list[RoleStatus] = field(default_factory=list)
    """All discovered roles."""
    expected_roles: list[str] = field(default_factory=list)
    """Role names from the configuration that were expected to exist."""
    other_login_roles: list[str] = field(default_factory=list)
    """Login roles that are not superusers and have no access to any configured schema.

    Version Added:
        1.5.0
    """

    @property
    def configured_roles(self) -> list["RoleStatus"]:
        """Roles that are mapped to a configuration entry."""
        return [r for r in self.roles if not r.is_unknown]

    @property
    def grantee_roles(self) -> list["RoleStatus"]:
        """Roles not in the configuration but that are members of a configured role.

        These are typically login users that were granted module roles
        via ``grant_to`` and inherit schema access through membership.
        """
        configured_names = {r.name for r in self.roles if not r.is_unknown}
        return [
            r
            for r in self.roles
            if r.is_unknown and any(g in configured_names for g in r.granted_to)
        ]

    @property
    def unknown_roles(self) -> list["RoleStatus"]:
        """Roles not in the configuration that have schema access and are not grantees."""
        configured_names = {r.name for r in self.roles if not r.is_unknown}
        return [
            r
            for r in self.roles
            if r.is_unknown and not any(g in configured_names for g in r.granted_to)
        ]

    @property
    def missing_roles(self) -> list[str]:
        """Configured role names for which no DB role was found."""
        found = {r.role.name for r in self.configured_roles}
        return [name for name in self.expected_roles if name not in found]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_role_status(
    connection: psycopg.Connection,
    name: str,
    role: Role,
    configured_schemas: set[str],
) -> RoleStatus:
    """Build a ``RoleStatus`` for a role known to exist in the database."""
    cursor = connection.cursor()

    # Fetch role attributes
    cursor.execute(
        "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s",
        (name,),
    )
    row = cursor.fetchone()
    can_login = bool(row and row[0])

    # Discover role memberships (GRANT <parent> TO <name>)
    cursor.execute(
        """
        SELECT r.rolname
        FROM pg_auth_members m
        JOIN pg_roles r ON r.oid = m.roleid
        JOIN pg_roles mr ON mr.oid = m.member
        WHERE mr.rolname = %s
        ORDER BY r.rolname
        """,
        (name,),
    )
    granted_to = [row[0] for row in cursor.fetchall()]

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

    return RoleStatus(
        name=name,
        role=role,
        suffix=name[len(role.name) + 1 :] if name != role.name else "",
        schema_permissions=schema_statuses,
        granted_to=granted_to,
        login=can_login,
    )


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
    *,
    include_superusers: bool = False,
) -> list[RoleStatus]:
    """Return roles not in *known_names* that have privileges on *schemas*.

    For each unknown role, all *schemas* are checked for read/write access
    so the caller can see which schemas it can or cannot reach.

    Excludes PostgreSQL built-in roles (``pg_*``).

    Args:
        include_superusers: When ``True``, superusers are included in the
            result.  When ``False`` (default), they are filtered out.
    """
    if not schemas:
        return []

    cursor = connection.cursor()
    if include_superusers:
        cursor.execute(
            """
            SELECT DISTINCT r.rolname, r.rolsuper, r.rolcanlogin
            FROM pg_namespace n
            CROSS JOIN pg_roles r
            WHERE n.nspname = ANY(%s)
              AND (r.rolsuper OR has_schema_privilege(r.rolname, n.nspname, 'USAGE'))
              AND r.rolname NOT LIKE 'pg_%%'
            ORDER BY r.rolname
            """,
            (list(schemas),),
        )
    else:
        cursor.execute(
            """
            SELECT DISTINCT r.rolname, r.rolsuper, r.rolcanlogin
            FROM pg_namespace n
            CROSS JOIN pg_roles r
            WHERE n.nspname = ANY(%s)
              AND has_schema_privilege(r.rolname, n.nspname, 'USAGE')
              AND NOT r.rolsuper
              AND r.rolname NOT LIKE 'pg_%%'
            ORDER BY r.rolname
            """,
            (list(schemas),),
        )

    role_info: dict[str, tuple[bool, bool]] = {}
    for rolname, is_super, can_login in cursor.fetchall():
        if rolname not in known_names:
            role_info[rolname] = (is_super, can_login)

    # For each unknown role, check all configured schemas
    results: list[RoleStatus] = []
    for name, (is_super, can_login) in role_info.items():
        schema_perms = []
        for schema in sorted(schemas):
            has_read = _has_read(connection, name, schema)
            has_write = _has_write(connection, name, schema)
            schema_perms.append(
                SchemaPermissionStatus(
                    schema=schema,
                    expected=None,
                    has_read=has_read,
                    has_write=has_write,
                )
            )

        # Discover memberships (which roles was this role granted?)
        cursor.execute(
            """
            SELECT r.rolname
            FROM pg_auth_members m
            JOIN pg_roles r ON r.oid = m.roleid
            JOIN pg_roles mr ON mr.oid = m.member
            WHERE mr.rolname = %s
            ORDER BY r.rolname
            """,
            (name,),
        )
        granted_to = [row[0] for row in cursor.fetchall()]

        results.append(
            RoleStatus(
                name=name,
                superuser=is_super,
                login=can_login,
                schema_permissions=schema_perms,
                granted_to=granted_to,
            )
        )

    return results


def _find_other_login_roles(
    connection: psycopg.Connection,
    schemas: set[str],
    known_names: set[str],
) -> list[str]:
    """Return login roles that are not superusers and have no access to any *schema*.

    These are database roles that can authenticate but do not have USAGE
    on any of the module's configured schemas.  Built-in PostgreSQL roles
    (``pg_*``) are excluded.

    Args:
        connection: The database connection to use.
        schemas: Set of configured schema names.
        known_names: Role names already accounted for (configured + unknown
            with schema access).  These are excluded from the result.

    Returns:
        Sorted list of role names.
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT r.rolname
        FROM pg_roles r
        WHERE r.rolcanlogin
          AND NOT r.rolsuper
          AND r.rolname NOT LIKE 'pg_%%'
        ORDER BY r.rolname
        """,
    )
    login_roles = [row[0] for row in cursor.fetchall()]

    results: list[str] = []
    for name in login_roles:
        if name in known_names:
            continue
        # Check that the role has no USAGE on any configured schema
        has_access = False
        for schema in schemas:
            cursor.execute(
                "SELECT has_schema_privilege(%s, %s, 'USAGE')",
                (name, schema),
            )
            if cursor.fetchone()[0]:
                has_access = True
                break
        if not has_access:
            results.append(name)

    return results
