"""Database management utilities (CREATE / DROP)."""

import logging
from collections.abc import Iterable

import psycopg
import psycopg.sql

logger = logging.getLogger(__name__)


def create_database(
    connection_params: dict,
    database_name: str,
    *,
    template: str | None = None,
) -> None:
    """Create a new PostgreSQL database.

    Parameters
    ----------
    connection_params:
        Keyword arguments forwarded to :func:`psycopg.connect`
        (e.g. ``{"service": "my_svc", "dbname": "postgres"}``).
    database_name:
        Name of the database to create.
    template:
        Optional template database name
        (used for duplicating an existing database).
    """
    conn = psycopg.connect(**connection_params)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            if template:
                stmt = psycopg.sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    psycopg.sql.Identifier(database_name),
                    psycopg.sql.Identifier(template),
                )
            else:
                stmt = psycopg.sql.SQL("CREATE DATABASE {}").format(
                    psycopg.sql.Identifier(database_name)
                )
            logger.info("Creating database '%s'…", database_name)
            cur.execute(stmt)
    finally:
        conn.close()


def drop_database(connection_params: dict, database_name: str) -> None:
    """Drop a PostgreSQL database, terminating active connections first.

    Parameters
    ----------
    connection_params:
        Keyword arguments forwarded to :func:`psycopg.connect`
        (must connect to a **different** database, e.g. ``postgres``).
    database_name:
        Name of the database to drop.
    """
    conn = psycopg.connect(**connection_params)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                [database_name],
            )
            logger.info("Dropping database '%s'…", database_name)
            cur.execute(
                psycopg.sql.SQL("DROP DATABASE {}").format(psycopg.sql.Identifier(database_name))
            )
    finally:
        conn.close()


def get_database_connect_access(
    connection: psycopg.Connection,
    database_name: str,
) -> tuple[bool, list[str]]:
    """Query current CONNECT privileges on a database.

    Parameters
    ----------
    connection:
        An existing database connection (any database on the same cluster).
    database_name:
        Name of the target database to inspect.

    Returns
    -------
    tuple[bool, list[str]]
        ``(public_has_connect, roles_with_connect)`` where
        *public_has_connect* is ``True`` when the ``PUBLIC`` pseudo-role
        has ``CONNECT``, and *roles_with_connect* is a list of role names
        that have been explicitly granted ``CONNECT``.

        When the database ACL is ``NULL`` (PostgreSQL default), ``PUBLIC``
        is considered to have ``CONNECT`` and the role list is empty.
    """
    row = connection.execute(
        "SELECT datacl::text[] FROM pg_database WHERE datname = %s",
        [database_name],
    ).fetchone()

    if row is None:
        raise ValueError(f"Database {database_name!r} not found")

    datacl = row[0]

    if datacl is None:
        # Default ACL: PUBLIC has all default privileges including CONNECT
        return True, []

    public_has_connect = False
    roles_with_connect: list[str] = []

    for entry in datacl:
        # ACL entry format: "grantee=privileges/grantor"
        # PUBLIC entries have empty grantee: "=privileges/grantor"
        eq_pos = entry.index("=")
        slash_pos = entry.index("/")
        grantee = entry[:eq_pos]
        privileges = entry[eq_pos + 1 : slash_pos]

        if "c" in privileges:  # 'c' = CONNECT
            if grantee == "":
                public_has_connect = True
            else:
                roles_with_connect.append(grantee)

    return public_has_connect, roles_with_connect


def configure_database_connect_access(
    connection_params: dict,
    database_name: str,
    *,
    grant_roles: Iterable[str] | None = None,
    revoke_roles: Iterable[str] | None = None,
    public: bool | None = None,
) -> None:
    """Configure CONNECT privileges on an existing PostgreSQL database.

    This function is intended for already-existing databases where CONNECT
    access needs to be tightened after initial setup.

    Parameters
    ----------
    connection_params:
        Keyword arguments forwarded to :func:`psycopg.connect`
        (must connect as a role allowed to alter database privileges).
    database_name:
        Name of the target database whose CONNECT privileges are managed.
    grant_roles:
        Roles to grant CONNECT on the target database.
    revoke_roles:
        Roles to revoke CONNECT from on the target database.
    public:
        Controls CONNECT for the PUBLIC pseudo-role.
        ``True`` grants CONNECT to PUBLIC, ``False`` revokes it,
        ``None`` (default) leaves it unchanged.
    """
    conn = psycopg.connect(**connection_params)
    try:
        conn.autocommit = True
        db_ident = psycopg.sql.Identifier(database_name)
        with conn.cursor() as cur:
            if public is False:
                logger.info("Revoking CONNECT on database '%s' from PUBLIC…", database_name)
                cur.execute(
                    psycopg.sql.SQL("REVOKE CONNECT ON DATABASE {} FROM PUBLIC").format(db_ident)
                )
            elif public is True:
                logger.info("Granting CONNECT on database '%s' to PUBLIC…", database_name)
                cur.execute(
                    psycopg.sql.SQL("GRANT CONNECT ON DATABASE {} TO PUBLIC").format(db_ident)
                )

            for role in revoke_roles or []:
                logger.info(
                    "Revoking CONNECT on database '%s' from role '%s'…", database_name, role
                )
                cur.execute(
                    psycopg.sql.SQL("REVOKE CONNECT ON DATABASE {} FROM {}").format(
                        db_ident,
                        psycopg.sql.Identifier(role),
                    )
                )

            for role in grant_roles or []:
                logger.info("Granting CONNECT on database '%s' to role '%s'…", database_name, role)
                cur.execute(
                    psycopg.sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                        db_ident,
                        psycopg.sql.Identifier(role),
                    )
                )
    finally:
        conn.close()
