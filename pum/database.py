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


def configure_database_connect_access(
    connection_params: dict,
    database_name: str,
    *,
    grant_roles: Iterable[str] | None = None,
    revoke_roles: Iterable[str] | None = None,
    revoke_public: bool = True,
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
    revoke_public:
        Whether to revoke CONNECT from PUBLIC first. Defaults to True.
    """
    conn = psycopg.connect(**connection_params)
    try:
        conn.autocommit = True
        db_ident = psycopg.sql.Identifier(database_name)
        with conn.cursor() as cur:
            if revoke_public:
                logger.info("Revoking CONNECT on database '%s' from PUBLIC…", database_name)
                cur.execute(
                    psycopg.sql.SQL("REVOKE CONNECT ON DATABASE {} FROM PUBLIC").format(db_ident)
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
