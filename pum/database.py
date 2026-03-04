"""Database management utilities (CREATE / DROP)."""

import logging

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
