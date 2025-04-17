"""
pum.core.schema_migrations
~~~~~~~~~~~~~~~~~~~~~~~~~~
This module contains the SchemaMigrations class, which is responsible for
dealing with the database schema migrations for the PUM system.
It provides methods to create the baseline table and set the baseline version
in the database.
"""

import json
import logging
import re

from packaging.version import Version
from psycopg import Connection, connect, sql

from pum.config import PumConfig
from pum.utils.execute_sql import execute_sql

logger = logging.getLogger(__name__)

migration_table_version = "2025.0"


class SchemaMigrations:
    def __init__(self, config: PumConfig):
        """
        Initialize the SchemaMigrations class with a database connection and configuration.

        Args:
            config (PumConfig): An instance of the PumConfig class containing configuration settings for the PUM system.
        """
        self.config = config

    def exists(self, conn: Connection) -> bool:
        """
        Checks if the schema_migrations information table exists
        Args:
            conn (Connection): The database connection to check for the existence of the table.
            Returns:
                bool: True if the table exists, False otherwise."""
        schema = "public"
        table = None
        table_identifiers = self.config.schema_migrations_table.split(".")
        if len(table_identifiers) > 2:
            raise ValueError(
                "The schema_migrations_table must be in the format 'schema.table'"
            )
        elif len(table_identifiers) == 2:
            schema = table_identifiers[0]
            table = table_identifiers[1]
        else:
            table = table_identifiers[0]

        query = sql.SQL(
            """
            SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = {table} AND table_schema = {schema}
            );
            """
        ).format(
            schema=sql.Literal(schema),
            table=sql.Literal(table),
        )

        cursor = execute_sql(conn, query)
        return cursor.fetchone()[0]

    def create(self, conn: Connection, commit: bool = True):
        """
        Creates the schema_migrations information table
        Args:
            conn (Connection): The database connection to create the table.
            commit (bool): If true, the transaction is committed. The default is true.
        """

        if self.exists(conn):
            logger.debug(f"{self.config.schema_migrations_table} table already exists")
            return

        create_query = sql.SQL(
            """CREATE TABLE IF NOT EXISTS {schema_migrations_table}
            (
            id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            date_installed timestamp without time zone NOT NULL DEFAULT now(),
            module character varying(50), -- TODO: NOT NULL,
            version character varying(50) NOT NULL,
            beta_testing boolean NOT NULL DEFAULT false,
            changelog_files text[],
            parameters jsonb,
            migration_table_version character varying(50) NOT NULL DEFAULT {version}
            );
        """
        ).format(
            schema_migrations_table=sql.Identifier(
                *self.config.schema_migrations_table.split(".")
            ),
            version=sql.Literal(migration_table_version),
        )

        comment_query = sql.SQL(
            """COMMENT ON TABLE {schema_migrations_table} IS 'version: 1 --  schema_migration table version';"""
        ).format(
            schema_migrations_table=sql.Identifier(
                *self.config.schema_migrations_table.split(".")
            )
        )

        execute_sql(conn, create_query)
        execute_sql(conn, comment_query)

        logger.info(f"Created {self.config.schema_migrations_table} table")

        if commit:
            conn.commit()

    def set_baseline(
        self,
        conn: Connection,
        version: Version | str,
        beta_testing: bool = False,
        commit: bool = True,
        changelog_files: list[str] = None,
        parameters: dict = None,
    ):
        """
        Sets the baseline into the migration table

        Args:
            conn: Connection
                The database connection to set the baseline version.
            version: Version | str
                The version of the current database to set in the information.
            beta_testing: bool
                If true, the baseline is set to beta testing mode. The default is false.
            commit: bool
                If true, the transaction is committed. The default is true.
        """
        if isinstance(version, Version):
            version = str(version)
        pattern = re.compile(r"^\d+\.\d+\.\d+$")
        if not re.match(pattern, version):
            raise ValueError(f"Wrong version format: {version}. Must be x.x.x")

        query = sql.SQL(
            """
            INSERT INTO {schema_migrations_table} (
            version,
            beta_testing,
            migration_table_version,
            changelog_files,
            parameters
            ) VALUES (
            {version},
            {beta_testing},
            {migration_table_version},
            {changelog_files},
            {parameters}
            )
        """
        ).format(
            version=sql.Literal(version),
            beta_testing=sql.Literal(beta_testing),
            migration_table_version=sql.Literal(migration_table_version),
            schema_migrations_table=sql.Identifier(
                *self.config.schema_migrations_table.split(".")
            ),
            changelog_files=sql.Literal(changelog_files or []),
            parameters=sql.Literal(json.dumps(parameters or {})),
        )
        logger.info(
            f"Setting baseline version {version} in {self.config.schema_migrations_table}"
        )
        conn.execute(query)
        if commit:
            conn.commit()

    def baseline(self, conn: Connection) -> str:
        """
        Returns the baseline version from the migration table
        Args:
            conn: Connection
                The database connection to get the baseline version.
        Returns:
            str: The baseline version.
        """
        query = sql.SQL(
            """
            SELECT version
            FROM {schema_migrations_table}
            WHERE id = (
                SELECT id
                FROM {schema_migrations_table}
                ORDER BY date_installed DESC
                LIMIT 1
            )
        """
        ).format(
            schema_migrations_table=sql.Identifier(
                *self.config.schema_migrations_table.split(".")
            )
        )
        cursor = execute_sql(conn, query)
        return cursor.fetchone()[0]

    def migration_details(self, conn: Connection, version: str = None) -> dict:
        """
        Returns the migration details from the migration table
        Args:
            conn: Connection
                The database connection to get the migration details.
            version: str
                The version of the migration to get details for. If None, last migration is returned.
        Returns:
            dict: The migration details.
        """
        query = None
        if version is None:
            query = sql.SQL(
                """
                SELECT *
                FROM {schema_migrations_table}
                WHERE id = (
                        SELECT id
                        FROM {schema_migrations_table}
                        ORDER BY date_installed DESC
                        LIMIT 1
                    )
                ORDER BY date_installed DESC
            """
            ).format(
                schema_migrations_table=sql.Identifier(
                    *self.config.schema_migrations_table.split(".")
                ),
            )
        else:
            query = sql.SQL(
                """
                SELECT *
                FROM {schema_migrations_table}
                WHERE version = {version}
            """
            ).format(
                schema_migrations_table=sql.Identifier(
                    *self.config.schema_migrations_table.split(".")
                ),
                version=sql.Literal(version),
            )
        cursor = execute_sql(conn, query)
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([desc[0] for desc in cursor.description], row))
