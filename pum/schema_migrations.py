"""
pum.core.schema_migrations
~~~~~~~~~~~~~~~~~~~~~~~~~~
This module contains the SchemaMigrations class, which is responsible for
dealing with the database schema migrations for the PUM system.
It provides methods to create the baseline table and set the baseline version
in the database.
"""

import logging
import re

from psycopg import connect, sql

from pum.config import PumConfig
from pum.utils.execute_sql import execute_sql

logger = logging.getLogger(__name__)


class SchemaMigrations:
    def __init__(self, pg_service: str, config: PumConfig):
        """
        Initialize the SchemaMigrations class with a database connection and configuration.

        Args:
            pg_service (str): The name of the PostgreSQL service to connect to.
            pum_config (PumConfig): An instance of the PumConfig class containing configuration settings for the PUM system.
        """
        ...
        self.connection = connect(f"service='{pg_service}'")
        self.config = config
        self.cursor = self.connection.cursor()

    def exists(self) -> bool:
        """Checks if the schema_migrations information table exists"""
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
            )
        """
        ).format(
            schema=sql.Literal(schema),
            table=sql.Literal(table),
        )
        execute_sql(self.cursor, query)
        return self.cursor.fetchone()[0]

    def installed_modules(self):
        query = sql.SQL(
            """
            SELECT module, version
            FROM (
            SELECT module, version,
                   ROW_NUMBER() OVER (PARTITION BY module ORDER BY date_installed DESC) AS rn
            FROM {schema_migrations_table}
            ) t
            WHERE t.rn = 1;
            """
        ).format(
            schema_migrations_table=sql.Identifier(
                *self.config.schema_migrations_table.split(".")
            )
        )
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def create(self, commit: bool = True):
        """
        Creates the schema_migrations information table
        Args:
            commit (bool): If true, the transaction is committed. The default is true.
        """

        if self.exists():
            logger.info(f"{self.config.schema_migrations_table} table already exists")
            return

        version = 1

        create_query = sql.SQL(
            """CREATE TABLE IF NOT EXISTS {schema_migrations_table}
            (
            id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            date_installed timestamp without time zone NOT NULL DEFAULT now(),
            module character varying(50) NOT NULL,
            version character varying(50) NOT NULL,
            beta_testing boolean NOT NULL DEFAULT false,
            changelog_files text[],
            schema_migrations_version integer NOT NULL DEFAULT {version}
            );
        """
        ).format(
            schema_migrations_table=sql.Identifier(
                *self.config.schema_migrations_table.split(".")
            ),
            version=sql.Literal(version),
        )

        comment_query = sql.SQL(
            """COMMENT ON TABLE {schema_migrations_table} IS 'version: 1 --  schema_migration table version';"""
        ).format(
            schema_migrations_table=sql.Identifier(
                *self.config.schema_migrations_table.split(".")
            )
        )

        execute_sql(self.cursor, create_query)
        execute_sql(self.cursor, comment_query)

        logger.info(f"Created {self.config.schema_migrations_table} table")

        if commit:
            self.connection.commit()

    def set_baseline(self, version, beta_testing: bool = False):
        """Sets the baseline into the creation information table

        version: str
            The version of the current database to set in the information
            table. The baseline must be in the format x.x.x where x are numbers.
        beta_testing: bool
            If true, the baseline is set to beta testing mode. The default is false.
        """
        pattern = re.compile(r"^\d+\.\d+\.\d+$")
        if not re.match(pattern, version):
            raise ValueError("Wrong version format")

        query = sql.SQL(
            """
            INSERT INTO {upgrades_table} (
                version,
                module,
                beta_testing,
            ) VALUES (
                %s, %s, %s
            )
        """
        ).format(upgrades_table=sql.Identifier(self.upgrades_table))
        self.cursor.execute(query, (version, self.pum_config.module, beta_testing))
        self.cursor.execute(query)
        self.connection.commit()
