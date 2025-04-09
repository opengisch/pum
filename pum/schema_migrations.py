"""
pum.core.schema_migrations
~~~~~~~~~~~~~~~~~~~~~~~~~~
This module contains the SchemaMigrations class, which is responsible for
dealing with the database schema migrations for the PUM system.
It provides methods to create the baseline table and set the baseline version
in the database.
"""

import re

from psycopg import connect, sql

from pum.config import PumConfig
from pum.exceptions import PumException


class SchemaMigrations:
    def __init__(self, pg_service: str, pum_config: PumConfig):
        """
        Initialize the SchemaMigrations class with a database connection and configuration.

        Args:
            pg_service (str): The name of the PostgreSQL service to connect to.
            pum_config (PumConfig): An instance of the PumConfig class containing configuration settings for the PUM system.
        """
        ...
        self.connection = connect(f"service='{pg_service}'")
        self.pum_config = pum_config
        self.cursor = self.connection.cursor()

    def exists(self) -> bool:
        """Checks if the schema_migrations information table exists"""

        query = sql.SQL(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = '{schema_migrations_table}'
            )
        """
        ).format(
            schema_migrations_table=sql.Identifier(
                self.pum_config.schema_migrations_table
            )
        )
        self.cursor.execute(query)
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
                self.pum_config.schema_migrations_table
            )
        )
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def create(self):
        """Creates the schema_migrations information table"""

        version = 1

        create_query = sql.SQL(
            """CREATE TABLE IF NOT EXISTS {schema_migrations_table} (
            (
            id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            date_installed timestamp without time zone NOT NULL DEFAULT now(),
            module character varying(50) NOT NULL,
            version character varying(50) NOT NULL,
            beta_testing boolean NOT NULL DEFAULT false,
            schema_migrations_version integer NOT NULL DEFAULT {version},
            );
        """
        ).format(
            baseline_table=sql.Identifier(self.pum_config.schema_migrations_table),
            version=sql.Literal(version),
        )

        comment_query = sql.SQL(
            """COMMENT ON TABLE {schema_migrations_table} IS 'version: 1 --  schema_migration table version';"""
        ).format(baseline_table=sql.Identifier(self.pum_config.schema_migrations_table))

        try:
            self.cursor.execute(create_query)
            self.cursor.execute(comment_query)
        except PumException as e:
            raise PumException(f"Error creating schema migrations table: {e}")

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
