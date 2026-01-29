import json
import logging
import re

import packaging
import packaging.version
import psycopg
import psycopg.sql

from .exceptions import PumSchemaMigrationError, PumSchemaMigrationNoBaselineError
from .sql_content import SqlContent
from .pum_config import PumConfig

logger = logging.getLogger(__name__)

MIGRATION_TABLE_VERSION = 2  # Current schema version
MIGRATION_TABLE_NAME = "pum_migrations"

# TABLE VERSION HISTORY
#
# Version 1:
#    Initial version with columns id, date_installed, module, version,
#    beta_testing, changelog_files, parameters, migration_table_version
#
# Version 2:
#   Changed migration_table_version type to integer and set module NOT NULL (version 2025.1 => 1, module 'tww')


class SchemaMigrations:
    """Manage the schema migrations in the database.
    It provides methods to create the schema_migrations table, check its existence,
    set the baseline version, and retrieve migration details.
    """

    def __init__(self, config: PumConfig) -> None:
        """Initialize the SchemaMigrations class with a database connection and configuration.

        Args:
            config (PumConfig): An instance of the PumConfig class containing configuration settings for the PUM system.

        """
        self.config = config
        self.migration_table_identifier = psycopg.sql.SQL(".").join(
            [
                psycopg.sql.Identifier(self.config.config.pum.migration_table_schema),
                psycopg.sql.Identifier(MIGRATION_TABLE_NAME),
            ]
        )
        self.migration_table_identifier_str = (
            f"{self.config.config.pum.migration_table_schema}.{MIGRATION_TABLE_NAME}"
        )

    def exists(self, connection: psycopg.Connection) -> bool:
        """Check if the schema_migrations information table exists.

        Args:
            connection: The database connection to check for the existence of the table.

        Returns:
            bool: True if the table exists, False otherwise.

        """
        query = psycopg.sql.SQL(
            """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'pum_migrations' AND table_schema = {schema}
        );
        """
        )

        parameters = {
            "schema": psycopg.sql.Literal(self.config.config.pum.migration_table_schema),
        }

        with connection.transaction():
            cursor = SqlContent(query).execute(connection, parameters=parameters)
            result = cursor._pum_results[0] if cursor._pum_results else None
            return result[0] if result else False

    def exists_in_other_schemas(self, connection: psycopg.Connection) -> list[str]:
        """Check if the schema_migrations information table exists in other schemas.

        Args:
            connection: The database connection to check for the existence of the table.

        Returns:
            List[str]: List of schemas where the table exists.

        """
        query = psycopg.sql.SQL(
            """
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_name = 'pum_migrations' AND table_schema != {schema}
        """
        )

        parameters = {
            "schema": psycopg.sql.Literal(self.config.config.pum.migration_table_schema),
        }
        with connection.transaction():
            cursor = SqlContent(query).execute(connection, parameters=parameters)
            return [row[0] for row in (cursor._pum_results or [])]

    def create(
        self,
        connection: psycopg.Connection,
        *,
        allow_multiple_schemas: bool = False,
        commit: bool = False,
    ) -> None:
        """Create the schema_migrations information table
        Args:
            connection: The database connection to create the table.
            commit: If true, the transaction is committed. The default is false.
            allow_multiple_schemas: If true, several pum_migrations tables are allowed in
                distinct schemas. Default is false.
        """
        if self.exists(connection):
            logger.debug(
                f"{self.config.config.pum.migration_table_schema}.pum_migrations table already exists."
            )
            return

        if not allow_multiple_schemas and len(self.exists_in_other_schemas(connection)) > 0:
            raise PumSchemaMigrationError(
                f"Another {self.config.config.pum.migration_table_schema}.{MIGRATION_TABLE_NAME} table exists in another schema (). "
                "Please use the allow_multiple_schemas option to create a new one."
            )

        # Create the schema if it doesn't exist
        parameters = {
            "version": psycopg.sql.Literal(MIGRATION_TABLE_VERSION),
            "schema": psycopg.sql.Identifier(self.config.config.pum.migration_table_schema),
            "table": self.migration_table_identifier,
        }

        create_schema_query = None
        if self.config.config.pum.migration_table_schema != "public":
            create_schema_query = psycopg.sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};")

        create_table_query = psycopg.sql.SQL(
            """CREATE TABLE IF NOT EXISTS {table}
            (
            id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            date_installed timestamp without time zone NOT NULL DEFAULT now(),
            module character varying(50) NOT NULL,
            version character varying(50) NOT NULL,
            beta_testing boolean NOT NULL DEFAULT false,
            changelog_files text[],
            parameters jsonb,
            migration_table_version integer NOT NULL DEFAULT {version}
            );
        """
        )

        comment_query = psycopg.sql.SQL("COMMENT ON TABLE {table} IS 'migration_table_version: 2';")

        if create_schema_query:
            SqlContent(create_schema_query).execute(connection, parameters=parameters)
        SqlContent(create_table_query).execute(connection, parameters=parameters)
        SqlContent(comment_query).execute(connection, parameters=parameters)

        logger.info(f"Created migration table: {self.migration_table_identifier_str}")

        if commit:
            connection.commit()

    def migration_table_version(self, connection: psycopg.Connection) -> int:
        """Return the migration table version.

        Args:
            connection: The database connection to get the migration table version.

        Returns:
            int | None: The migration table version, or None if the table does not exist.

        """
        query = psycopg.sql.SQL(
            """
            SELECT migration_table_version
            FROM {table}
            ORDER BY migration_table_version DESC
            LIMIT 1;
        """
        )

        parameters = {
            "table": self.migration_table_identifier,
        }

        cursor = SqlContent(query).execute(connection, parameters=parameters)
        row = cursor._pum_results[0] if cursor._pum_results else None
        if row is None:
            raise PumSchemaMigrationError(
                f"Migration table {self.migration_table_identifier_str} does not exist."
            )
        return row[0]

    def update_migration_table_schema(self, connection: psycopg.Connection) -> None:
        """Update the migration table schema to the latest version.

        Args:
            connection: The database connection to update the table.

        """
        table_version = self.migration_table_version(connection)
        logger.info(
            f"Updating migration table {self.migration_table_identifier_str} from version {table_version} to {MIGRATION_TABLE_VERSION}."
        )
        if table_version == 1:
            alter_query = psycopg.sql.SQL("""
                                          ALTER TABLE {table} ALTER COLUMN migration_table_version ALTER TYPE integer SET DEFAULT {version} USING 1;
                                          ALTER TABLE {table} ALTER COLUMN module SET NOT NULL USING 'tww';
                                          """)
            parameters = {
                "table": self.migration_table_identifier,
                "version": psycopg.sql.Literal(MIGRATION_TABLE_VERSION),
            }
            SqlContent(alter_query).execute(connection, parameters=parameters)

    def set_baseline(
        self,
        connection: psycopg.Connection,
        version: packaging.version.Version | str,
        changelog_files: list[str] | None = None,
        parameters: dict | None = None,
        *,
        beta_testing: bool = False,
        commit: bool = False,
    ) -> None:
        """Set the baseline into the migration table.

        Args:
            connection: The database connection to set the baseline version.
            version: The version of the current database to set in the information.
            changelog_files: The list of changelog files that were applied.
            parameters: The parameters used in the migration.
            beta_testing: If true, the baseline is set to beta testing mode. The default is false.
            commit: If true, the transaction is committed. The default is False.

        """
        version_str = version
        version_packaging = version
        if isinstance(version_str, packaging.version.Version):
            version_str = str(version_str)
        if isinstance(version_packaging, str):
            version_packaging = packaging.version.parse(version_packaging)
        pattern = re.compile(r"^\d+\.\d+(\.\d+)?$")
        if not re.match(pattern, version_str):
            raise ValueError(f"Wrong version format: {version}. Must be x.y or x.y.z")

        try:
            current = self.baseline(connection=connection)
        except PumSchemaMigrationNoBaselineError:
            current = None
        if current:
            self.update_migration_table_schema(connection)
        if current and current >= version_packaging:
            raise PumSchemaMigrationError(
                f"Cannot set baseline {version_str} as it is already set at {current}."
            )

        code = psycopg.sql.SQL("""
INSERT INTO {table} (
    module,
    version,
    beta_testing,
    migration_table_version,
    changelog_files,
    parameters
) VALUES (
    {module},
    {version},
    {beta_testing},
    {migration_table_version},
    {changelog_files},
    {parameters}
);""")

        query_parameters = {
            "table": self.migration_table_identifier,
            "module": psycopg.sql.Literal(self.config.config.pum.module),
            "version": psycopg.sql.Literal(version_str),
            "beta_testing": psycopg.sql.Literal(beta_testing),
            "migration_table_version": psycopg.sql.Literal(MIGRATION_TABLE_VERSION),
            "changelog_files": psycopg.sql.Literal(changelog_files or []),
            "parameters": psycopg.sql.Literal(json.dumps(parameters or {})),
        }

        logger.info(
            f"Setting baseline version {version} in {self.migration_table_identifier_str} table"
        )
        SqlContent(code).execute(connection, parameters=query_parameters, commit=commit)

    def has_baseline(self, connection: psycopg.Connection) -> bool:
        """Check if the migration table has a baseline version.

        Args:
            connection: The database connection to check for the baseline version.
        Returns:
            bool: True if the baseline version exists, False otherwise.
        """
        try:
            self.baseline(connection=connection)
            return True
        except PumSchemaMigrationError:
            return False

    def baseline(self, connection: psycopg.Connection) -> packaging.version.Version:
        """Return the baseline version from the migration table.

        Args:
            connection: psycopg.Connection
                The database connection to get the baseline version.

        Returns:
            packaging.version.Version | None: The baseline version.

        Raises:
            PumSchemaMigrationError: If the migration table does not exist or if no baseline version is found
            PumSchemaMigrationNoBaselineError: If the migration table does not exist
        """

        if not self.exists(connection=connection):
            raise PumSchemaMigrationError(
                f"{self.migration_table_identifier_str} table does not exist."
            )

        query = psycopg.sql.SQL(
            """
            SELECT version
            FROM {table}
            WHERE id = (
                SELECT id
                FROM {table}
                ORDER BY version DESC, date_installed DESC
                LIMIT 1
            )
        """
        )

        parameters = {
            "table": self.migration_table_identifier,
        }

        with connection.transaction():
            cursor = SqlContent(query).execute(connection, parameters=parameters)
            row = cursor._pum_results[0] if cursor._pum_results else None
            if row is None:
                raise PumSchemaMigrationNoBaselineError(
                    f"Baseline version not found in the {self.migration_table_identifier_str} table."
                )
            return packaging.version.parse(row[0])

    def migration_details(self, connection: psycopg.Connection, version: str | None = None) -> dict:
        """Return the migration details from the migration table.

        Args:
            connection:
                The database connection to get the migration details.
            version:
                The version of the migration to get details for.
                If None, last migration is returned.

        Returns:
            dict: The migration details.

        Raises:
            PumSchemaMigrationError: If the migration table does not exist or if no migration details are found.
        """
        query = None
        if version is None:
            query = psycopg.sql.SQL(
                """
                SELECT *
                FROM {table}
                WHERE id = (
                        SELECT id
                        FROM {table}
                        ORDER BY version DESC, date_installed DESC
                        LIMIT 1
                    )
                ORDER BY date_installed DESC
            """
            )

            parameters = {
                "table": self.migration_table_identifier,
            }
        else:
            query = psycopg.sql.SQL(
                """
                SELECT *
                FROM {table}
                WHERE version = {version}
            """
            )

            parameters = {
                "table": self.migration_table_identifier,
                "version": psycopg.sql.Literal(version),
            }

        with connection.transaction():
            cursor = SqlContent(query).execute(connection, parameters=parameters)
            row = cursor._pum_results[0] if cursor._pum_results else None
            if row is None:
                raise PumSchemaMigrationError(
                    f"Migration details not found for version {version} in the {self.migration_table_identifier_str} table."
                )
            return dict(zip([desc[0] for desc in cursor._pum_description], row, strict=False))

    def compare(self, connection: psycopg.Connection) -> int:
        """Compare the migrations details in the database to the changelogs in the source.

        Args:
            connection: The database connection to get the baseline version.
        Returns:
            int: -1 if database is behind, 0 if up to date.

        Raises:
            PumSchemaMigrationError: If there is a mismatch between the database and the source.
        """

        current_version = self.baseline(connection=connection)
        migration_details = self.migration_details(connection=connection)
        changelogs = [str(changelog.version) for changelog in self.config.changelogs()]

        # Check if the current migration version is in the changelogs
        if migration_details["version"] not in changelogs:
            raise PumSchemaMigrationError(
                f"Changelog for version {migration_details['version']} not found in the source."
            )

        # Check if there are newer changelogs than current version
        for changelog_version in changelogs:
            if packaging.version.parse(changelog_version) > current_version:
                return -1  # database is behind

        return 0  # database is up to date
