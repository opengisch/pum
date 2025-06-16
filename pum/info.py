import logging
import sys
import psycopg

from .pum_config import PumConfig
from .schema_migrations import SchemaMigrations, MIGRATION_TABLE_NAME

logger = logging.getLogger(__name__)
# set to info here
logger.setLevel(logging.INFO)


def run_info(connection: psycopg.Connection, config: PumConfig) -> None:
    """Print info about the schema migrations.

    Args:
        connection: The database connection to use for checking migrations.
        config: An instance of the PumConfig class containing configuration settings for the PUM system.

    """
    try:
        schema_migrations = SchemaMigrations(config=config)
        if not schema_migrations.exists(connection=connection):
            logger.info(
                f"No migrations found in {config.config.pum.migration_table_schema}.{MIGRATION_TABLE_NAME}."
            )
        else:
            # Add your logic for when migrations exist; for now, we simply print a message.
            logger.info("Migrations found.")
            logger.info(
                f"Schema migrations table: {config.config.pum.migration_table_schema}.{MIGRATION_TABLE_NAME}"
            )
            logger.info(
                f"Version: {schema_migrations.migration_details(connection=connection)['version']}"
            )
    except Exception:
        logger.exception("An error occurred while checking for migrations.")
        sys.exit(1)
