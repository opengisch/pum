import logging
import sys
import psycopg

from .pum_config import PumConfig
from .schema_migrations import SchemaMigrations

logger = logging.getLogger(__name__)


def run_info(connection: psycopg.Connection, config: PumConfig) -> None:
    """Print info about the schema migrations.

    Args:
        connection: The database connection to use for checking migrations.
        config: An instance of the PumConfig class containing configuration settings for the PUM system.

    """
    try:
        schema_migrations = SchemaMigrations(connection, config)
        if not schema_migrations.exists():
            logger.info(
                f"No migrations found in {config.pum.migration_table_schema}.{config.pum.migration_table_name}."
            )
        else:
            # Add your logic for when migrations exist; for now, we simply print a message.
            logger.info("Migrations found.")
    except Exception:
        logger.exception("An error occurred while checking for migrations.")
        sys.exit(1)
