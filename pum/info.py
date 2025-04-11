import logging
import sys

from pum.config import PumConfig
from pum.schema_migrations import SchemaMigrations

logger = logging.getLogger(__name__)


def run_info(pg_service: str, config: PumConfig) -> None:
    """Prints info about the schema migrations.
    Args:
        pg_service (str): The name of the PostgreSQL service to connect to.
        config (PumConfig): An instance of the PumConfig class containing configuration settings for the PUM system.
        out_fn (callable): Function to print output messages.
    """
    try:
        schema_migrations = SchemaMigrations(pg_service, config)
        if not schema_migrations.exists():
            logger.info(f"No migrations found in {config.schema_migrations_table}")
        else:
            # Add your logic for when migrations exist; for now, we simply print a message.
            logger.info("Migrations found.")
    except Exception as e:
        logger.error(str(e))
        sys.exit(1)
