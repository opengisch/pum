import logging
import sys

from .config import PumConfig
from .schema_migrations import SchemaMigrations

logger = logging.getLogger(__name__)


def run_info(pg_service: str, config: PumConfig) -> None:
    """Print info about the schema migrations.

    Args:
        pg_service (str): The name of the PostgreSQL service to connect to.
        config (PumConfig): An instance of the PumConfig class containing configuration settings for the PUM system.
        out_fn (callable): Function to print output messages.

    """
    try:
        schema_migrations = SchemaMigrations(pg_service, config)
        if not schema_migrations.exists():
            logger.info(f"No migrations found in {config.pum_migrations_table}")
        else:
            # Add your logic for when migrations exist; for now, we simply print a message.
            logger.info("Migrations found.")
    except Exception:
        logger.exception("An error occurred while checking for migrations.")
        sys.exit(1)
