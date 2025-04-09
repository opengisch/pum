import sys

from pum.config import PumConfig
from pum.schema_migrations import SchemaMigrations
from pum.utils.message_type import MessageType


def run_info(pg_service: str, config: PumConfig, out_fn: callable) -> None:
    """Prints info about the schema migrations.
    Args:
        pg_service (str): The name of the PostgreSQL service to connect to.
        config (PumConfig): An instance of the PumConfig class containing configuration settings for the PUM system.
        out_fn (callable): Function to print output messages.
    """
    try:
        schema_migrations = SchemaMigrations(pg_service, config)
        if not schema_migrations.exists():
            out_fn(
                f"No migrations found in {config.schema_migrations_table}",
                MessageType.WARNING,
            )
        else:
            # Add your logic for when migrations exist; for now, we simply print a message.
            out_fn("Migrations found.", MessageType.OKGREEN)
    except Exception as e:
        out_fn(str(e), MessageType.FAIL)
        sys.exit(1)
