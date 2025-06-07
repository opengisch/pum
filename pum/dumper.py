import subprocess
import logging
from enum import Enum

from .exceptions import (
    PgDumpCommandError,
    PgDumpFailed,
    PgRestoreCommandError,
    PgRestoreFailed,
)

logger = logging.getLogger(__name__)


class DumpFormat(Enum):
    CUSTOM = "custom"
    PLAIN = "plain"

    def to_pg_dump_flag(self):
        if self == DumpFormat.CUSTOM:
            return "-Fc"
        elif self == DumpFormat.PLAIN:
            return "-Fp"
        raise ValueError(f"Unknown dump format: {self}")


class Dumper:
    """This class is used to dump and restore a Postgres database."""

    def __init__(self, pg_service: str, dump_path: str):
        self.pg_service = pg_service
        self.dump_path = dump_path

    def pg_dump(
        self,
        dbname: str | None = None,
        *,
        pg_dump_exe: str = "pg_dump",
        exclude_schema: list[str] | None = None,
        format: DumpFormat = DumpFormat.CUSTOM,
    ):
        """
        Call the pg_dump command to dump a db backup

        Args:
            dbname: Name of the database to dump.
            pg_dump_exe: Path to the pg_dump executable.
            exclude_schema: List of schemas to exclude from the dump.
            format: DumpFormat, either custom (default) or plain
        """

        connection = f"service={self.pg_service}"
        if dbname:
            connection = f"{connection} dbname={dbname}"

        command = [
            pg_dump_exe,
            format.to_pg_dump_flag(),
            "--no-owner",
            "--no-privileges",
            "-f",
            self.dump_path,
        ]
        if exclude_schema:
            for schema in exclude_schema:
                command.append(f"--exclude-schema={schema}")
        command.extend(["-d", connection])

        logger.debug("Running pg_dump command: %s", " ".join(command))

        try:
            output = subprocess.run(command, capture_output=True, text=True, check=False)
            if output.returncode != 0:
                logger.error("pg_dump failed: %s", output.stderr)
                raise PgDumpFailed(output.stderr)
        except TypeError:
            logger.error("Invalid command: %s", " ".join(command))
            raise PgDumpCommandError("invalid command: {}".format(" ".join(filter(None, command))))

    def pg_restore(
        self,
        dbname: str | None = None,
        pg_restore_exe: str = "pg_restore",
        exclude_schema: list[str] | None = None,
    ):
        """ """

        connection = f"service={self.pg_service}"
        if dbname:
            connection = f"{connection} dbname={dbname}"

        command = [pg_restore_exe, "-d", connection, "--no-owner"]

        if exclude_schema:
            for schema in exclude_schema:
                command.append(f"--exclude-schema={schema}")
        command.append(self.dump_path)

        logger.debug("Running pg_restore command: %s", " ".join(command))

        try:
            output = subprocess.run(command, capture_output=True, text=True, check=False)
            if output.returncode != 0:
                logger.error("pg_restore failed: %s", output.stderr)
                raise PgRestoreFailed(output.stderr)
        except TypeError:
            logger.error("Invalid command: %s", " ".join(command))
            raise PgRestoreCommandError(
                "invalid command: {}".format(" ".join(filter(None, command)))
            )
