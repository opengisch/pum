import subprocess
import logging
from packaging.version import Version

from .exceptions import (
    PgDumpCommandError,
    PgDumpFailed,
    PgRestoreCommandError,
    PgRestoreFailed,
)

logger = logging.getLogger(__name__)


class Dumper:
    """This class is used to dump and restore a Postgres database."""

    def __init__(self, pg_service: str, file: str):
        self.file = file
        self.pg_service = pg_service

    def pg_backup(self, pg_dump_exe: str = "pg_dump", exclude_schema: list[str] = None):
        """Call the pg_dump command to create a db backup

        Parameters
        ----------
        pg_dump_exe: str
            the pg_dump command path
        exclude_schema: list[str]
            list of schemas to be skipped
        """
        command = [pg_dump_exe, "-Fc", "-f", self.file, f"service={self.pg_service}"]
        if exclude_schema:
            for schema in exclude_schema:
                command.insert(-1, f"--exclude-schema={schema}")

        try:
            output = subprocess.run(command, capture_output=True, text=True, check=False)
            if output.returncode != 0:
                logger.error("pg_dump failed: %s", output.stderr)
                raise PgDumpFailed(output.stderr)
        except TypeError:
            logger.error("Invalid command: %s", " ".join(command))
            raise PgDumpCommandError("invalid command: {}".format(" ".join(filter(None, command))))

    def pg_restore(self, pg_restore_exe: str = "pg_restore", exclude_schema: list[str] = None):
        """Call the pg_restore command to restore a db backup

        Parameters
        ----------
        pg_restore_exe: str
            the pg_restore command path
        exclude_schema: list[str]
            list of schemas to be skipped
        """
        command = [pg_restore_exe, "-d", f"service={self.pg_service}", "--no-owner"]

        if exclude_schema:
            exclude_schema_available = False
            try:
                pg_version_output = subprocess.check_output(
                    [pg_restore_exe, "--version"], text=True
                )
                pg_version = pg_version_output.strip().split()[-1]
                exclude_schema_available = Version(pg_version) >= Version("10.0")
            except subprocess.CalledProcessError as e:
                logger.error("Could not get pg_restore version: %s", e.stderr)
            except Exception as e:
                logger.error("Error checking pg_restore version: %s", e)
            if exclude_schema_available:
                for schema in exclude_schema:
                    command.append(f"--exclude-schema={schema}")
        command.append(self.file)

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
