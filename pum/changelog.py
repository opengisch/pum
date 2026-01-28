import logging
from os import listdir
from os.path import basename
from pathlib import Path
from typing import TYPE_CHECKING

from packaging.version import parse as parse_version
import psycopg

from .schema_migrations import SchemaMigrations
from .exceptions import PumInvalidChangelog, PumSqlError
from .sql_content import SqlContent

if TYPE_CHECKING:
    from .feedback import Feedback

logger = logging.getLogger(__name__)


class Changelog:
    """This class represent a changelog directory.
    The directory name is the version of the changelog.
    """

    def __init__(self, dir):
        """Args:
        dir (str): The directory where the changelog is located.

        """
        self.dir = dir
        self.version = parse_version(basename(dir))

    def __repr__(self):
        return f"<dir: {self.dir} (v: {self.version})>"

    def files(self) -> list[Path]:
        """Get the ordered list of SQL files in the changelog directory.
        This is not recursive, it only returns the files in the given changelog directory.

        Returns:
            list[Path]: A list of paths to the changelog files.

        """
        files = [
            self.dir / f
            for f in listdir(self.dir)
            if (self.dir / f).is_file() and f.endswith(".sql")
        ]
        files.sort()
        return files

    def validate(self, parameters: dict | None = None) -> bool:
        """Validate the changelog directory.
        This is done by checking if the directory exists and if it contains at least one SQL file.

        Args:
            parameters: The parameters to pass to the SQL files.

        Raises:
            PumInvalidChangelog: If the changelog directory does not exist or does not contain any SQL files.

        """
        if not self.dir.is_dir():
            raise PumInvalidChangelog(f"Changelog directory `{self.dir}` does not exist.")
        files = self.files()
        if not files:
            raise PumInvalidChangelog(
                f"Changelog directory `{self.dir}` does not contain any SQL files."
            )
        for file in files:
            if not file.is_file():
                raise PumInvalidChangelog(f"Changelog file `{file}` does not exist.")
            if not file.suffix == ".sql":
                raise PumInvalidChangelog(f"Changelog file `{file}` is not a SQL file.")
            try:
                SqlContent(file).validate(parameters=parameters)
            except PumSqlError as e:
                raise PumInvalidChangelog(
                    f"Changelog file `{file}` is not a valid SQL file."
                ) from e

        if not self.version:
            raise PumInvalidChangelog(
                f"Changelog directory `{self.dir}` does not have a valid version."
            )
        return True

    def apply(
        self,
        connection: psycopg.Connection,
        parameters: dict | None = None,
        commit: bool = True,
        schema_migrations: SchemaMigrations | None = None,
        beta_testing: bool = False,
        feedback: "Feedback | None" = None,
    ) -> list[Path]:
        """Apply a changelog
        This will execute all the files in the changelog directory.
        The changelog directory is the one that contains the delta files.

        Args:
            connection: Connection
                The connection to the database
            parameters: dict
                The parameters to pass to the SQL files
            commit: bool
                If true, the transaction is committed. The default is true.
            schema_migrations: SchemaMigrations | None
                The SchemaMigrations instance to use to record the applied changelog.
                If None, the changelog will not be recorded.
            beta_testing: bool
                If true, the changelog will be recorded as a beta testing version.
            feedback: Feedback | None
                Optional feedback object for progress reporting.

        Returns:
            list[Path]
                The list of changelogs that were executed

        """
        logger.info(f"Applying changelog version {self.version} from {self.dir}")

        parameters_literals = SqlContent.prepare_parameters(parameters)
        files = self.files()
        for file in files:
            if feedback:
                feedback.increment_step()
                feedback.report_progress(f"Executing {file.name}")
            try:
                SqlContent(file).execute(
                    connection=connection, commit=commit, parameters=parameters_literals
                )
            except PumSqlError as e:
                raise PumSqlError(f"Error applying changelog {file}: {e}") from e
        if schema_migrations:
            schema_migrations.set_baseline(
                connection=connection,
                version=self.version,
                beta_testing=beta_testing,
                commit=commit,
                changelog_files=[str(f) for f in files],
                parameters=parameters,
            )
        return files

    def is_applied(
        self,
        connection: psycopg.Connection,
        schema_migrations: SchemaMigrations,
    ) -> bool:
        """Check if the changelog has been applied.

        Args:
            connection: The database connection to use.
        Returns:
            bool: True if the changelog has been applied, False otherwise.
        """
        query = psycopg.sql.SQL("""
                SELECT EXISTS (
                    SELECT 1
                    FROM {table}
                    WHERE version = {version}
                )
                """)
        parameters = {
            "version": psycopg.sql.Literal(str(self.version)),
            "table": schema_migrations.migration_table_identifier,
        }
        cursor = SqlContent(query).execute(connection, parameters=parameters)
        result = cursor._pum_results[0] if cursor._pum_results else None
        return result[0] if result else False
