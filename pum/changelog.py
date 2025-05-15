from os.path import basename
from packaging.version import parse as parse_version
from pathlib import Path
from os import listdir
from .exceptions import PumInvalidChangelog, PumSqlException, PumInvalidSqlFile
from psycopg import Connection
from .utils.execute_sql import execute_sql
from .utils.sql_chunks_from_file import sql_chunks_from_file


class Changelog:
    """
    This class represent a changelog directory.
    The directory name is the version of the changelog.
    """

    def __init__(self, dir):
        """
        Args:
            dir (str): The directory where the changelog is located.
        """
        self.dir = dir
        self.version = parse_version(basename(dir))

    def __repr__(self):
        return f"<dir: {self.dir} (v: {self.version})>"

    def files(self) -> list[Path]:
        """
        Get the ordered list of SQL files in the changelog directory.
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

    def validate(self):
        """
        Validate the changelog directory.
        This is done by checking if the directory exists and if it contains at least one SQL file.

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
                sql_chunks_from_file(file)
            except PumInvalidSqlFile as e:
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
        connection: Connection,
        parameters: dict | None = None,
        commit: bool = True,
    ) -> list[Path]:
        """
        Apply a changelog
        This will execute all the files in the changelog directory.
        The changelog directory is the one that contains the delta files.

        Args:
            connection: Connection
                The connection to the database
            parameters: dict
                The parameters to pass to the SQL files
            commit: bool
                If true, the transaction is committed. The default is true.

        Returns:
            list[Path]
                The list of changelogs that were executed
        """
        files = self.files()
        for file in files:
            try:
                assert isinstance(file, Path)
                execute_sql(connection=connection, sql=file, commit=commit, parameters=parameters)
            except Exception as e:
                raise PumSqlException(f"Error applying changelog {file}: {e}") from e
        return files
