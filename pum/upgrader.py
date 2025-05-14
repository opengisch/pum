#!/usr/bin/env python

import logging
from pathlib import Path
import psycopg
from psycopg import Connection
import packaging

from .config import PumConfig
from .exceptions import PumException
from .schema_migrations import SchemaMigrations
from .utils.execute_sql import execute_sql
from .changelog import Changelog
from .changelog_utils import list_changelogs, changelog_files

logger = logging.getLogger(__name__)


class Upgrader:
    """
    This class is used to upgrade an existing database using sql delta files.

    Stores the info about the upgrade in a table on the database.
    """

    def __init__(
        self,
        pg_service: str,
        config: PumConfig,
        parameters=None,
        dir: str | Path = ".",
        max_version=None,
    ):
        """
        Initialize the Upgrader class.
        This class is used to install a new instance or to upgrade an existing instance of a module.
        Stores the info about the upgrade in a table on the database.
        The table is created in the schema defined in the config file if it does not exist.

        Args:
            pg_service: str
                The name of the postgres service (defined in pg_service.conf)
                related to the db
            config: PumConfig
                The configuration object
            parameters: dict
                The parameters to pass to the SQL files.
            dir: str | Path
                The directory where the module is located.
            max_version: str
                Maximum (including) version to run the deltas up to.
        """

        self.pg_service = pg_service
        self.config = config
        self.max_version = packaging.parse(max_version) if max_version else None
        self.schema_migrations = SchemaMigrations(self.config)
        self.dir = dir
        self.parameters = parameters

    def install(self, max_version: str | packaging.version.Version | None = None):
        """
        Installs the given module
        This will create the schema_migrations table if it does not exist.
        The changelogs are applied in the order they are found in the directory.
        It will also set the baseline version to the current version of the module.

        Args:
            max_version:
                The maximum version to apply. If None, all versions are applied.
        """

        with psycopg.connect(f"service={self.pg_service}") as conn:
            if self.schema_migrations.exists(conn):
                raise PumException(
                    f"Schema migrations '{self.config.pum_migrations_table}' table already exists. Use upgrade() to upgrade the db or start with a clean db."
                )
            self.schema_migrations.create(conn, commit=False)
            for changelog in list_changelogs(
                config=self.config, dir=self.dir, max_version=max_version
            ):
                changelog_files = self._apply_changelog(
                    conn, changelog, commit=False, parameters=self.parameters
                )
                changelog_files = [str(f) for f in changelog_files]
                self.schema_migrations.set_baseline(
                    conn=conn,
                    version=changelog.version,
                    beta_testing=False,
                    commit=False,
                    changelog_files=changelog_files,
                    parameters=self.parameters,
                )
                for post_hook in self.config.post_hooks:
                    post_hook.execute(
                        conn=conn, commit=False, parameters=self.parameters, dir=self.dir
                    )
            conn.commit()
            logger.info(
                f"Installed {self.config.pum_migrations_table} table and applied changelogs up to version {changelog.version}"
            )

    def _apply_changelog(
        self,
        conn: Connection,
        changelog: Changelog,
        parameters: dict | None = None,
        commit: bool = True,
    ) -> list[Path]:
        """
        Apply a changelog
        This will execute all the files in the changelog directory.
        The changelog directory is the one that contains the delta files.

        Args:
            conn: Connection
                The connection to the database
            changelog: Changelog
                The changelog to apply
            parameters: dict
                The parameters to pass to the SQL files
            commit: bool
                If true, the transaction is committed. The default is true.

        Returns:
            list[Path]
                The list of changelogs that were executed
        """
        files = changelog_files(changelog)
        for file in files:
            try:
                execute_sql(conn=conn, sql=file, commit=commit, parameters=parameters)
            except Exception as e:
                raise PumException(f"Error applying changelog {file}: {e}") from e
        return files
