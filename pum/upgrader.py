#!/usr/bin/env python

import logging
import packaging.version
import psycopg
import packaging

from .config import PumConfig
from .exceptions import PumException
from .schema_migrations import SchemaMigrations

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
        parameters: dict | None = None,
        max_version: packaging.version.Version | str | None = None,
    ):
        """
        Initialize the Upgrader class.
        This class is used to install a new instance or to upgrade an existing instance of a module.
        Stores the info about the upgrade in a table on the database.
        The table is created in the schema defined in the config file if it does not exist.

        Args:
            pg_service:
                The name of the postgres service (defined in pg_service.conf)
                related to the db
            config:
                The configuration object
            parameters:
                The parameters to pass to the SQL files.
            max_version:
                Maximum (including) version to run the deltas up to.
        """

        self.pg_service = pg_service
        self.config = config
        self.max_version = packaging.parse(max_version) if max_version else None
        self.schema_migrations = SchemaMigrations(self.config)
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

        with psycopg.connect(f"service={self.pg_service}") as connection:
            if self.schema_migrations.exists(connection):
                raise PumException(
                    f"Schema migrations '{self.config.pum_migrations_table}' table already exists. Use upgrade() to upgrade the db or start with a clean db."
                )
            self.schema_migrations.create(connection, commit=False)
            for changelog in self.config.list_changelogs(max_version=max_version):
                changelog_files = changelog.apply(
                    connection, commit=False, parameters=self.parameters
                )
                changelog_files = [str(f) for f in changelog_files]
                self.schema_migrations.set_baseline(
                    connection=connection,
                    version=changelog.version,
                    beta_testing=False,
                    commit=False,
                    changelog_files=changelog_files,
                    parameters=self.parameters,
                )
                for post_hook in self.config.post_hooks:
                    post_hook.execute(
                        connection=connection, commit=False, parameters=self.parameters
                    )
            connection.commit()
            logger.info(
                f"Installed {self.config.pum_migrations_table} table and applied changelogs up to version {changelog.version}"
            )
