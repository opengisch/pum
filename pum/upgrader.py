#!/usr/bin/env python

import logging

import packaging
import packaging.version
import psycopg
import copy

from .pum_config import PumConfig
from .exceptions import PumException
from .schema_migrations import SchemaMigrations

logger = logging.getLogger(__name__)


class Upgrader:
    """Class to handle the upgrade of a module.
    This class is used to install a new instance or to upgrade an existing instance of a module.
    It stores the info about the upgrade in a table on the database.
    """

    def __init__(
        self,
        config: PumConfig,
        max_version: packaging.version.Version | str | None = None,
    ) -> None:
        """Initialize the Upgrader class.
        This class is used to install a new instance or to upgrade an existing instance of a module.
        Stores the info about the upgrade in a table on the database.
        The table is created in the schema defined in the config file if it does not exist.

        Args:
            connection:
                The database connection to use for the upgrade.
            config:
                The configuration object
            max_version:
                Maximum (including) version to run the deltas up to.

        """
        self.config = config
        self.max_version = packaging.parse(max_version) if max_version else None
        self.schema_migrations = SchemaMigrations(self.config)

    def install(
        self,
        connection: psycopg.Connection | None = None,
        *,
        parameters: dict | None = None,
        max_version: str | packaging.version.Version | None = None,
        roles: bool = False,
        grant: bool = False,
        commit: bool = False,
    ) -> None:
        """Installs the given module
        This will create the schema_migrations table if it does not exist.
        The changelogs are applied in the order they are found in the directory.
        It will also set the baseline version to the current version of the module.

        Args:
            connection:
                The database connection to use for the upgrade.
            parameters:
                The parameters to pass for the migration.
            max_version:
                The maximum version to apply. If None, all versions are applied.
            roles:
                If True, roles will be created.
            grant:
                If True, permissions will be granted to the roles.
            commit:
                If True, the changes will be committed to the database.
        """
        parameters_literals = copy.deepcopy(parameters) if parameters else {}
        for key, value in parameters_literals.items():
            parameters_literals[key] = psycopg.sql.Literal(value)

        if self.schema_migrations.exists(connection):
            msg = (
                f"Schema migrations table {self.config.pum.migration_table_schema}.{self.config.pum.migration_table_name} already exists. "
                "This means that the module is already installed or the database is not empty. "
                "Use upgrade() to upgrade the db or start with a clean db."
            )
            raise PumException(msg)
        self.schema_migrations.create(connection, commit=False)
        for pre_hook in self.config.pre_hook_handlers():
            pre_hook.execute(connection=connection, commit=False, parameters=parameters_literals)
        last_changelog = None
        for changelog in self.config.changelogs(max_version=max_version):
            last_changelog = changelog
            changelog_files = changelog.apply(
                connection, commit=False, parameters=parameters_literals
            )
            changelog_files = [str(f) for f in changelog_files]
            self.schema_migrations.set_baseline(
                connection=connection,
                version=changelog.version,
                beta_testing=False,
                commit=False,
                changelog_files=changelog_files,
                parameters=parameters,
            )
        for post_hook in self.config.post_hook_handlers():
            post_hook.execute(connection=connection, commit=False, parameters=parameters_literals)
        logger.info(
            "Installed %s.%s table and applied changelogs up to version %s",
            self.config.config.pum.migration_table_schema,
            self.config.config.pum.migration_table_name,
            last_changelog.version,
        )

        if roles or grant:
            if not self.config.roles:
                raise PumException(
                    "Roles are requested to be created, but no roles are defined in the configuration."
                )
            self.config.role_manager().create_roles(
                connection=connection, grant=grant, commit=False
            )

        if commit:
            connection.commit()
            logger.info("Changes committed to the database.")
