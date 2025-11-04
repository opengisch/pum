#!/usr/bin/env python
import logging

import packaging
import packaging.version
import psycopg

from .pum_config import PumConfig
from .exceptions import PumException
from .schema_migrations import SchemaMigrations
from .sql_content import SqlContent


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
        connection: psycopg.Connection = None,
        *,
        parameters: dict | None = None,
        max_version: str | packaging.version.Version | None = None,
        roles: bool = False,
        grant: bool = False,
        beta_testing: bool = False,
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
            beta_testing:
                If True, the module is installed in beta testing mode.
                This means that the module will not be able to receive any future updates.
                We strongly discourage using this for production.
            commit:
                If True, the changes will be committed to the database.
        """
        if self.schema_migrations.exists(connection):
            msg = (
                f"Schema migrations table {self.config.config.pum.migration_table_schema}.pum_migrations already exists. "
                "This means that the module is already installed or the database is not empty. "
                "Use upgrade() to upgrade the db or start with a clean db."
            )
            raise PumException(msg)
        self.schema_migrations.create(connection, commit=False)

        if roles or grant:
            self.config.role_manager().create_roles(
                connection=connection, grant=False, commit=False
            )

        for pre_hook in self.config.pre_hook_handlers():
            pre_hook.execute(connection=connection, commit=False, parameters=parameters)

        parameters_literals = SqlContent.prepare_parameters(parameters)
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
                beta_testing=beta_testing,
                commit=False,
                changelog_files=changelog_files,
                parameters=parameters,
            )

        for post_hook in self.config.post_hook_handlers():
            post_hook.execute(connection=connection, commit=False, parameters=parameters)

        logger.info(
            "Installed %s.pum_migrations table and applied changelogs up to version %s",
            self.config.config.pum.migration_table_schema,
            last_changelog.version,
        )

        if grant:
            self.config.role_manager().grant_permissions(connection=connection, commit=False)

        if commit:
            connection.commit()
            logger.info("Changes committed to the database.")

    def install_demo_data(
        self,
        connection: psycopg.Connection,
        name: str,
        *,
        parameters: dict | None = None,
    ) -> None:
        """Install demo data for the module.

        Args:
            connection: The database connection to use.
            name: The name of the demo data to install.
            parameters: The parameters to pass to the demo data SQL.
        """
        if name not in self.config.demo_data():
            raise PumException(f"Demo data '{name}' not found in the configuration.")

        logger.info(f"Installing demo data {name}")

        for pre_hook in self.config.pre_hook_handlers():
            pre_hook.execute(connection=connection, commit=False, parameters=parameters)

        connection.commit()

        parameters_literals = SqlContent.prepare_parameters(parameters)
        for demo_data_file in self.config.demo_data()[name]:
            demo_data_file = self.config.base_path / demo_data_file
            SqlContent(sql=demo_data_file).execute(
                connection=connection,
                commit=False,
                parameters=parameters_literals,
            )

        connection.commit()

        for post_hook in self.config.post_hook_handlers():
            post_hook.execute(connection=connection, commit=False, parameters=parameters)

        connection.commit()

        logger.info("Demo data '%s' installed successfully.", name)
