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
        skip_drop_app: bool = False,
        skip_create_app: bool = False,
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
                This means that the module will not be allowed to receive any future updates.
                We strongly discourage using this for production.
            skip_drop_app:
                If True, drop app handlers will be skipped.
            skip_create_app:
                If True, create app handlers will be skipped.
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

        logger.info("Installing module...")

        if roles or grant:
            self.config.role_manager().create_roles(
                connection=connection, grant=False, commit=False
            )

        if not skip_drop_app:
            for drop_app_hook in self.config.drop_app_handlers():
                drop_app_hook.execute(connection=connection, commit=False, parameters=parameters)

        last_changelog = None
        for changelog in self.config.changelogs(max_version=max_version):
            last_changelog = changelog
            changelog.apply(
                connection,
                commit=False,
                parameters=parameters,
                schema_migrations=self.schema_migrations,
                beta_testing=beta_testing,
            )

        if not skip_create_app:
            for create_app_hook in self.config.create_app_handlers():
                create_app_hook.execute(connection=connection, commit=False, parameters=parameters)

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
        grant: bool = True,
        skip_drop_app: bool = False,
        skip_create_app: bool = False,
    ) -> None:
        """Install demo data for the module.

        Args:
            connection: The database connection to use.
            name: The name of the demo data to install.
            parameters: The parameters to pass to the demo data SQL.
            grant: If True, grant permissions to the roles after installing the demo data. Default is True.
            skip_drop_app: If True, skip drop app handlers during demo data installation. Default is False.
            skip_create_app: If True, skip create app handlers during demo data installation. Default is False.
        """
        if name not in self.config.demo_data():
            raise PumException(f"Demo data '{name}' not found in the configuration.")

        logger.info(f"Installing demo data {name}")

        if not skip_drop_app:
            for drop_app_hook in self.config.drop_app_handlers():
                drop_app_hook.execute(connection=connection, commit=False, parameters=parameters)

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

        if not skip_create_app:
            for create_app_hook in self.config.create_app_handlers():
                create_app_hook.execute(connection=connection, commit=False, parameters=parameters)

        connection.commit()

        if grant:
            self.config.role_manager().grant_permissions(connection=connection, commit=False)

        connection.commit()

        logger.info("Demo data '%s' installed successfully.", name)

    def upgrade(
        self,
        connection: psycopg.Connection,
        *,
        parameters: dict | None = None,
        max_version: str | packaging.version.Version | None = None,
        beta_testing: bool = False,
        force: bool = False,
        skip_drop_app: bool = False,
        skip_create_app: bool = False,
        grant: bool = True,
    ) -> None:
        """Upgrades the given module
        The changelogs are applied in the order they are found in the directory.

        Args:
            connection:
                The database connection to use for the upgrade.
            parameters:
                The parameters to pass for the migration.
            max_version:
                The maximum version to apply. If None, all versions are applied.
            beta_testing:
                If True, the module is upgraded in beta testing mode.
                This means that the module will not be allowed to receive any future updates.
                We strongly discourage using this for production.
            force:
                If True, allow upgrading a module that is installed in beta testing mode.
            skip_drop_app:
                If True, drop app handlers will be skipped.
            skip_create_app:
                If True, create app handlers will be skipped.
            grant:
                If True, permissions will be granted to the roles.
        """
        if not self.schema_migrations.exists(connection):
            msg = (
                f"Schema migrations table {self.config.config.pum.migration_table_schema}.pum_migrations does not exist. "
                "This means that the module is not installed yet. Use install() to install the module."
            )
            raise PumException(msg)

        migration_details = self.schema_migrations.migration_details(connection)
        installed_beta_testing = bool(migration_details.get("beta_testing", False))
        if installed_beta_testing and not force:
            msg = (
                "This module is installed in beta testing mode, upgrades are disabled. "
                "Re-run with force=True (or --force in the CLI) if you really want to upgrade anyway."
            )
            raise PumException(msg)

        effective_beta_testing = beta_testing or installed_beta_testing

        logger.info("Starting upgrade process...")

        if not skip_drop_app:
            for drop_app_hook in self.config.drop_app_handlers():
                drop_app_hook.execute(connection=connection, commit=False, parameters=parameters)

        for changelog in self.config.changelogs(max_version=max_version):
            if changelog.version <= self.schema_migrations.baseline(connection):
                if not changelog.is_applied(
                    connection=connection, schema_migrations=self.schema_migrations
                ):
                    msg = (
                        f"Changelog version {changelog.version} is lower than or equal to the current version "
                        f"{self.schema_migrations.current_version(connection)} but not applied. "
                        "This indicates a problem with the database state."
                    )
                    logger.error(msg)
                    raise PumException(msg)
                logger.debug("Changelog version %s already applied, skipping.", changelog.version)
                continue

            changelog.apply(
                connection,
                commit=False,
                parameters=parameters,
                schema_migrations=self.schema_migrations,
                beta_testing=effective_beta_testing,
            )

        if not skip_create_app:
            for create_app_hook in self.config.create_app_handlers():
                create_app_hook.execute(connection=connection, commit=False, parameters=parameters)

        if grant:
            self.config.role_manager().grant_permissions(connection=connection, commit=False)

        connection.commit()
        logger.info("Upgrade completed and changes committed to the database.")
