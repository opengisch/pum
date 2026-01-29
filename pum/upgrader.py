#!/usr/bin/env python
import logging

import packaging
import packaging.version
import psycopg

from .pum_config import PumConfig
from .exceptions import PumException
from .schema_migrations import SchemaMigrations
from .sql_content import SqlContent
from .feedback import Feedback, LogFeedback


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
        self.max_version = packaging.version.parse(max_version) if max_version else None
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
        feedback: Feedback | None = None,
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
            feedback:
                A Feedback instance to report progress and check for cancellation.
                If None, a LogFeedback instance will be used.
        """
        if feedback is None:
            feedback = LogFeedback()

        if self.schema_migrations.exists(connection):
            msg = (
                f"Schema migrations table {self.config.config.pum.migration_table_schema}.pum_migrations already exists. "
                "This means that the module is already installed or the database is not empty. "
                "Use upgrade() to upgrade the db or start with a clean db."
            )
            raise PumException(msg)

        feedback.report_progress("Creating migrations table...")
        self.schema_migrations.create(connection, commit=False)

        logger.info("Installing module...")
        feedback.report_progress("Installing module...")

        # Calculate total steps: drop handlers + all SQL files in changelogs + create handlers + role operations
        drop_handlers = self.config.drop_app_handlers() if not skip_drop_app else []
        changelogs = list(self.config.changelogs(max_version=max_version))
        create_handlers = self.config.create_app_handlers() if not skip_create_app else []

        total_changelog_files = sum(len(changelog.files()) for changelog in changelogs)

        # Count role operations
        role_steps = 0
        if roles or grant:
            role_manager = self.config.role_manager()
            role_steps += len(role_manager.roles)  # create roles
            if grant:
                role_steps += len(role_manager.roles)  # grant permissions

        total_steps = len(drop_handlers) + total_changelog_files + len(create_handlers) + role_steps
        feedback.set_total_steps(total_steps)

        if roles or grant:
            feedback.report_progress("Creating roles...")
            self.config.role_manager().create_roles(
                connection=connection, grant=False, commit=False, feedback=feedback
            )

        if not skip_drop_app:
            for drop_app_hook in drop_handlers:
                if feedback.is_cancelled():
                    raise PumException("Installation cancelled by user")
                feedback.increment_step()
                feedback.report_progress(
                    f"Executing drop app handler: {drop_app_hook.file or 'SQL code'}"
                )
                drop_app_hook.execute(connection=connection, commit=False, parameters=parameters)

        last_changelog = None
        for changelog in changelogs:
            if feedback.is_cancelled():
                raise PumException("Installation cancelled by user")
            last_changelog = changelog
            changelog.apply(
                connection,
                commit=False,
                parameters=parameters,
                schema_migrations=self.schema_migrations,
                beta_testing=beta_testing,
                feedback=feedback,
            )

        if not skip_create_app:
            for create_app_hook in create_handlers:
                if feedback.is_cancelled():
                    raise PumException("Installation cancelled by user")
                feedback.increment_step()
                feedback.report_progress(
                    f"Executing create app handler: {create_app_hook.file or 'SQL code'}"
                )
                create_app_hook.execute(connection=connection, commit=False, parameters=parameters)

        logger.info(
            "Installed %s.pum_migrations table and applied changelogs up to version %s",
            self.config.config.pum.migration_table_schema,
            last_changelog.version,
        )

        if grant:
            feedback.report_progress("Granting permissions...")
            self.config.role_manager().grant_permissions(
                connection=connection, commit=False, feedback=feedback
            )

        if commit:
            feedback.lock_cancellation()
            feedback.report_progress("Committing changes...")
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
        roles: bool = False,
        grant: bool = False,
        feedback: Feedback | None = None,
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
            roles:
                If True, roles will be created.
            grant:
                If True, permissions will be granted to the roles.
            feedback:
                A Feedback instance to report progress and check for cancellation.
                If None, a LogFeedback instance will be used.
        """
        if feedback is None:
            feedback = LogFeedback()

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
        feedback.report_progress("Starting upgrade...")

        # Calculate total steps: drop handlers + applicable changelog files + create handlers
        drop_handlers = self.config.drop_app_handlers() if not skip_drop_app else []
        changelogs = list(self.config.changelogs(max_version=max_version))
        create_handlers = self.config.create_app_handlers() if not skip_create_app else []

        # First pass: determine applicable changelogs
        applicable_changelogs = []
        for changelog in changelogs:
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
            applicable_changelogs.append(changelog)

        total_changelog_files = sum(len(changelog.files()) for changelog in applicable_changelogs)

        # Count role operations
        role_steps = 0
        if roles or grant:
            role_manager = self.config.role_manager()
            role_steps += len(role_manager.roles)  # create roles
            if grant:
                role_steps += len(role_manager.roles)  # grant permissions

        total_steps = len(drop_handlers) + total_changelog_files + len(create_handlers) + role_steps
        feedback.set_total_steps(total_steps)

        if not skip_drop_app:
            for drop_app_hook in drop_handlers:
                if feedback.is_cancelled():
                    raise PumException("Upgrade cancelled by user")
                feedback.increment_step()
                feedback.report_progress(
                    f"Executing drop app handler: {drop_app_hook.file or 'SQL code'}"
                )
                drop_app_hook.execute(connection=connection, commit=False, parameters=parameters)

        for changelog in applicable_changelogs:
            if feedback.is_cancelled():
                raise PumException("Upgrade cancelled by user")
            changelog.apply(
                connection,
                commit=False,
                parameters=parameters,
                schema_migrations=self.schema_migrations,
                beta_testing=effective_beta_testing,
                feedback=feedback,
            )

        if not skip_create_app:
            for create_app_hook in create_handlers:
                if feedback.is_cancelled():
                    raise PumException("Upgrade cancelled by user")
                feedback.increment_step()
                feedback.report_progress(
                    f"Executing create app handler: {create_app_hook.file or 'SQL code'}"
                )
                create_app_hook.execute(connection=connection, commit=False, parameters=parameters)

        if roles or grant:
            feedback.report_progress("Creating roles...")
            self.config.role_manager().create_roles(
                connection=connection, grant=False, commit=False, feedback=feedback
            )
            if grant:
                feedback.report_progress("Granting permissions...")
                self.config.role_manager().grant_permissions(
                    connection=connection, commit=False, feedback=feedback
                )

        feedback.lock_cancellation()
        feedback.report_progress("Committing changes...")
        connection.commit()
        logger.info("Upgrade completed and changes committed to the database.")

    def uninstall(
        self,
        connection: psycopg.Connection,
        *,
        parameters: dict | None = None,
        commit: bool = True,
        feedback: Feedback | None = None,
    ) -> None:
        """Uninstall the module by executing uninstall hooks.

        Args:
            connection: The database connection to use for the uninstall.
            parameters: The parameters to pass to the uninstall hooks.
            commit: If True, the changes will be committed to the database. Default is True.
            feedback: A Feedback instance to report progress and check for cancellation.
                If None, a LogFeedback instance will be used.

        Raises:
            PumException: If no uninstall hooks are defined in the configuration.
        """
        if feedback is None:
            feedback = LogFeedback()

        uninstall_hooks = self.config.uninstall_handlers()

        if not uninstall_hooks:
            raise PumException(
                "No uninstall hooks defined in the configuration. "
                "Add 'uninstall' section to your .pum.yaml file to define uninstall hooks."
            )

        logger.info("Uninstalling module...")
        feedback.report_progress("Starting uninstall...")

        # Set total steps for progress tracking
        total_steps = len(uninstall_hooks)
        feedback.set_total_steps(total_steps)

        for uninstall_hook in uninstall_hooks:
            if feedback.is_cancelled():
                raise PumException("Uninstall cancelled by user")
            feedback.increment_step()
            feedback.report_progress(
                f"Executing uninstall handler: {uninstall_hook.file or 'SQL code'}"
            )
            uninstall_hook.execute(connection=connection, commit=False, parameters=parameters)

        if commit:
            feedback.lock_cancellation()
            feedback.report_progress("Committing changes...")
            connection.commit()
            logger.info("Uninstall completed and changes committed to the database.")
            logger.info("Uninstall completed and changes committed to the database.")
