#!/usr/bin/env python3

import argparse
import importlib.metadata
import logging
import sys
from pathlib import Path

import psycopg

from .checker import Checker
from .report_generator import ReportGenerator
from .pum_config import PumConfig
from .connection import format_connection_string

from .info import run_info
from .upgrader import Upgrader
from .parameter import ParameterType
from .schema_migrations import SchemaMigrations
from .dumper import DumpFormat, Dumper
from .role_manager import RoleInventory
from . import SQL


def setup_logging(verbosity: int = 0):
    """Configure logging for the CLI.

    Args:
        verbosity: Verbosity level (-1=quiet/WARNING, 0=INFO, 1=DEBUG, 2+=SQL).

    """
    # Register custom SQL log level
    logging.addLevelName(SQL, "SQL")

    if verbosity < 0:
        level = logging.WARNING  # quiet mode
    elif verbosity >= 2:
        level = SQL  # Most verbose - shows all SQL statements
    elif verbosity >= 1:
        level = logging.DEBUG
    else:
        level = logging.INFO  # default

    class ColorFormatter(logging.Formatter):
        COLORS = {
            logging.ERROR: "\033[31m",  # Red
            logging.WARNING: "\033[33m",  # Yellow
            logging.INFO: "\033[36m",  # Cyan
            logging.DEBUG: "\033[35m",  # Magenta
            SQL: "\033[90m",  # Gray for SQL statements
        }
        RESET = "\033[0m"

        def format(self, record):
            color = self.COLORS.get(record.levelno, "")
            message = super().format(record)
            if color:
                message = f"{color}{message}{self.RESET}"
            return message

    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter("%(message)s"))
    logging.basicConfig(
        level=level,
        handlers=[handler],
        format="%(message)s",
        force=True,
    )


class Pum:
    def __init__(self, pg_connection: str, config: str | PumConfig = None) -> None:
        """Initialize the PUM class with a database connection and configuration.

        Args:
            pg_connection (str): PostgreSQL service name or connection string.
                Can be a service name (e.g., 'mydb') or a full connection string
                (e.g., 'postgresql://user:pass@host/db' or 'host=localhost dbname=mydb').
            config (str | PumConfig): The configuration file path or a PumConfig object.

        """
        self.pg_connection = pg_connection

        if isinstance(config, str):
            self.config = PumConfig.from_yaml(config)
        else:
            self.config = config


def create_parser(
    max_help_position: int | None = None, width: int | None = None
) -> argparse.ArgumentParser:
    """Create the main argument parser and all subparsers.

    Args:
        max_help_position: Maximum help position for formatting.
        width: Width for formatting.

    Returns:
        The fully configured argument parser.

    """
    if max_help_position is not None or width is not None:

        def formatter_class(prog):
            return argparse.HelpFormatter(
                prog, max_help_position=max_help_position or 40, width=width or 200
            )
    else:
        formatter_class = argparse.HelpFormatter
    parser = argparse.ArgumentParser(
        prog="pum",
        formatter_class=formatter_class,
    )
    parser.add_argument("-c", "--config_file", help="set the config file. Default: .pum.yaml")
    parser.add_argument(
        "-p",
        "--pg-connection",
        help="PostgreSQL service name or connection string (e.g., 'mydb' or 'postgresql://user:pass@host/db')",
        required=True,
    )

    parser.add_argument(
        "-d", "--dir", help="Directory or URL of the module. Default: .", default="."
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v for DEBUG, -vv for SQL statements)",
    )

    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress info messages, only show warnings and errors",
    )

    version = importlib.metadata.version("pum")
    parser.add_argument(
        "--version",
        action="version",
        version=f"pum {version}",
        help="Show program's version number and exit.",
    )

    subparsers = parser.add_subparsers(
        title="commands", description="valid pum commands", dest="command"
    )

    # Parser for the "info" command
    parser_info = subparsers.add_parser(  # NOQA
        "info",
        help="show info about schema migrations history.",
        formatter_class=formatter_class,
    )

    # Parser for the "install" command
    parser_install = subparsers.add_parser(
        "install", help="Installs the module.", formatter_class=formatter_class
    )
    parser_install.add_argument(
        "-p",
        "--parameter",
        nargs=2,
        help="Assign variable for running SQL deltas. Format is name value.",
        action="append",
    )
    parser_install.add_argument("--max-version", help="maximum version to install")
    parser_install.add_argument("--skip-roles", help="Skip creating roles", action="store_true")
    parser_install.add_argument(
        "--skip-grant", help="Skip granting permissions to roles", action="store_true"
    )
    parser_install.add_argument(
        "-d", "--demo-data", help="Load demo data with the given name", type=str, default=None
    )
    parser_install.add_argument(
        "--beta-testing",
        help="This will install the module in beta testing, meaning that it will not be possible to receive any future updates.",
        action="store_true",
    )
    parser_install.add_argument(
        "--skip-drop-app",
        help="Skip drop app handlers during installation.",
        action="store_true",
    )
    parser_install.add_argument(
        "--skip-create-app",
        help="Skip create app handlers during installation.",
        action="store_true",
    )
    parser_install.add_argument(
        "--allow-multiple-modules",
        help="Allow multiple PUM modules (with separate migration tables) in the same database",
        action="store_true",
    )

    # Upgrade parser
    parser_upgrade = subparsers.add_parser(
        "upgrade", help="Upgrade the database.", formatter_class=formatter_class
    )
    parser_upgrade.add_argument(
        "-p",
        "--parameter",
        nargs=2,
        help="Assign variable for running SQL deltas. Format is name value.",
        action="append",
    )
    parser_upgrade.add_argument("-u", "--max-version", help="maximum version to upgrade")
    parser_upgrade.add_argument(
        "--skip-grant", help="Skip granting permissions to roles", action="store_true"
    )
    parser_upgrade.add_argument(
        "--beta-testing", help="Install in beta testing mode.", action="store_true"
    )
    parser_upgrade.add_argument(
        "--force",
        help="Allow upgrading a module installed in beta testing mode.",
        action="store_true",
    )
    parser_upgrade.add_argument(
        "--skip-drop-app",
        help="Skip drop app handlers during upgrade.",
        action="store_true",
    )
    parser_upgrade.add_argument(
        "--skip-create-app",
        help="Skip create app handlers during upgrade.",
        action="store_true",
    )

    # Role management parser
    parser_role = subparsers.add_parser(
        "role", help="manage roles in the database", formatter_class=formatter_class
    )
    parser_role.add_argument(
        "action", choices=["create", "grant", "revoke", "drop", "list"], help="Action to perform"
    )
    parser_role.add_argument(
        "--suffix",
        help="Create DB-specific roles by appending this suffix to each role name (e.g. 'lausanne' creates 'role_lausanne')",
        type=str,
        default=None,
    )
    parser_role.add_argument(
        "--roles",
        help="Restrict the action to specific configured role names (space-separated). When omitted, all configured roles are affected.",
        nargs="+",
        default=None,
    )
    parser_role.add_argument(
        "--to",
        help="Target database user to grant role membership to (used with 'grant' action)",
        type=str,
        default=None,
        dest="to_role",
    )
    parser_role.add_argument(
        "--from",
        help="Target database user to revoke role membership from (used with 'revoke' action)",
        type=str,
        default=None,
        dest="from_role",
    )
    parser_role.add_argument(
        "--include-superusers",
        help="Include superusers in the role listing (they are hidden by default)",
        action="store_true",
        default=False,
    )

    # Parser for the "check" command
    parser_checker = subparsers.add_parser(
        "check", help="check the differences between two databases", formatter_class=formatter_class
    )

    parser_checker.add_argument(
        "pg_connection_compared",
        help="PostgreSQL service name or connection string for the database to compare against",
    )

    parser_checker.add_argument(
        "-i",
        "--ignore",
        help="Elements to be ignored",
        nargs="+",
        choices=[
            "tables",
            "columns",
            "constraints",
            "views",
            "sequences",
            "indexes",
            "triggers",
            "functions",
            "rules",
        ],
    )
    parser_checker.add_argument(
        "-N", "--exclude-schema", help="Schema to be ignored.", action="append"
    )
    parser_checker.add_argument(
        "-P",
        "--exclude-field-pattern",
        help="Fields to be ignored based on a pattern compatible with SQL LIKE.",
        action="append",
    )

    parser_checker.add_argument("-o", "--output_file", help="Output file")
    parser_checker.add_argument(
        "-f",
        "--format",
        choices=["text", "html", "json"],
        default="text",
        help="Output format: text, html, or json. Default: text",
    )

    # Parser for the "dump" command
    parser_dump = subparsers.add_parser(
        "dump", help="dump a Postgres database", formatter_class=formatter_class
    )
    parser_dump.add_argument(
        "-f",
        "--format",
        type=lambda s: DumpFormat[s.upper()],
        choices=list(DumpFormat),
        default=DumpFormat.PLAIN,
        help=f"Dump format. Choices: {[e.name.lower() for e in DumpFormat]}. Default: plain.",
    )
    parser_dump.add_argument(
        "-N", "--exclude-schema", help="Schema to be ignored.", action="append"
    )
    parser_dump.add_argument("file", help="The backup file")

    # Parser for the "restore" command
    parser_restore = subparsers.add_parser(
        "restore",
        help="restore a Postgres database from a dump file",
        formatter_class=formatter_class,
    )
    parser_restore.add_argument("-x", help="ignore pg_restore errors", action="store_true")
    parser_restore.add_argument(
        "-N", "--exclude-schema", help="Schema to be ignored.", action="append"
    )
    parser_restore.add_argument("file", help="The backup file")

    # Parser for the "baseline" command
    parser_baseline = subparsers.add_parser(
        "baseline",
        help="Create upgrade information table and set baseline",
        formatter_class=formatter_class,
    )
    parser_baseline.add_argument(
        "-b", "--baseline", help="Set baseline in the format x.x.x", required=True
    )
    parser_baseline.add_argument(
        "--create-table",
        help="Create the pum_migrations table if it does not exist",
        action="store_true",
    )
    parser_baseline.add_argument(
        "--allow-multiple-modules",
        help="Allow multiple PUM modules (with separate migration tables) in the same database",
        action="store_true",
    )

    # Parser for the "uninstall" command
    parser_uninstall = subparsers.add_parser(
        "uninstall",
        help="Uninstall the module by executing uninstall hooks",
        formatter_class=formatter_class,
    )
    parser_uninstall.add_argument(
        "-p",
        "--parameter",
        nargs=2,
        help="Assign variable for running SQL hooks. Format is name value.",
        action="append",
    )
    parser_uninstall.add_argument(
        "--force",
        help="Skip confirmation prompt and proceed with uninstall",
        action="store_true",
        dest="force",
    )

    # Parser for the "app" command
    parser_app = subparsers.add_parser(
        "app",
        help="Manage application handlers (create, drop, recreate)",
        formatter_class=formatter_class,
    )
    parser_app.add_argument(
        "action",
        choices=["create", "drop", "recreate"],
        help="Action to perform: create (run create_app handlers), drop (run drop_app handlers), recreate (run drop then create)",
    )
    parser_app.add_argument(
        "-p",
        "--parameter",
        nargs=2,
        help="Assign variable for running SQL handlers. Format is name value.",
        action="append",
    )

    return parser


def cli() -> int:  # noqa: PLR0912
    """Run the command line interface.

    Returns:
        Process exit code.

    """
    parser = create_parser()
    args = parser.parse_args()

    # Validate mutually exclusive flags
    if args.quiet and args.verbose:
        parser.error("--quiet and --verbose are mutually exclusive")

    # Set verbosity level (-1 for quiet, 0 for normal, 1+ for verbose)
    if args.quiet:
        verbosity = -1
    else:
        verbosity = args.verbose

    setup_logging(verbosity)
    logger = logging.getLogger(__name__)

    # if no command is passed, print the help and exit
    if not args.command:
        parser.print_help()
        parser.exit()

    # Handle check command separately (doesn't need db connection)
    if args.command == "check":
        exit_code = 0
        checker = Checker(
            args.pg_connection,
            args.pg_connection_compared,
            exclude_schema=args.exclude_schema or [],
            exclude_field_pattern=args.exclude_field_pattern or [],
            ignore_list=args.ignore or [],
        )
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        if report.passed:
            logger.info("OK")
        else:
            logger.info("DIFFERENCES FOUND")

        if args.format == "html":
            html_report = ReportGenerator.generate_html(report)
            if args.output_file:
                with open(args.output_file, "w", encoding="utf-8") as f:
                    f.write(html_report)
                logger.info(f"HTML report written to {args.output_file}")
            else:
                print(html_report)
        elif args.format == "json":
            json_report = ReportGenerator.generate_json(report)
            if args.output_file:
                with open(args.output_file, "w", encoding="utf-8") as f:
                    f.write(json_report)
                logger.info(f"JSON report written to {args.output_file}")
            else:
                print(json_report)
        else:
            # Text output (backward compatible)
            text_output = ReportGenerator.generate_text(report)
            if args.output_file:
                with open(args.output_file, "w") as f:
                    f.write(text_output)
            else:
                print(text_output)

        if not report.passed:
            exit_code = 1

        return exit_code

    validate = args.command not in ("info", "baseline")
    if args.config_file:
        config = PumConfig.from_yaml(args.config_file, validate=validate, install_dependencies=True)
    else:
        config = PumConfig.from_yaml(
            Path(args.dir) / ".pum.yaml", validate=validate, install_dependencies=True
        )

    with psycopg.connect(format_connection_string(args.pg_connection)) as conn:
        # Check if the connection is successful
        if not conn:
            logger.error(f"Could not connect to the database: {args.pg_connection}")
            sys.exit(1)

        # Build parameters dict for install and upgrade commands
        # Start with config-declared defaults, then override with CLI-provided values
        parameters = {}
        if args.command in ("install", "upgrade", "uninstall", "app"):
            for param_def in config.parameters():
                parameters[param_def.name] = param_def.default
            for p in args.parameter or ():
                param = config.parameter(p[0])
                if not param:
                    logger.error(f"Unknown parameter: {p[0]}")
                    sys.exit(1)
                if param.type == ParameterType.DECIMAL:
                    parameters[p[0]] = float(p[1])
                elif param.type == ParameterType.INTEGER:
                    parameters[p[0]] = int(p[1])
                elif param.type == ParameterType.BOOLEAN:
                    parameters[p[0]] = p[1].lower() in ("true", "1", "yes")
                elif param.type == ParameterType.TEXT:
                    parameters[p[0]] = p[1]
                elif param.type == ParameterType.PATH:
                    parameters[p[0]] = p[1]
                else:
                    raise ValueError(f"Unsupported parameter type for {p[0]}: {param.type}")
                if param.values and parameters[p[0]] not in param.values:
                    logger.error(
                        f"Parameter '{p[0]}' value '{parameters[p[0]]}' is not allowed. "
                        f"Allowed values: {param.values}"
                    )
                    sys.exit(1)
            logger.debug(f"Parameters: {parameters}")

        exit_code = 0

        if args.command == "info":
            run_info(connection=conn, config=config)
        elif args.command == "install":
            upg = Upgrader(config=config)
            upg.install(
                connection=conn,
                parameters=parameters,
                max_version=args.max_version,
                roles=not args.skip_roles,
                grant=not args.skip_grant,
                beta_testing=args.beta_testing,
                skip_drop_app=args.skip_drop_app,
                skip_create_app=args.skip_create_app,
                allow_multiple_modules=args.allow_multiple_modules,
            )
            conn.commit()
            if args.demo_data:
                upg.install_demo_data(
                    name=args.demo_data,
                    connection=conn,
                    parameters=parameters,
                    grant=not args.skip_grant,
                    skip_create_app=args.skip_create_app,
                    skip_drop_app=args.skip_drop_app,
                )
        elif args.command == "upgrade":
            upg = Upgrader(config=config)
            upg.upgrade(
                connection=conn,
                parameters=parameters,
                max_version=args.max_version,
                grant=not args.skip_grant,
                beta_testing=args.beta_testing,
                force=args.force,
                skip_drop_app=args.skip_drop_app,
                skip_create_app=args.skip_create_app,
            )
        elif args.command == "role":
            if not args.action:
                logger.error(
                    "You must specify an action for the role command (create, grant, revoke, drop, check)."
                )
                exit_code = 1
            else:
                if args.action == "create":
                    config.role_manager().create_roles(
                        connection=conn,
                        suffix=args.suffix,
                        grant=True,
                        commit=True,
                    )
                elif args.action == "grant":
                    if args.to_role:
                        config.role_manager().grant_to(
                            connection=conn,
                            to=args.to_role,
                            roles=args.roles,
                            suffix=args.suffix,
                            commit=True,
                        )
                    else:
                        config.role_manager().grant_permissions(connection=conn)
                elif args.action == "revoke":
                    if args.from_role:
                        config.role_manager().revoke_from(
                            connection=conn,
                            from_role=args.from_role,
                            roles=args.roles,
                            suffix=args.suffix,
                            commit=True,
                        )
                    else:
                        config.role_manager().revoke_permissions(
                            connection=conn, roles=args.roles, suffix=args.suffix, commit=True
                        )
                elif args.action == "drop":
                    config.role_manager().drop_roles(
                        connection=conn, roles=args.roles, suffix=args.suffix, commit=True
                    )
                elif args.action == "list":
                    result = config.role_manager().roles_inventory(
                        connection=conn,
                        include_superusers=args.include_superusers,
                    )
                    _print_roles_inventory(result)
                else:
                    logger.error(f"Unknown action: {args.action}")
                    exit_code = 1
        elif args.command == "dump":
            dumper = Dumper(args.pg_connection, args.file)
            dumper.pg_dump(
                exclude_schema=args.exclude_schema or [],
                format=args.format,
            )
            logger.info(f"Database dumped to {args.file}")
        elif args.command == "restore":
            dumper = Dumper(args.pg_connection, args.file)
            try:
                dumper.pg_restore(exclude_schema=args.exclude_schema or [])
                logger.info(f"Database restored from {args.file}")
            except Exception as e:
                if not args.x:
                    raise
                logger.warning(f"Restore completed with errors (ignored): {e}")
        elif args.command == "baseline":
            sm = SchemaMigrations(config=config)
            if not sm.exists(connection=conn):
                if args.create_table:
                    sm.create(
                        connection=conn,
                        allow_multiple_modules=args.allow_multiple_modules,
                    )
                    logger.info("Created pum_migrations table.")
                else:
                    logger.error(
                        "pum_migrations table does not exist. Use --create-table to create it."
                    )
                    exit_code = 1
                    return exit_code
            SchemaMigrations(config=config).set_baseline(connection=conn, version=args.baseline)

        elif args.command == "uninstall":
            # Confirmation prompt unless --force is used
            if not args.force:
                logger.warning(
                    "⚠️  WARNING: This will execute uninstall hooks which may drop schemas and data!"
                )
                response = input("Are you sure you want to proceed? (yes/no): ").strip().lower()
                if response not in ("yes", "y"):
                    logger.info("Uninstall cancelled.")
                    return 0

            upg = Upgrader(config=config)
            upg.uninstall(connection=conn, parameters=parameters, commit=True)
            logger.info("Uninstall completed successfully.")

        elif args.command == "app":
            if not args.action:
                logger.error(
                    "You must specify an action for the app command (create, drop, recreate)."
                )
                exit_code = 1
            else:
                upg = Upgrader(config=config)
                if args.action == "drop":
                    upg.drop_app(connection=conn, parameters=parameters, commit=True)
                elif args.action == "create":
                    upg.create_app(connection=conn, parameters=parameters, commit=True)
                elif args.action == "recreate":
                    upg.recreate_app(connection=conn, parameters=parameters, commit=True)
                else:
                    logger.error(f"Unknown action: {args.action}")
                    exit_code = 1

        else:
            logger.error(f"Unknown command: {args.command}")
            logger.error("Use -h or --help for help.")
            exit_code = 1

    return exit_code


def _print_roles_inventory(result: RoleInventory) -> None:
    """Print the roles inventory to stdout."""
    ok_mark = "\033[32m✓\033[0m"
    fail_mark = "\033[31m✗\033[0m"

    for role_status in result.configured_roles:
        perms_ok = all(sp.satisfied for sp in role_status.schema_permissions)
        mark = ok_mark if perms_ok else fail_mark

        badges = []
        if role_status.is_suffixed:
            badges.append("\033[90m[suffixed]\033[0m")
        if role_status.login:
            badges.append("\033[90m[login]\033[0m")
        if role_status.superuser:
            badges.append("\033[31m[superuser]\033[0m")
        badge_str = "  " + " ".join(badges) if badges else ""
        print(f"  {mark} {role_status.name}{badge_str}")
        if role_status.granted_to:
            members_str = ", ".join(role_status.granted_to)
            print(f"      \033[90mmember of: {members_str}\033[0m")
        for sp in role_status.schema_permissions:
            sp_mark = ok_mark if sp.satisfied else fail_mark
            actual = []
            if sp.has_read:
                actual.append("read")
            if sp.has_write:
                actual.append("write")
            actual_str = ", ".join(actual) if actual else "none"
            expected_str = sp.expected.value if sp.expected else "none"
            if sp.satisfied:
                print(f"      {sp_mark} {sp.schema}  ({actual_str})")
            else:
                print(
                    f"      {sp_mark} {sp.schema}  (expected: {expected_str}, actual: {actual_str})"
                )

    if result.missing_roles:
        print()
        for name in result.missing_roles:
            print(f"  {fail_mark} {name}  (missing)")

    if result.grantee_roles:
        print()
        print("  \033[36mGrantee roles (members of configured roles):\033[0m")
        for gr in result.grantee_roles:
            schemas_str = ", ".join(gr.schemas)
            members_str = ", ".join(gr.granted_to)
            badges = []
            if gr.login:
                badges.append("\033[90m[login]\033[0m")
            if gr.superuser:
                badges.append("\033[31m[superuser]\033[0m")
            badge_str = " " + " ".join(badges) if badges else ""
            print(f"    \033[36m→\033[0m {gr.name}{badge_str}  ({schemas_str})")
            print(f"      \033[90mmember of: {members_str}\033[0m")

    if result.unknown_roles:
        print()
        print("  \033[33mOther roles with schema access:\033[0m")
        for ur in result.unknown_roles:
            schemas_str = ", ".join(ur.schemas)
            badges = []
            if ur.login:
                badges.append("\033[90m[login]\033[0m")
            if ur.superuser:
                badges.append("\033[31m[superuser]\033[0m")
            badge_str = " " + " ".join(badges) if badges else ""
            print(f"    \033[33m?\033[0m {ur.name}{badge_str}  ({schemas_str})")

    if result.other_login_roles:
        print()
        print("  \033[33mOther login roles (no schema access):\033[0m")
        for name in result.other_login_roles:
            print(f"    \033[90m- {name}\033[0m")


if __name__ == "__main__":
    sys.exit(cli())
