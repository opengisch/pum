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
        "action", choices=["create", "grant", "revoke", "drop"], help="Action to perform"
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
        parameters = {}
        if args.command in ("install", "upgrade", "uninstall"):
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
                else:
                    raise ValueError(f"Unsupported parameter type for {p[0]}: {param.type}")
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
                    "You must specify an action for the role command (create, grant, revoke, drop)."
                )
                exit_code = 1
            else:
                if args.action == "create":
                    config.role_manager().create_roles(connection=conn)
                elif args.action == "grant":
                    config.role_manager().grant_permissions(connection=conn)
                elif args.action == "revoke":
                    config.role_manager().revoke_permissions(connection=conn)
                elif args.action == "drop":
                    config.role_manager().drop_roles(connection=conn)
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
                    sm.create(connection=conn)
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
            upg.uninstall(connection=conn, parameters=parameters)
            logger.info("Uninstall completed successfully.")

        else:
            logger.error(f"Unknown command: {args.command}")
            logger.error("Use -h or --help for help.")
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(cli())
