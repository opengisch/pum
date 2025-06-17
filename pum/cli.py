#!/usr/bin/env python3

import argparse
import importlib.metadata
import logging
import sys
from pathlib import Path

import psycopg

from .checker import Checker
from .pum_config import PumConfig

from .info import run_info
from .upgrader import Upgrader
from .parameter import ParameterType
from .schema_migrations import SchemaMigrations
from .dumper import DumpFormat


def setup_logging(verbosity: int = 0):
    """Setup logging based on verbosity level (0=WARNING, 1=INFO, 2+=DEBUG) with colored output."""
    level = logging.WARNING  # default

    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    class ColorFormatter(logging.Formatter):
        COLORS = {
            logging.ERROR: "\033[31m",  # Red
            logging.WARNING: "\033[33m",  # Yellow
            logging.INFO: "\033[36m",  # Cyan
            logging.DEBUG: "\033[35m",  # Magenta
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
    def __init__(self, pg_service: str, config: str | PumConfig = None) -> None:
        """Initialize the PUM class with a database connection and configuration.

        Args:
            pg_service (str): The name of the postgres service (defined in pg_service.conf)
            config (str | PumConfig): The configuration file path or a PumConfig object.

        """
        self.pg_service = pg_service

        if isinstance(config, str):
            self.config = PumConfig.from_yaml(config)
        else:
            self.config = config

    def run_check(
        self,
        pg_service1: str,
        pg_service2: str,
        ignore_list: list[str] | None,
        exclude_schema: list[str] | None,
        exclude_field_pattern: list[str] | None,
        verbose_level: int = 1,
        output_file: str | None = None,
    ) -> bool:
        """Run the check command.

        Args:
            pg_service1:
                The name of the postgres service (defined in pg_service.conf)
                related to the first db to be compared
            pg_service2:
                The name of the postgres service (defined in pg_service.conf)
                related to the first db to be compared
            ignore_list:
                List of elements to be ignored in check (ex. tables, columns,
                views, ...)
            exclude_schema:
                List of schemas to be ignored in check.
            exclude_field_pattern:
                List of field patterns to be ignored in check.
            verbose_level:
                verbose level, 0 -> nothing, 1 -> print first 80 char of each
                difference, 2 -> print all the difference details
            output_file:
                a file path where write the differences

        Returns:
            True if no differences are found, False otherwise.

        """
        # self.__out("Check...")
        verbose_level = verbose_level or 1
        ignore_list = ignore_list or []
        exclude_schema = exclude_schema or []
        exclude_field_pattern = exclude_field_pattern or []
        try:
            checker = Checker(
                pg_service1,
                pg_service2,
                exclude_schema=exclude_schema,
                exclude_field_pattern=exclude_field_pattern,
                ignore_list=ignore_list,
                verbose_level=verbose_level,
            )
            result, differences = checker.run_checks()

            if result:
                self.__out("OK")
            else:
                self.__out("DIFFERENCES FOUND")

            if differences:
                if output_file:
                    with open(output_file, "w") as f:
                        for k, values in differences.items():
                            f.write(k + "\n")
                            f.writelines(f"{v}\n" for v in values)
                else:
                    for k, values in differences.items():
                        print(k)
                        for v in values:
                            print(v)
            return result

        except psycopg.Error as e:
            self.__out("ERROR")
            self.__out(e.args[0] if e.args else str(e))
            sys.exit(1)

        except Exception as e:
            self.__out("ERROR")
            # if e.args is empty then use str(e)
            self.__out(e.args[0] if e.args else str(e))
            sys.exit(1)


def create_parser() -> argparse.ArgumentParser:
    """Creates the main parser with its sub-parsers"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_file", help="set the config file. Default: .pum.yaml")
    parser.add_argument("-s", "--pg-service", help="Name of the postgres service", required=True)

    parser.add_argument(
        "-d", "--dir", help="Directory or URL of the module. Default: .", default="."
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase output verbosity (e.g. -v, -vv)",
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
    parser_info = subparsers.add_parser("info", help="show info about schema migrations history.")  # NOQA

    # Parser for the "install" command
    parser_install = subparsers.add_parser("install", help="Installs the module.")
    parser_install.add_argument(
        "-p",
        "--parameter",
        nargs=2,
        help="Assign variable for running SQL deltas. Format is name value.",
        action="append",
    )
    parser_install.add_argument("--max-version", help="maximum version to install")
    parser_install.add_argument("-r", "--roles", help="Create roles", action="store_true")
    parser_install.add_argument(
        "-g", "--grant", help="Grant permissions to roles", action="store_true"
    )
    parser_install.add_argument(
        "-d", "--demo-data", help="Load demo data with the given name", type=str, default=None
    )
    parser_install.add_argument(
        "--beta-testing",
        help="This will install the module in beta testing, meaning that it will not be possible to receive any future updates.",
        action="store_true",
    )

    # Role management parser
    parser_role = subparsers.add_parser("role", help="manage roles in the database")
    parser_role.add_argument(
        "action", choices=["create", "grant", "revoke", "drop"], help="Action to perform"
    )

    # Parser for the "check" command
    parser_check = subparsers.add_parser(
        "check", help="check the differences between two databases"
    )

    parser_check.add_argument(
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
    parser_check.add_argument(
        "-N", "--exclude-schema", help="Schema to be ignored.", action="append"
    )
    parser_check.add_argument(
        "-P",
        "--exclude-field-pattern",
        help="Fields to be ignored based on a pattern compatible with SQL LIKE.",
        action="append",
    )

    parser_check.add_argument("-o", "--output_file", help="Output file")

    # Parser for the "dump" command
    parser_dump = subparsers.add_parser("dump", help="dump a Postgres database")
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
        "restore", help="restore a Postgres database from a dump file"
    )
    parser_restore.add_argument("-x", help="ignore pg_restore errors", action="store_true")
    parser_restore.add_argument(
        "-N", "--exclude-schema", help="Schema to be ignored.", action="append"
    )
    parser_restore.add_argument("file", help="The backup file")

    # Parser for the "baseline" command
    parser_baseline = subparsers.add_parser(
        "baseline", help="Create upgrade information table and set baseline"
    )
    parser_baseline.add_argument(
        "-b", "--baseline", help="Set baseline in the format x.x.x", required=True
    )

    # Parser for the "upgrade" command
    parser_upgrade = subparsers.add_parser("upgrade", help="upgrade db")
    parser_upgrade.add_argument("-u", "--max-version", help="upper bound limit version")
    parser_upgrade.add_argument(
        "-p",
        "--parameter",
        nargs=2,
        help="Assign variable for running SQL deltas. Format is: name value.",
        action="append",
    )

    return parser


def cli() -> int:  # noqa: PLR0912
    """Main function to run the command line interface."""
    parser = create_parser()
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # if no command is passed, print the help and exit
    if not args.command:
        parser.print_help()
        parser.exit()

    if args.config_file:
        config = PumConfig.from_yaml(args.config_file, install_dependencies=True)
    else:
        config = PumConfig.from_yaml(Path(args.dir) / ".pum.yaml", install_dependencies=True)

    with psycopg.connect(f"service={args.pg_service}") as conn:
        # Check if the connection is successful
        if not conn:
            logger.error(f"Could not connect to the database using service: {args.pg_service}")
            sys.exit(1)

        # Build parameters dict for install and upgrade commands
        parameters = {}
        if args.command in ("install", "upgrade"):
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

        pum = Pum(args.pg_service, config)
        exit_code = 0

        if args.command == "info":
            run_info(connection=conn, config=config)
        elif args.command == "install":
            upg = Upgrader(config=config)
            upg.install(
                connection=conn,
                parameters=parameters,
                max_version=args.max_version,
                roles=args.roles,
                grant=args.grant,
                beta_testing=args.beta_testing,
            )
            conn.commit()
            if args.demo_data:
                upg.install_demo_data(name=args.demo_data, connection=conn, parameters=parameters)
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
        elif args.command == "check":
            success = pum.run_check(
                args.pg_service1,
                args.pg_service2,
                ignore_list=args.ignore,
                exclude_schema=args.exclude_schema,
                exclude_field_pattern=args.exclude_field_pattern,
                verbose_level=args.verbose_level,
                output_file=args.output_file,
            )
            if not success:
                exit_code = 1
        elif args.command == "dump":
            pass
        elif args.command == "restore":
            pum.run_restore(args.pg_service, args.file, args.x, args.exclude_schema)
        elif args.command == "baseline":
            SchemaMigrations(config=config).set_baseline(connection=conn, version=args.baseline)

        elif args.command == "upgrade":
            # TODO
            logger.error("Upgrade is not implemented yet")
        else:
            logger.error(f"Unknown command: {args.command}")
            logger.error("Use -h or --help for help.")
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(cli())
