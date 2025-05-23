#!/usr/bin/env python3

import argparse
import importlib.metadata
import logging
import sys
from pathlib import Path
from typing import Any

import psycopg

from .checker import Checker
from .config import PumConfig

# from .dumper import Dumper
from .info import run_info
from .upgrader import Upgrader


def setup_logging(verbosity: int = 0):
    """Setup logging based on verbosity level (0=WARNING, 1=INFO, 2+=DEBUG)"""
    level = logging.WARNING  # default

    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="%(message)s",
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

    # def run_dump(
    #     self, pg_service: str, file: str, exclude_schema: list[str] | None
    # ) -> None:
    #     """
    #     Run the dump command

    #     Parameters
    #     ----------
    #     pg_service: string
    #         The name of the postgres service (defined in
    #         pg_service.conf) related to the first db to be compared
    #     file: string
    #         The path of the desired backup file
    #     """
    #     self.__out("Dump...", type="WAITING")
    #     try:
    #         dumper = Dumper(pg_service, file)
    #         if self.pg_dump_exe:
    #             dumper.pg_backup(
    #                 pg_dump_exe=self.pg_dump_exe, exclude_schema=exclude_schema
    #             )
    #         else:
    #             dumper.pg_backup(exclude_schema=exclude_schema)
    #     except (PgDumpFailed, PgDumpCommandError) as e:
    #         self.__out("ERROR", "FAIL")
    #         self.__out(e.args[0] if e.args else str(e), "FAIL")
    #         sys.exit(1)
    #     self.__out("OK", "OKGREEN")

    # def run_restore(
    #     self,
    #     pg_service: str,
    #     file: str,
    #     ignore_restore_errors: bool,
    #     exclude_schema: list[str] | None = None,
    # ) -> None:
    #     """
    #     Run the dump command

    #     Parameters
    #     ----------
    #     pg_service: string
    #         The name of the postgres service (defined in
    #         pg_service.conf) related to the first db to be compared
    #     file: string
    #         The path of the desired backup file
    #     ignore_restore_errors: Boolean
    #         If true the pg_restore errors don't cause the exit of the program
    #     """
    #     self.__out("Restore...", type="WAITING")
    #     try:
    #         dumper = Dumper(pg_service, file)
    #         if self.pg_restore_exe:
    #             dumper.pg_restore(
    #                 pg_restore_exe=self.pg_restore_exe, exclude_schema=exclude_schema
    #             )
    #         else:
    #             dumper.pg_restore(exclude_schema=exclude_schema)
    #     except PgRestoreFailed as e:
    #         self.__out("ERROR", "FAIL")
    #         self.__out(str(e), "FAIL")
    #         if ignore_restore_errors:
    #             return
    #         else:
    #             sys.exit(1)
    #     except PgRestoreCommandError as e:
    #         self.__out("ERROR", "FAIL")
    #         self.__out(e.args[0] if e.args else str(e), "FAIL")
    #         sys.exit(1)
    #     self.__out("OK", "OKGREEN")

    def run_baseline(
        self, pg_service: str, table: str, delta_dirs: list[str], baseline: str
    ) -> None:
        """Run the baseline command. Set the current database version
        (baseline) into the specified table.

        Parameters
        ----------
        pg_service: str
            The name of the postgres service (defined in
            pg_service.conf)
        table: str
            The name of the upgrades information table in the format
            schema.table
        delta_dirs: list(str)
            The paths to the delta directories
        baseline: str
            The version of the current database to set in the information
            table. The baseline must be in the format x.x.x where x are numbers.

        """
        self.__out("Set baseline...")
        try:
            upgrader = Upgrader(pg_service, table, delta_dirs)
            upgrader.create_upgrades_table()
            upgrader.set_baseline(baseline)
        except ValueError as e:
            self.__out("ERROR")
            self.__out(e.args[0] if e.args else str(e))
            sys.exit(1)
        self.__out("OK")

    def run_upgrade(
        self,
        pg_service: str,
        table: str,
        delta_dirs: list[str],
        variables: dict[str, Any],
        max_version: str,
        verbose: bool,
    ) -> None:
        """Apply the delta files to upgrade the database

        Parameters
        ----------
        pg_service: str
            The name of the postgres service (defined in pg_service.conf)
        table: str
            The name of the upgrades information table in the format
            schema.table
        delta_dirs: list(str)
            The paths to the delta directories
        variables: dict
            dictionary for variables to be used in SQL deltas ( name => value )
        max_version: str
            Maximum (including) version to run the deltas up to.
        verbose: bool
            Whether to display extra information

        """
        self.__out("Upgrade...")
        try:
            upgrader = Upgrader(
                pg_service,
                table,
                delta_dirs,
                variables=variables,
                max_version=max_version,
            )
            upgrader.run(verbose=verbose)
        except Exception as e:
            print(e)
            if verbose:
                raise e
            sys.exit(1)
        self.__out("OK")


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
    parser_baseline.add_argument("-t", "--table", help="Upgrades information table", required=True)
    parser_baseline.add_argument(
        "-d",
        "--dir",
        nargs="+",
        help="Delta directories (space-separated)",
        required=True,
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

    if args.config_file:
        config = PumConfig.from_yaml(args.config_file)
    else:
        config = PumConfig.from_yaml(Path(args.dir) / ".pum.yaml")

    # if no command is passed, print the help and exit
    if not args.command:
        parser.print_help()
        parser.exit()

    # Build parameters dict for install and upgrade commands
    parameters = {}
    if args.command in ("install", "upgrade"):
        for p in args.parameter or ():
            if p[0] not in config.parameters():
                print(f"Unknown parameter: {p[0]}")  # noqa: T201
                sys.exit(1)
            if config.parameter(p[0]).type == "float":
                parameters[p[0]] = float(p[1])
            if config.parameter(p[0]).type == "integer":
                parameters[p[0]] = int(p[1])
            else:
                parameters[p[0]] = p[1]

    logger = logging.getLogger(__name__)
    logger.debug(f"Parameters: {parameters}")

    pum = Pum(args.pg_service, config)
    exit_code = 0

    if args.command == "info":
        run_info(args.pg_service, config)
    elif args.command == "install":
        Upgrader(args.pg_service, config=config, parameters=parameters).install(
            max_version=args.max_version
        )
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
        pum.run_dump(args.pg_service, args.file, args.exclude_schema)
    elif args.command == "restore":
        pum.run_restore(args.pg_service, args.file, args.x, args.exclude_schema)
    elif args.command == "baseline":
        pum.run_baseline(args.pg_service, args.table, args.dir, args.baseline)
    elif args.command == "upgrade":
        pum.run_upgrade(
            args.pg_service,
            args.table,
            args.dir,
            parameters,
            args.max_version,
            args.verbose,
        )
    elif args.command == "help":
        parser.print_help()
    else:
        print(f"Unknown command: {args.command}")  # noqa: T201
        print("Use -h or --help for help.")  # noqa: T201
        exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(cli())
