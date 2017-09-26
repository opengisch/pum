#!/usr/bin/env python

from __future__ import print_function

import argparse

import yaml
import psycopg2
import subprocess

from core.checker import Checker
from core.dumper import Dumper
from core.upgrader import Upgrader
from utils.utils import ask_for_confirmation, Bcolors


class Pum:
    def __init__(self, config_file=None):

        self.upgrades_table = None
        self.delta_dir = None
        self.backup_file = None
        self.ignore_list = None
        self.pg_dump_exe = None
        self.pg_restore_exe = None

        if config_file:
            self.__load_config_file(config_file)

    def __load_config_file(self, config_file):
        """Load the configurations from yaml configuration file and store it
        to instance variables.

        Parameters
        ----------
        config_file: string
            The path of the config file
        """
        configs = yaml.safe_load(open(config_file))
        self.set_configs(configs)

    def set_configs(self, configs):
        """Save the configuration values into the instance variables.

        Parameters
        ----------
        configs: dict
            Dictionary of configurations
            """

        self.upgrades_table = configs['upgrades_table']
        self.delta_dir = configs['delta_dir']
        self.backup_file = configs['backup_file']
        self.ignore_list = configs['ignore_elements']
        self.pg_dump_exe = configs['pg_dump_exe']
        self.pg_restore_exe = configs['pg_restore_exe']

    def run_check(self, pg_service1, pg_service2, ignore_list=None,
                  verbose_level=1):
        """Run the check command
        
        Parameters
        ----------
        pg_service1: string
            The name of the postgres service (defined in pg_service.conf)
            related to the first db to be compared
        pg_service2: sting
            The name of the postgres service (defined in pg_service.conf)
            related to the first db to be compared
        ignore_list: list of strings
            List of elements to be ignored in check (ex. tables, columns,
            views, ...)
        verbose_level: int
            verbose level, 0 -> nothing, 1 -> print first 80 char of each
            difference, 2 -> print all the difference details
            
        Returns
        -------
        True if no differences are found, False otherwise.
        """

        self.__out('Check...', type='WAITING')
        if not verbose_level:
            verbose_level = 1
        if not ignore_list:
            ignore_list = []
        try:
            checker = Checker(
                pg_service1, pg_service2, ignore_list, verbose_level)
            result, differences = checker.run_checks()

            if result:
                self.__out('OK', 'OKGREEN')
            else:
                self.__out('DIFFERENCES FOUND', 'WARNING')

            if differences:
                print(yaml.dump(differences, default_flow_style=False))

            return result

        except psycopg2.Error as e:
            self.__out('ERROR', 'FAIL')
            self.__out(e.args[0], 'FAIL')
            exit(1)

        except Exception as e:
            self.__out('ERROR', 'FAIL')
            self.__out(e.args[0])
            exit(1)

    def run_dump(self, pg_service, file):
        """
        Run the dump command

        Parameters
        ----------
        pg_service: string
            The name of the postgres service (defined in
            pg_service.conf) related to the first db to be compared
        file: string
            The path of the desired backup file
        """

        self.__out('Dump...', type='WAITING')

        try:
            dumper = Dumper(pg_service, file)
            if self.pg_dump_exe:
                dumper.pg_backup(self.pg_dump_exe)
            else:
                dumper.pg_backup()
        except subprocess.CalledProcessError as e:
            self.__out('ERROR', 'FAIL')
            self.__out(e.output)
            exit(1)
        except Exception as e:
            self.__out('ERROR', 'FAIL')
            self.__out(e.args[0])
            exit(1)
        self.__out('OK', 'OKGREEN')

    def run_restore(self, pg_service, file):
        """
        Run the dump command

        Parameters
        ----------
        pg_service: string
            The name of the postgres service (defined in
            pg_service.conf) related to the first db to be compared
        file: string
            The path of the desired backup file
        """

        self.__out('Restore...', type='WAITING')

        try:
            dumper = Dumper(pg_service, file)
            if self.pg_restore_exe:
                dumper.pg_restore(self.pg_restore_exe)
            else:
                dumper.pg_restore()
        except subprocess.CalledProcessError as e:
            self.__out('ERROR', 'FAIL')
            self.__out(e.output)
            exit(1)
        except Exception as e:
            self.__out('ERROR', 'FAIL')
            self.__out(e.args[0])
            exit(1)
        self.__out('OK', 'OKGREEN')

    def run_baseline(self, pg_service, table, delta_dir, baseline):
        """
        Run the baseline command. Set the current database version
        (baseline) into the specified table.

        Parameters
        -----------
        pg_service: str
            The name of the postgres service (defined in
            pg_service.conf)
        table: str
            The name of the upgrades information table in the format
            schema.table
        delta_dir: str
            The path of the delta directory
        baseline: str
            The version of the current database to set in the information
            table. The baseline must be in the format x.x.x where x are numbers.

        """

        self.__out('Set baseline...', type='WAITING')

        try:
            upgrader = Upgrader(pg_service, table, delta_dir)
            upgrader.create_upgrades_table()
            upgrader.set_baseline(baseline)

        except ValueError as e:
            self.__out('ERROR', 'FAIL')
            self.__out(e)
            exit(1)

        self.__out('OK', 'OKGREEN')

    def run_info(self, pg_service, table, delta_dir):
        """Print info about delta file and about already made upgrade

        Parameters
        -----------
        pg_service: str
            The name of the postgres service (defined in
            pg_service.conf)
        table: str
            The name of the upgrades information table in the format
            schema.table
        delta_dir: str
            The path of the delta directory
        """

        try:
            upgrader = Upgrader(pg_service, table, delta_dir)
            upgrader.show_info()

        except Exception as e:
            print(e)
            exit(1)

    def run_upgrade(self, pg_service, table, delta_dir):
        """Apply the delta files to upgrade the database

        Parameters
        -----------
        pg_service: str
            The name of the postgres service (defined in pg_service.conf)
        table: str
            The name of the upgrades information table in the format
            schema.table
        delta_dir: str
            The path of the delta directory
        """

        self.__out('Upgrade...', type='WAITING')

        try:
            upgrader = Upgrader(pg_service, table, delta_dir)
            upgrader.run()

        except Exception as e:
            print(e)
            exit(1)

        self.__out('OK', 'OKGREEN')

    def run_test_and_upgrade(
            self, pg_service_prod, pg_service_test, pg_service_comp, file,
            table, delta_dir, ignore_list):
        """
        Do the following steps:
            - creates a dump of the production db
            - restores the db dump into a test db
            - applies the delta files found in the delta directory to the test
                db.
            - checks if there are differences between the test db and a
                comparison db
            - if no significant differences are found, after confirmation,
            applies the delta files to the production dbD.


        pg_service_prod: str
            The name of the postgres service (defined in pg_service.conf)
            related to the production database
        pg_service_test:
            The name of the postgres service (defined in pg_service.conf)
            related to the test database
        pg_service_comp:
            The name of the postgres service (defined in pg_service.conf)
            related to the comparison database
        file:
            The path of the desired backup file
        table: str
            The name of the upgrades information table in the format
            schema.table
        delta_dir: str
            The path of the delta directory
        ignore_list: list of strings
            List of elements to be ignored in check (ex. tables, columns,
            views, ...)
        """

        self.__out('Test and upgrade...', type='WAITING')

        # Backup of db prod
        self.run_dump(pg_service_prod, file)

        # Restore db dump on db test
        self.run_restore(pg_service_test, file)

        # Apply deltas on db test
        self.run_upgrade(pg_service_test, table, delta_dir)

        # Compare db test with db comp
        check_result = self.run_check(
            pg_service_test, pg_service_comp, ignore_list)

        if check_result:
            if ask_for_confirmation(prompt='Apply deltas to {}?'.format(
                    pg_service_prod)):
                self.run_upgrade(pg_service_prod, table, delta_dir)
        else:
            # print error
            pass

        self.__out('OK', 'OKGREEN')

    def __out(self, message, type='DEFAULT'):
        # print output of the commands
        if type == 'WAITING':
            print(Bcolors.WAITING + message + Bcolors.ENDC, end='')
        elif type == 'OKGREEN':
            print(Bcolors.OKGREEN + message + Bcolors.ENDC)
        elif type == 'WARNING':
            print(Bcolors.WARNING + message + Bcolors.ENDC)
        elif type == 'FAIL':
            print(Bcolors.FAIL + message + Bcolors.ENDC)
        elif type == 'BOLD':
            print(Bcolors.BOLD + message + Bcolors.ENDC)
        elif type == 'UNDERLINE':
            print(Bcolors.UNDERLINE + message + Bcolors.ENDC)
        else:
            print(message)


if __name__ == "__main__":
    """
    Main process
    """

    # TODO refactor and set p1 and p2 as positional args, and uniform args

    # create the top-level parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v", "--version", help="print the version and exit",
        action='store_true')
    parser.add_argument("-c", "--config_file", help="set the config file")

    subparsers = parser.add_subparsers(
        title='commands', description='valid pum commands', dest='command')

    # create the parser for the "check" command

    parser_check = subparsers.add_parser(
        'check', help='check the differences between two databases')
    parser_check.add_argument(
        '-p1', '--pg_service1', help='Name of the first postgres service',
        required=True)
    parser_check.add_argument(
        '-p2', '--pg_service2', help='Name of the second postgres service',
        required=True)
    parser_check.add_argument(
        '-i', '--ignore', help='Elements to be ignored', nargs='+',
        choices=['tables',
                 'columns',
                 'constraints',
                 'views',
                 'sequences',
                 'indexes',
                 'triggers',
                 'functions',
                 'rules'])
    parser_check.add_argument(
        '-v', '--verbose_level', help='Verbose level (0, 1 or 2)', type=int)

    # create the parser for the "dump" command
    parser_dump = subparsers.add_parser('dump', help='dump a Postgres database')

    parser_dump.add_argument(
        '-p', '--pg_service', help='Name of the postgres service',
        required=True)
    parser_dump.add_argument('file', help='The backup file')

    # create the parser for the "restore" command
    parser_restore = subparsers.add_parser(
        'restore', help='restore a Postgres database from a dump file')
    parser_restore.add_argument(
        '-p', '--pg_service', help='Name of the postgres service',
        required=True)
    parser_restore.add_argument('file', help='The backup file')

    # create the parser for the "baseline" command
    parser_baseline = subparsers.add_parser(
        'baseline', help='Create upgrade information table and set baseline')
    parser_baseline.add_argument(
        '-p', '--pg_service', help='Name of the postgres service',
        required=True)
    parser_baseline.add_argument(
        '-t', '--table', help='Upgrades information table', required=True)
    parser_baseline.add_argument(
        '-d', '--dir', help='Delta directory', required=True)
    parser_baseline.add_argument(
        '-b', '--baseline', help='Set baseline in the format x.x.x',
        required=True)

    # create the parser for the "info" command
    parser_info = subparsers.add_parser('info', help='show info about upgrades')
    parser_info.add_argument(
        '-p', '--pg_service', help='Name of the postgres service',
        required=True)
    parser_info.add_argument(
        '-t', '--table', help='Upgrades information table', required=True)
    parser_info.add_argument(
        '-d', '--dir', help='Set delta directory', required=True)

    # create the parser for the "upgrade" command
    parser_upgrade = subparsers.add_parser('upgrade', help='upgrade db')
    parser_upgrade.add_argument(
        '-p', '--pg_service', help='Name of the postgres service',
        required=True)
    parser_upgrade.add_argument(
        '-t', '--table', help='Upgrades information table', required=True)
    parser_upgrade.add_argument(
        '-d', '--dir', help='Set delta directory', required=True)

    # create the parser for the "test-and-upgrade" command
    parser_test_and_upgrade = subparsers.add_parser(
        'test-and-upgrade',
        help='try the upgrade on a test db and if all it\'s ok, do upgrade '
             'the production db')
    parser_test_and_upgrade.add_argument(
        '-pp', '--pg_service_prod',
        help='Name of the pg_service related to production db')
    parser_test_and_upgrade.add_argument(
        '-pt', '--pg_service_test',
        help='Name of the pg_service related to a test db used to test the '
             'migration')
    parser_test_and_upgrade.add_argument(
        '-pc', '--pg_service_comp',
        help='Name of the pg_service related to a db used to compare the '
             'updated db test with the last version of the db')
    parser_test_and_upgrade.add_argument(
        '-t', '--table', help='Upgrades information table')
    parser_test_and_upgrade.add_argument(
        '-d', '--dir', help='Set delta directory')
    parser_test_and_upgrade.add_argument('-f', '--file', help='The backup file')
    parser_test_and_upgrade.add_argument(
        '-i', '--ignore', help='Elements to be ignored', nargs='+',
        choices=['tables',
                 'columns',
                 'constraints',
                 'views',
                 'sequences',
                 'indexes',
                 'triggers',
                 'functions',
                 'rules'])

    args = parser.parse_args()

    # print the version and exit
    if args.version:
        print('pum version {}'.format('0.0.1'))
        parser.exit()

    # if no command is passed, print the help and exit
    if not args.command:
        parser.print_help()
        parser.exit()

    pum = Pum(args.config_file)

    if args.command == 'check':
        pum.run_check(args.pg_service1, args.pg_service2, args.ignore,
                      args.verbose_level)
    elif args.command == 'dump':
        pum.run_dump(args.pg_service, args.file)
    elif args.command == 'restore':
        pum.run_restore(args.pg_service, args.file)
    elif args.command == 'baseline':
        pum.run_baseline(args.pg_service, args.table, args.dir, args.baseline)
    elif args.command == 'info':
        pum.run_info(args.pg_service, args.table, args.dir)
    elif args.command == 'upgrade':
        pum.run_upgrade(args.pg_service, args.table, args.dir)
    elif args.command == 'test-and-upgrade':
        pum.run_test_and_upgrade(
            args.pg_service_prod, args.pg_service_test, args.pg_service_comp,
            args.file, args.table, args.dir, args.ignore)
