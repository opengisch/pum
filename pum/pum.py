#!/usr/bin/env python

from __future__ import print_function

import argparse
import yaml

from core.checker import Checker
from core.dumper import Dumper
from core.upgrader import Upgrader


class Pum():
    def __init__(self, config_file=None):
        if config_file:
            self.__load_config_file()

    def __load_config_file(self):
        """Load the configurations from yaml configuration file and store it
        to instance variables."""
        config = yaml.safe_load(open(self.config_file))

        # TODO which variables to load?

        self.upgrades_table = config['upgrades_table']
        self.delta_dir = config['delta_dir']
        self.backup_file = config['backup_file']
        self.ignore_list = config['ignore_elements']

    def set_configs(self, configs):
        # TODO receive a dict of configs, used when is called from a python
        # program and not from command line
        pass

    def run_check(
            self, pg_service1, pg_service2, ignore_list=None):

        if not ignore_list:
            ignore_list = []
        try:
            db_checker = Checker(
                pg_service1, pg_service2, ignore_list)
            result, differences = db_checker.run_checks()

            print('result: {}'.format(result))
            print('differences: {}'.format(differences))

        # TODO exceptions raised by checker
        except Exception:
            raise Exception
            pass
            # print message error and return or exit ?
        # print message ok

    def run_dump(self):
        pass

    def run_restore(self):
        pass

    def run_baseline(self):
        pass

    def run_info(self):
        pass

    def out(self, message, type):
        # print output of the commands
        pass

if __name__ == "__main__":
    """
    Main process
    """

    # TODO refactor and set p1 and p2 as positional args, an uniform args

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
        '-s', '--silent', help='Don\'t print lines with differences')
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
        '-d', '--dir', help='Set delta directory', required=True)

    # create the parser for the "info" command
    parser_info = subparsers.add_parser('info', help='show info about upgrades')
    parser_info.add_argument(
        '-p', '--pg_service', help='Name of the postgres service',
        required=True)
    parser_info.add_argument(
        '-t', '--table', help='Upgrades information table', required=True)
    parser_info.add_argument(
        '-d', '--dir', help='Set delta directory', required=True)

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
        pum.run_check(args.pg_service1, args.pg_service2, args.ignore)


