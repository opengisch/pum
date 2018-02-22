# -*- coding: utf-8 -*-

import argparse

from commands.info import Info
from commands.baseline import Baseline
from commands.check import Check


class Pum(object):

    COMMANDS = [
        Info,
        Baseline, 
        Check
    ]

    def __init__(self):
        pass

    def create_cli(self):

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-v", "--version", help="print the version and exit",
            action='store_true')
        parser.add_argument("-c", "--config_file", help="set the config file")

        subparsers = parser.add_subparsers(
            title='commands', description='valid pum commands', dest='command')

        # create the parser for the "check" command

        for command in Pum.COMMANDS:
            command.add_cli_parser(subparsers)

        args = parser.parse_args()

        # print the version and exit
        if args.version:
            print('pum version {}'.format('[DEV]'))
            parser.exit()

        # if no command is passed, print the help and exit
        if not args.command:
            parser.print_help()
            parser.exit()

        # call the function defined into the command defaults
        args.func(args)

    # TODO
    def create_python_api(self):
        pass

    # TODO inspect all the imported modules to get a list of subclasses of PUMCommand


if __name__ == "__main__":

    pum = Pum()
    pum.create_cli()
