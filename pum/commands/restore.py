# -*- coding: utf-8 -*-

import subprocess

from commands.PUMCommand import PUMCommand


class Restore(PUMCommand):

    def add_cli_parser(subparsers):
        parser = subparsers.add_parser(
            'restore', help='restore a Postgres database from a dump file')

        parser.add_argument(
            '-p', '--pg_service', help='Name of the postgres service',
            required=True)
        parser.add_argument(
            '-x', help='ignore pg_restore errors', action="store_true")
        parser.add_argument('file', help='The backup file')

        parser.set_defaults(func=Restore.run)

    def run(args):
        pg_service = args.pg_service
        file_path = args.file

        # TODO get from configs or arguments?
        pg_restore_exe = 'pg_restore'

        command = [
            pg_restore_exe, '-d',
            'service={}'.format(pg_service), file_path]

        subprocess.check_output(command, stderr=subprocess.STDOUT)
