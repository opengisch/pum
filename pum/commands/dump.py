# -*- coding: utf-8 -*-

import subprocess

from .PUMCommand import PUMCommand


class Dump(PUMCommand):

    def add_cli_parser(subparsers):
        parser = subparsers.add_parser(
            'dump', help='dump a Postgres database')

        parser.add_argument(
            '-p', '--pg_service', help='Name of the postgres service',
            required=True)
        parser.add_argument('file', help='The backup file')

        parser.set_defaults(func=Dump.run)

    def run(args):
        pg_service = args.pg_service
        file_path = args.file

        # TODO get from configs or arguments?
        pg_dump_exe = 'pg_dump'

        command = [
            pg_dump_exe, '-Fc', '-f', file_path,
            'service={}'.format(pg_service)
        ]

        subprocess.check_output(command, stderr=subprocess.STDOUT)
