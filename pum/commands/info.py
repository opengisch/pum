# -*- coding: utf-8 -*-

import psycopg2

from .PUMCommand import PUMCommand


class Info(PUMCommand):

    def add_cli_parser(subparsers):
        parser = subparsers.add_parser(
            'info', help='show info about database migrations')
        parser.add_argument(
            '-p', '--pg_service', help='Name of the postgres service',
            required=True)
        parser.add_argument(
            '-t', '--table', help='Upgrades information table', required=True)
        parser.set_defaults(func=Info.run)

    def run(args):
        print('type args {}'.format(type(args)))
        pg_service = args.pg_service
        connection = psycopg2.connect("service={}".format(pg_service))
        cursor = connection.cursor()
        info_table = args.table

        query = """SELECT
                version,
                description,
                type,
                installed_by,
                installed_on,
                success
                FROM {}
                """.format(info_table)

        cursor.execute(query)
        records = cursor.fetchall()

        for _ in records:
            Info.write_output(_)
