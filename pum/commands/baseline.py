# -*- coding: utf-8 -*-

import psycopg2
import re

from commands.PUMCommand import PUMCommand


class Baseline(PUMCommand):

    def add_cli_parser(subparsers):
        parser = subparsers.add_parser(
            'baseline',
            help='Create upgrade information table and set baseline')
        parser.add_argument(
            '-p', '--pg_service', help='Name of the postgres service',
            required=True)
        parser.add_argument(
            '-t', '--table', help='Upgrades information table', required=True)
        parser.add_argument(
            '-b', '--baseline', help='Set baseline in the format x.x.x',
            required=True)
        parser.set_defaults(func=Baseline.run)

    def run(args):
        pg_service = args.pg_service
        connection = psycopg2.connect("service={}".format(pg_service))
        cursor = connection.cursor()
        info_table = args.table
        baseline = args.baseline

        """Create the upgrades information table"""

        query = """CREATE TABLE IF NOT EXISTS {}
                (
                id serial NOT NULL,
                version character varying(50),
                description character varying(200) NOT NULL,
                type integer NOT NULL,
                script character varying(1000) NOT NULL,
                checksum character varying(32) NOT NULL,
                installed_by character varying(100) NOT NULL,
                installed_on timestamp without time zone
                    NOT NULL DEFAULT now(),
                execution_time integer NOT NULL,
                success boolean NOT NULL,
                PRIMARY KEY (id)
                )
        """.format(info_table)

        cursor.execute(query)
        connection.commit()

        """Set the baseline into the creation information table

        version: str
            The version of the current database to set in the information
        table. The baseline must be in the format x.x.x where x are numbers.
        """
        pattern = re.compile(r"^\d+\.\d+\.\d+$")
        if not re.match(pattern, baseline):
            raise ValueError('Wrong version format')

        dbuser = connection.get_dsn_parameters()['user']

        query = """
                INSERT INTO {} (
                    version,
                    description,
                    type,
                    script,
                    checksum,
                    installed_by,
                    execution_time,
                    success
                ) VALUES(
                    '{}',
                    '{}',
                    {},
                    '{}',
                    '{}',
                    '{}',
                    1,
                    TRUE
                ) """.format(info_table, baseline, 'baseline', 0,
                             '', '', dbuser)
        cursor.execute(query)
        connection.commit()
