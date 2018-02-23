import os
import shutil
from unittest import TestCase

import psycopg2
import psycopg2.extras

from commands.dump import Dump


class TestDump(TestCase):

    PG_SERVICE = "pum_test_1"

    def setUp(self):
        self.conn1 = psycopg2.connect("service={0}".format(
            TestDump.PG_SERVICE))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""
            CREATE SCHEMA test_dumper;
            CREATE TABLE test_dumper.dumper_table
                (
                id serial NOT NULL,
                version character varying(50),
                description character varying(200) NOT NULL,
                type integer NOT NULL
                );
            """)
        self.conn1.commit()

        os.mkdir('/tmp/test_dumper/')

    def tearDown(self):
        self.conn1 = psycopg2.connect("service={0}".format(
            TestDump.PG_SERVICE))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""DROP SCHEMA test_dumper CASCADE;""")
        self.conn1.commit()

        shutil.rmtree('/tmp/test_dumper')

    def test_dump(self):
        args = Args()
        args.pg_service = TestDump.PG_SERVICE
        args.file = '/tmp/test_dumper/dump'
        args.verbose_level = 0

        Dump.run(args)

        self.assertTrue(os.path.isfile(args.file))

    # TODO add more tests


class Args(object):
    pass
