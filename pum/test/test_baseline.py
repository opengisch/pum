import psycopg2
from unittest import TestCase

from pum.commands.baseline import Baseline


class TestBaseline(TestCase):

    PG_SERVICE = "pum_test_1"

    def setUp(self):
        self.info_table = 'test_baseline.info_table'

        self.conn1 = psycopg2.connect("service={0}".format(
            TestBaseline.PG_SERVICE))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""
            CREATE SCHEMA test_baseline;
        """)
        self.conn1.commit()

    def tearDown(self):
        self.conn1 = psycopg2.connect("service={0}".format(
            TestBaseline.PG_SERVICE))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""
            DROP SCHEMA test_baseline CASCADE;
        """)
        self.conn1.commit()

    def test_set_baseline_without_info_table(self):
        args = Args()
        args.pg_service = TestBaseline.PG_SERVICE
        args.table = self.info_table
        args.baseline = '1.0.0'
        Baseline.run(args)

        query = """
         SELECT version from {} WHERE success = TRUE
        """.format(self.info_table)

        self.cur1.execute(query)
        self.assertEqual(self.cur1.fetchone()[0], '1.0.0')

    def test_set_baseline_with_info_table(self):
        args = Args()
        args.pg_service = TestBaseline.PG_SERVICE
        args.table = self.info_table
        args.baseline = '1.0.0'
        Baseline.run(args)

        args.baseline = '2.0.1'
        Baseline.run(args)

        query = """
         SELECT version from {} WHERE success = TRUE ORDER BY version DESC
        """.format(self.info_table)

        self.cur1.execute(query)
        records = self.cur1.fetchall()
        self.assertEqual(records[0][0], '2.0.1')
        self.assertEqual(records[1][0], '1.0.0')


class Args(object):
    pass
