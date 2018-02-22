import os
import shutil
from unittest import TestCase

import psycopg2
import psycopg2.extras
from pum.commands.upgrade import Upgrade, Delta
from pum.commands.baseline import Baseline


class TestUpgrade(TestCase):

    PG_SERVICE = "pum_test_1"

    def setUp(self):
        self.info_table = 'test_upgrade.info_table'

        self.conn1 = psycopg2.connect("service={0}".format(
            TestUpgrade.PG_SERVICE))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""
            CREATE SCHEMA test_upgrade;
            CREATE TABLE {}
                (
                id serial NOT NULL,
                version character varying(50),
                description character varying(200) NOT NULL,
                type integer NOT NULL,
                script character varying(1000) NOT NULL,
                checksum character varying(32) NOT NULL,
                installed_by character varying(100) NOT NULL,
                installed_on timestamp without time zone NOT NULL DEFAULT now(),
                execution_time integer NOT NULL,
                success boolean NOT NULL,
                CONSTRAINT upgrades_pk PRIMARY KEY (id)
                );
            """.format(self.info_table))
        self.conn1.commit()

        os.mkdir('/tmp/test_upgrade/')

        with open('/tmp/test_upgrade/delta_0.0.1_0.sql', 'w+') as f:
            f.write('DROP TABLE IF EXISTS test_upgrade.bar;')
            f.write(
                'CREATE TABLE test_upgrade.bar '
                '(id smallint, value integer, name varchar(100));')

        with open('/tmp/test_upgrade/delta_0.0.1_a.sql', 'w+') as f:
            f.write('SELECT 2;')

        with open('/tmp/test_upgrade/delta_0.0.1_1.sql', 'w+') as f:
            f.write('SELECT 1;')

        args = Args()
        args.pg_service = TestUpgrade.PG_SERVICE
        args.table = self.info_table
        args.baseline = '0.0.1'
        Baseline.run(args)

    def tearDown(self):
        self.info_table = 'test_upgrade.upgrades'

        self.conn1 = psycopg2.connect("service={0}".format(
            TestUpgrade.PG_SERVICE))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""DROP SCHEMA IF EXISTS test_upgrade CASCADE;""")
        self.conn1.commit()

        shutil.rmtree('/tmp/test_upgrade')

    def test_upgrade(self):
        args = Args()
        args.pg_service = TestUpgrade.PG_SERVICE
        args.table = self.info_table
        args.dir = '/tmp/test_upgrade/'
        Upgrade.run(args)

        # postgres > 9.4
        self.cur1.execute(
            "SELECT to_regclass('{}');".format(self.info_table))
        self.assertIsNotNone(self.cur1.fetchone()[0])

        self.cur1.execute(
            "SELECT description from {};".format(self.info_table))
        results = self.cur1.fetchall()
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0][0], 'baseline')
        self.assertEqual(results[1][0], '0')
        self.assertEqual(results[2][0], '1')
        self.assertEqual(results[3][0], 'a')

    def test_delta_valid_name(self):
        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_17072017.py'))
        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_17072017.sql'))

        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_17072017.pre.py'))
        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_17072017.pre.sql'))

        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_17072017.post.py'))
        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_17072017.post.sql'))

        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0.sql'))
        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_blahblah_foo_bar.sql'))

        self.assertFalse(Delta.is_valid_delta_name('1.1.0_17072017.sql'))
        self.assertFalse(Delta.is_valid_delta_name('Delta_1.1.0_17072017.sql'))
        self.assertFalse(Delta.is_valid_delta_name('delta_1.1.0_17072017'))
        self.assertFalse(Delta.is_valid_delta_name('delta_1.1.0_17072017.post'))
        self.assertFalse(Delta.is_valid_delta_name('delta_1.1.0_17072017.pre'))
        self.assertFalse(Delta.is_valid_delta_name('delta_1.1_17072017.sql'))

    def test_delta_get_version(self):
        delta = Delta('delta_0.0.0_17072017.sql')
        self.assertEqual(delta.get_version(), '0.0.0')

        delta = Delta('delta_1.2.3_17072017.pre.sql')
        self.assertEqual(delta.get_version(), '1.2.3')

        delta = Delta('delta_100.002.9999_17072017.post.sql')
        self.assertEqual(delta.get_version(), '100.002.9999')

    def test_delta_get_name(self):
        delta = Delta('delta_0.0.0_17072017.sql')
        self.assertEqual(delta.get_name(), '17072017')

        delta = Delta('delta_0.0.0_17072017.py')
        self.assertEqual(delta.get_name(), '17072017')

        delta = Delta('delta_0.0.0_.sql')
        self.assertEqual(delta.get_name(), '')

        delta = Delta('delta_0.0.0_.py')
        self.assertEqual(delta.get_name(), '')

        delta = Delta('delta_0.0.0.sql')
        self.assertEqual(delta.get_name(), '')

        delta = Delta('delta_0.0.0.py')
        self.assertEqual(delta.get_name(), '')

        delta = Delta('delta_0.0.0_foo.pre.sql')
        self.assertEqual(delta.get_name(), 'foo')

        delta = Delta('delta_0.0.0_foo.pre.py')
        self.assertEqual(delta.get_name(), 'foo')

        delta = Delta('delta_0.0.0_foo.post.sql')
        self.assertEqual(delta.get_name(), 'foo')

        delta = Delta('delta_0.0.0_foo.post.py')
        self.assertEqual(delta.get_name(), 'foo')

    def test_delta_get_checksum(self):
        file = open('/tmp/foo.bar', 'w+')
        delta = Delta('/tmp/foo.bar')
        self.assertEqual(
            delta.get_checksum(), 'd41d8cd98f00b204e9800998ecf8427e')
        file.write('The quick brown fox jumps over the lazy dog')
        file.close()
        self.assertEqual(
            delta.get_checksum(), '9e107d9d372bb6826bd81d3542a419d6')

    def test_delta_get_type(self):
        delta = Delta('delta_0.0.0_17072017.sql')
        self.assertEqual(delta.get_type(), Delta.DELTA_SQL)

        delta = Delta('delta_0.0.0_17072017.py')
        self.assertEqual(delta.get_type(), Delta.DELTA_PY)

        delta = Delta('delta_0.0.0_17072017.pre.sql')
        self.assertEqual(delta.get_type(), Delta.DELTA_PRE_SQL)

        delta = Delta('delta_0.0.0_17072017.pre.py')
        self.assertEqual(delta.get_type(), Delta.DELTA_PRE_PY)

        delta = Delta('delta_0.0.0_17072017.post.sql')
        self.assertEqual(delta.get_type(), Delta.DELTA_POST_SQL)

        delta = Delta('delta_0.0.0_17072017.post.py')
        self.assertEqual(delta.get_type(), Delta.DELTA_POST_PY)


class Args(object):
    pass