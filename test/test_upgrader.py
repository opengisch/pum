import os
import shutil
import unittest

import psycopg2
import psycopg2.extras
from pum.core.upgrader import Upgrader, Delta, DeltaType


class TestUpgrader(unittest.TestCase):
    """Test the class Upgrader.

    1 pg_service needed for test:
        pum_test_1
    """

    def tearDown(self):
        del self.upgrader

        self.cur1.execute('DROP SCHEMA IF EXISTS test_upgrader CASCADE;')
        self.conn1.commit()
        self.conn1.close()

    def setUp(self):
        pg_service1 = 'pum_test_1'
        self.upgrades_table = 'test_upgrader.upgrades'

        self.conn1 = psycopg2.connect("service={0}".format(pg_service1))
        self.cur1 = self.conn1.cursor()

        self.cur1.execute("""
            DROP SCHEMA IF EXISTS test_upgrader CASCADE;
            CREATE SCHEMA test_upgrader;
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
            """.format(self.upgrades_table))
        self.conn1.commit()

        try:
            shutil.rmtree('/tmp/pum_deltas_1')
            shutil.rmtree('/tmp/pum_deltas_2')
        except OSError:
            pass

        os.mkdir('/tmp/pum_deltas_1/')
        os.mkdir('/tmp/pum_deltas_2/')

        with open('/tmp/pum_deltas_1/delta_0.0.1_0.sql', 'w+') as f:
            f.write('DROP TABLE IF EXISTS test_upgrader.bar;')
            f.write(
                'CREATE TABLE test_upgrader.bar '
                '(id smallint, value integer, name varchar(100));')

        with open('/tmp/pum_deltas_1/delta_0.0.1_a.sql', 'w+') as f:
            f.write('SELECT 2;')

        with open('/tmp/pum_deltas_1/delta_0.0.1_1.sql', 'w+') as f:
            f.write('SELECT 1;')

        with open('/tmp/pum_deltas_2/delta_0.0.1_0.sql', 'w+') as f:
            f.write('SELECT 3;')

        self.upgrader = Upgrader(
            pg_service1, self.upgrades_table, ['/tmp/pum_deltas_1/', '/tmp/pum_deltas_2/'])
        self.upgrader.set_baseline('0.0.1')

    def test_upgrader_run(self):
        self.upgrader.run()
        # postgres > 9.4
        self.cur1.execute(
            "SELECT to_regclass('{}');".format(self.upgrades_table))
        self.assertIsNotNone(self.cur1.fetchone()[0])

        self.cur1.execute(
            "SELECT description from {};".format(self.upgrades_table))
        results = self.cur1.fetchall()
        self.assertEqual(len(results), 5)
        self.assertEqual(results[0][0], 'baseline')
        self.assertEqual(results[1][0], '0')
        self.assertEqual(results[2][0], '1')
        self.assertEqual(results[3][0], 'a')
        self.assertEqual(results[4][0], '0')

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
        self.assertEqual(delta.get_type(), DeltaType.SQL)

        delta = Delta('delta_0.0.0_17072017.py')
        self.assertEqual(delta.get_type(), DeltaType.PYTHON)

        delta = Delta('delta_0.0.0_17072017.pre.sql')
        self.assertEqual(delta.get_type(), DeltaType.PRE_SQL)

        delta = Delta('delta_0.0.0_17072017.pre.py')
        self.assertEqual(delta.get_type(), DeltaType.PRE_PYTHON)

        delta = Delta('delta_0.0.0_17072017.post.sql')
        self.assertEqual(delta.get_type(), DeltaType.POST_SQL)

        delta = Delta('delta_0.0.0_17072017.post.py')
        self.assertEqual(delta.get_type(), DeltaType.POST_PYTHON)

if __name__ == '__main__':
    unittest.main()
