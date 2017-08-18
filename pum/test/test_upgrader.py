import os
import shutil
from unittest import TestCase

import psycopg2
import psycopg2.extras

from pum.core.upgrader import Upgrader, Delta


class TestUpgrader(TestCase):
    """Test the class Upgrader.
    
    1 pg_service needed for test:
        qwat_test_1 
    """

    def setUp(self):
        pg_service1 = 'qwat_test_1'
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
            shutil.rmtree('/tmp/test_upgrader')
        except Exception:
            pass
        
        os.mkdir('/tmp/test_upgrader/')

        file = open('/tmp/test_upgrader/delta_0.0.1.sql', 'w+')
        file.write('DROP TABLE IF EXISTS test_upgrader.bar;')
        file.write(
            'CREATE TABLE test_upgrader.bar '
            '(id smallint, value integer, name varchar(100));')
        file.close()

        self.upgrader = Upgrader(
            pg_service1, self.upgrades_table, '/tmp/test_upgrader/')
        self.upgrader.set_baseline('0.0.1')

    def test_upgrader_run(self):
        self.upgrader.run()
        #postgres > 9.4
        self.cur1.execute(
            "SELECT to_regclass('{}');".format(self.upgrades_table))
        self.assertIsNotNone(self.cur1.fetchone()[0])

    def test_delta_valid_name(self):
        self.assertTrue(Delta.is_valid_delta_name('delta_1.1.0_17072017.sql'))
        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_17072017.sql.pre'))
        self.assertTrue(
            Delta.is_valid_delta_name('delta_1.1.0_17072017.sql.post'))
        self.assertTrue(Delta.is_valid_delta_name('delta_1.1.0.sql'))
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

        delta = Delta('delta_1.2.3_17072017.sql')
        self.assertEqual(delta.get_version(), '1.2.3')

        delta = Delta('delta_100.002.9999_17072017.sql')
        self.assertEqual(delta.get_version(), '100.002.9999')

    def test_delta_get_name(self):
        delta = Delta('delta_0.0.0_17072017.sql')
        self.assertEqual(delta.get_name(), '17072017')

        delta = Delta('delta_0.0.0_.sql')
        self.assertEqual(delta.get_name(), '')

        delta = Delta('delta_0.0.0.sql')
        self.assertEqual(delta.get_name(), '')

        delta = Delta('delta_0.0.0_foo.sql.pre')
        self.assertEqual(delta.get_name(), 'foo')


        delta = Delta('delta_0.0.0_foo.sql.post')
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
        self.assertEqual(delta.get_type(), Delta.DELTA)

        delta = Delta('delta_0.0.0_17072017.sql.pre')
        self.assertEqual(delta.get_type(), Delta.PRE)

        delta = Delta('delta_0.0.0_17072017.sql.post')
        self.assertEqual(delta.get_type(), Delta.POST)