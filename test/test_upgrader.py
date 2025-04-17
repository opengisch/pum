import logging
import os
import shutil
import tempfile
import unittest

import psycopg

from pum.config import PumConfig
from pum.schema_migrations import SchemaMigrations
from pum.upgrader import Delta, DeltaType, Upgrader


class TestUpgrader(unittest.TestCase):
    """
    Test the class Upgrader.
    """

    def tearDown(self):
        self.cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
        self.cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")
        self.conn.commit()
        self.conn.close()

        self.tmpdir.cleanup()
        self.tmp = None

    def setUp(self):
        logging.basicConfig(level=logging.INFO, format="%(message)s")

        self.pg_service = "pum_test"
        self.conn = psycopg.connect(f"service={self.pg_service}")
        self.cur = self.conn.cursor()
        self.cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
        self.cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")
        self.conn.commit()

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = self.tmpdir.name

    def test_install_simple(self):
        cfg = PumConfig()
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service, config=cfg, dir="test/data/simple"
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))
        self.assertEqual(sm.baseline(self.conn), "1.2.3")
        self.assertEqual(
            sm.migration_details(self.conn), sm.migration_details(self.conn, "1.2.3")
        )
        self.assertEqual(sm.migration_details(self.conn)["version"], "1.2.3")
        self.assertEqual(
            sm.migration_details(self.conn)["changelog_files"],
            ["test/data/simple/changelogs/1.2.3/create_northwind.sql"],
        )

    def test_parameters(self):
        cfg = PumConfig()
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir="test/data/parameters",
        )
        upgrader.install(parameters={"SRID": 2056})
        self.assertTrue(sm.exists(self.conn))
        self.assertEqual(sm.migration_details(self.conn)["parameters"], "{'SRID': 2056}")
        self.cur.execute("SELECT Find_SRID('public', 'pum_migrations', 'geometry_column_name');")
        srid = self.cur.fetchone()[0]
        self.assertEqual(srid, 2056)

    def test_install_custom_directory(self):
        cfg = PumConfig.from_yaml("test/data/custom_directory/.pum-config.yaml")
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir="test/data/custom_directory",
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))


if __name__ == "__main__":
    unittest.main()
