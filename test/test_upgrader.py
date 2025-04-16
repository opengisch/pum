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


if __name__ == "__main__":
    unittest.main()
