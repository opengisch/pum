import logging
import tempfile
import unittest
from pathlib import Path

import psycopg

from pum.pum_config import PumConfig
from pum.exceptions import PumException
from pum.schema_migrations import SchemaMigrations
from pum.upgrader import Upgrader


class TestSchemaMigrations(unittest.TestCase):
    """Test the class Upgrader."""

    def tearDown(self) -> None:
        """Clean up the test environment."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_custom_migrations_schema CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_test_app CASCADE;")
            cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")

        self.tmpdir.cleanup()
        self.tmp = None

    def setUp(self) -> None:
        """Set up the test environment."""
        logging.basicConfig(level=logging.INFO, format="%(message)s")

        self.maxDiff = 5000

        self.pg_service = "pum_test"

        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_custom_migrations_schema CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_test_app CASCADE;")
            cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = self.tmpdir.name

    def test_install_single_changelog(self) -> None:
        """Test the installation of a single changelog."""
        test_dir = Path("test") / "data" / "single_changelog"
        changelog_file = test_dir / "changelogs" / "1.2.3" / "single_changelog.sql"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))
            self.assertEqual(sm.baseline(conn), "1.2.3")
            self.assertEqual(sm.migration_details(conn), sm.migration_details(conn, "1.2.3"))
            self.assertEqual(sm.migration_details(conn)["version"], "1.2.3")
            self.assertEqual(
                sm.migration_details(conn)["changelog_files"],
                [str(changelog_file)],
            )

    def test_baseline(self) -> None:
        """Test the baselie"""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertIsNone(sm.baseline(connection=conn))
            sm.create(connection=conn)
            self.assertIsNone(sm.baseline(connection=conn))
            sm.set_baseline(connection=conn, version="1.2.3")
            self.assertEqual(sm.baseline(connection=conn), "1.2.3")
            with self.assertRaises(PumException):
                sm.set_baseline(connection=conn, version="1.2.3")
            sm.set_baseline(connection=conn, version="1.2.4")
