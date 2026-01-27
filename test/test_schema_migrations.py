import logging
import tempfile
import unittest
from pathlib import Path
from packaging.version import Version

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
        cfg = PumConfig(test_dir, pum={"module": "test_single_changelog"})
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))
            self.assertEqual(sm.baseline(conn), Version("1.2.3"))
            self.assertEqual(sm.migration_details(conn), sm.migration_details(conn, "1.2.3"))
            self.assertEqual(sm.migration_details(conn)["version"], "1.2.3")
            self.assertEqual(
                sm.migration_details(conn)["changelog_files"],
                [str(changelog_file)],
            )

    def test_baseline(self) -> None:
        """Test the baselie"""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir, pum={"module": "test_single_changelog"})
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.has_baseline(connection=conn))
            sm.create(connection=conn)
            self.assertFalse(sm.has_baseline(connection=conn))
            sm.set_baseline(connection=conn, version="1.2.3")
            self.assertTrue(sm.has_baseline(connection=conn))
            self.assertEqual(sm.baseline(connection=conn), Version("1.2.3"))
            with self.assertRaises(PumException):
                sm.set_baseline(connection=conn, version="1.2.3")
            sm.set_baseline(connection=conn, version="1.2.4")
            self.assertEqual(sm.baseline(connection=conn), Version("1.2.4"))

    def test_compare(self) -> None:
        """Test the compare method."""
        test_dir = Path("test") / "data" / "multiple_changelogs"
        cfg = PumConfig(test_dir, pum={"module": "test_multiple_changelogs"})
        sm = SchemaMigrations(cfg)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Create the migrations table and set baseline
            sm.create(connection=conn)
            sm.set_baseline(connection=conn, version="1.2.3")

            # Database is behind since there are more changelogs (1.2.4, 1.3.0, 2.0.0)
            result = sm.compare(connection=conn)
            self.assertEqual(result, -1)

            # Add more migrations to match all changelogs
            sm.set_baseline(connection=conn, version="1.2.4")
            result = sm.compare(connection=conn)
            self.assertEqual(result, -1)  # Still behind

            sm.set_baseline(connection=conn, version="1.3.0")
            result = sm.compare(connection=conn)
            self.assertEqual(result, -1)  # Still behind

            sm.set_baseline(connection=conn, version="2.0.0")
            # Now database should be up to date
            result = sm.compare(connection=conn)
            self.assertEqual(result, 0)

    def test_compare_error_version_not_in_changelog(self) -> None:
        """Test the compare method raises error when migration version is not in changelogs."""
        test_dir = Path("test") / "data" / "multiple_changelogs"
        cfg = PumConfig(test_dir, pum={"module": "test_multiple_changelogs"})
        sm = SchemaMigrations(cfg)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Create the migrations table and set baseline with a version not in changelogs
            sm.create(connection=conn)
            sm.set_baseline(connection=conn, version="9.9.9")

            # Should raise error because 9.9.9 is not in the changelogs
            with self.assertRaises(PumException) as context:
                sm.compare(connection=conn)

            self.assertIn(
                "Changelog for version 9.9.9 not found in the source", str(context.exception)
            )
