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

    def test_schemas_with_migrations(self) -> None:
        """Test the schemas_with_migrations static method."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg1 = PumConfig(
            test_dir, pum={"module": "test_module1", "migration_table_schema": "public"}
        )
        cfg2 = PumConfig(
            test_dir,
            pum={
                "module": "test_module2",
                "migration_table_schema": "pum_custom_migrations_schema",
            },
        )

        sm1 = SchemaMigrations(cfg1)
        sm2 = SchemaMigrations(cfg2)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Initially, no schemas have migrations
            schemas = SchemaMigrations.schemas_with_migrations(conn)
            self.assertEqual(schemas, [])

            # Create migration table in public schema
            sm1.create(connection=conn, allow_multiple_modules=True)
            schemas = SchemaMigrations.schemas_with_migrations(conn)
            self.assertEqual(schemas, ["public"])

            # Create migration table in custom schema
            sm2.create(connection=conn, allow_multiple_modules=True)
            schemas = SchemaMigrations.schemas_with_migrations(conn)
            self.assertIn("public", schemas)
            self.assertIn("pum_custom_migrations_schema", schemas)
            self.assertEqual(len(schemas), 2)

            # Test exists_in_other_schemas for sm1
            other_schemas = sm1.exists_in_other_schemas(conn)
            self.assertEqual(other_schemas, ["pum_custom_migrations_schema"])

            # Test exists_in_other_schemas for sm2
            other_schemas = sm2.exists_in_other_schemas(conn)
            self.assertEqual(other_schemas, ["public"])

    def test_schemas_with_migration_details_empty(self) -> None:
        """Test schemas_with_migration_details when no migrations exist."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            details = SchemaMigrations.schemas_with_migration_details(conn)
            self.assertEqual(details, [])

    def test_schemas_with_migration_details_single_schema(self) -> None:
        """Test schemas_with_migration_details with a single installed module."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir, pum={"module": "test_module"})
        sm = SchemaMigrations(cfg)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            sm.create(connection=conn)
            sm.set_baseline(connection=conn, version="1.2.3")

            details = SchemaMigrations.schemas_with_migration_details(conn)
            self.assertEqual(len(details), 1)
            detail = details[0]
            self.assertEqual(detail["schema"], "public")
            self.assertEqual(detail["module"], "test_module")
            self.assertEqual(detail["version"], "1.2.3")
            self.assertIsNotNone(detail["installed_date"])
            self.assertIsNone(detail["upgrade_date"])  # No upgrade yet
            self.assertFalse(detail["beta_testing"])

    def test_schemas_with_migration_details_after_upgrade(self) -> None:
        """Test schemas_with_migration_details shows upgrade_date after a second baseline."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir, pum={"module": "test_module"})
        sm = SchemaMigrations(cfg)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            sm.create(connection=conn)
            sm.set_baseline(connection=conn, version="1.2.3", commit=True)

        # Use a separate connection/transaction so now() differs
        with psycopg.connect(f"service={self.pg_service}") as conn:
            sm.set_baseline(connection=conn, version="1.2.4", commit=True)

            details = SchemaMigrations.schemas_with_migration_details(conn)
            self.assertEqual(len(details), 1)
            detail = details[0]
            self.assertEqual(detail["version"], "1.2.4")
            self.assertIsNotNone(detail["installed_date"])
            self.assertIsNotNone(detail["upgrade_date"])  # Upgrade happened

    def test_schemas_with_migration_details_multiple_schemas(self) -> None:
        """Test schemas_with_migration_details with modules in different schemas."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg1 = PumConfig(test_dir, pum={"module": "module_a", "migration_table_schema": "public"})
        cfg2 = PumConfig(
            test_dir,
            pum={"module": "module_b", "migration_table_schema": "pum_custom_migrations_schema"},
        )
        sm1 = SchemaMigrations(cfg1)
        sm2 = SchemaMigrations(cfg2)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            sm1.create(connection=conn, allow_multiple_modules=True)
            sm1.set_baseline(connection=conn, version="1.0.0")

            sm2.create(connection=conn, allow_multiple_modules=True)
            sm2.set_baseline(connection=conn, version="2.0.0")

            details = SchemaMigrations.schemas_with_migration_details(conn)
            self.assertEqual(len(details), 2)

            by_schema = {d["schema"]: d for d in details}
            self.assertIn("public", by_schema)
            self.assertIn("pum_custom_migrations_schema", by_schema)
            self.assertEqual(by_schema["public"]["module"], "module_a")
            self.assertEqual(by_schema["public"]["version"], "1.0.0")
            self.assertEqual(by_schema["pum_custom_migrations_schema"]["module"], "module_b")
            self.assertEqual(by_schema["pum_custom_migrations_schema"]["version"], "2.0.0")

    def test_upgrade_module_mismatch(self) -> None:
        """Test that upgrading module X over an installation of module Y is rejected."""
        test_dir = Path("test") / "data" / "single_changelog"

        # Install module_a
        cfg_a = PumConfig(test_dir, pum={"module": "module_a"})
        upgrader_a = Upgrader(config=cfg_a)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            upgrader_a.install(connection=conn)

        # Try to upgrade with module_b config — should fail
        cfg_b = PumConfig(test_dir, pum={"module": "module_b"})
        upgrader_b = Upgrader(config=cfg_b)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            with self.assertRaises(PumException) as context:
                upgrader_b.upgrade(connection=conn)
            self.assertIn("module_a", str(context.exception))
            self.assertIn("module_b", str(context.exception))
