import logging
import tempfile
import unittest
from pathlib import Path

import psycopg

from pum.pum_config import PumConfig
from pum.upgrader import Upgrader


class TestRoles(unittest.TestCase):
    """Test the class Upgrader."""

    def tearDown(self) -> None:
        """Clean up the test environment."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS pum_test_data_schema_1 CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_test_data_schema_2 CASCADE;")
            cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")
            cur.execute("DROP ROLE IF EXISTS pum_test_user;")
            cur.execute("DROP ROLE IF EXISTS pum_test_viewer;")

        self.tmpdir.cleanup()
        self.tmp = None

    def setUp(self) -> None:
        """Set up the test environment."""
        logging.basicConfig(level=logging.INFO, format="%(message)s")

        self.maxDiff = 5000

        self.pg_service = "pum_test"

        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS pum_test_data_schema_1 CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_test_data_schema_2 CASCADE;")
            cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")
            cur.execute("DROP ROLE IF EXISTS pum_test_user;")
            cur.execute("DROP ROLE IF EXISTS pum_test_viewer;")

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = self.tmpdir.name

    def test_create(self) -> None:
        """Test the installation of a single changelog."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            rm.create_roles(connection=conn)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN ('pum_test_viewer', 'pum_test_user');"
            )
            roles = cur.fetchall()
            self.assertEqual(len(roles), 2)
            self.assertIn(("pum_test_viewer",), roles)
            self.assertIn(("pum_test_user",), roles)

    def test_grant_permissions(self) -> None:
        """Test granting permissions to roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            rm.create_roles(connection=conn, grant=True)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()

            # viewer
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', 'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0])
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', 'pum_test_data_schema_2.some_table_2', 'SELECT');"
            )
            self.assertFalse(cur.fetchone()[0])

            # user
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0])
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_1.some_table_1', 'INSERT');"
            )
            self.assertFalse(cur.fetchone()[0])
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_2.some_table_2', 'INSERT');"
            )
            self.assertTrue(cur.fetchone()[0])

    def test_multiple_installs_roles(self) -> None:
        """Test granting permissions to roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True, commit=True)
            cur = conn.cursor()
            cur.execute(
                "DROP TABLE public.pum_migrations CASCADE; "
                "DROP SCHEMA pum_test_data_schema_1 CASCADE; "
                "DROP SCHEMA pum_test_data_schema_2 CASCADE;"
            )
            Upgrader(cfg).install(connection=conn, roles=True, grant=True, commit=True)


if __name__ == "__main__":
    unittest.main()
