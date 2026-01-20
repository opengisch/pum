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

            # viewer - READ permissions on schema_1
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', 'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0])

            # viewer - no permissions on schema_2
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', 'pum_test_data_schema_2.some_table_2', 'SELECT');"
            )
            self.assertFalse(cur.fetchone()[0])

            # user - READ permissions on schema_1
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0])

            # user - no INSERT on schema_1 (READ only)
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_1.some_table_1', 'INSERT');"
            )
            self.assertFalse(cur.fetchone()[0])

            # user - WRITE permissions on schema_2
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_2.some_table_2', 'INSERT');"
            )
            self.assertTrue(cur.fetchone()[0])

    def test_grant_permissions_comprehensive(self) -> None:
        """Test that all object types get correct permissions."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)
            cur = conn.cursor()

            # Test viewer (READ) permissions on schema_1

            # Tables
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', 'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0], "Viewer should have SELECT on table")

            # Views
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', 'pum_test_data_schema_1.some_view_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0], "Viewer should have SELECT on view")

            # Sequences
            cur.execute(
                "SELECT has_sequence_privilege('pum_test_viewer', 'pum_test_data_schema_1.some_sequence_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0], "Viewer should have SELECT on sequence")

            cur.execute(
                "SELECT has_sequence_privilege('pum_test_viewer', 'pum_test_data_schema_1.some_sequence_1', 'UPDATE');"
            )
            self.assertFalse(cur.fetchone()[0], "Viewer should NOT have UPDATE on sequence")

            # Functions
            cur.execute(
                "SELECT has_function_privilege('pum_test_viewer', 'pum_test_data_schema_1.some_function_1()', 'EXECUTE');"
            )
            self.assertTrue(cur.fetchone()[0], "Viewer should have EXECUTE on function")

            # Test user (WRITE) permissions on schema_2

            # Tables
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_2.some_table_2', 'INSERT');"
            )
            self.assertTrue(cur.fetchone()[0], "User should have INSERT on table")

            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_2.some_table_2', 'UPDATE');"
            )
            self.assertTrue(cur.fetchone()[0], "User should have UPDATE on table")

            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_2.some_table_2', 'DELETE');"
            )
            self.assertTrue(cur.fetchone()[0], "User should have DELETE on table")

            # Sequences
            cur.execute(
                "SELECT has_sequence_privilege('pum_test_user', 'pum_test_data_schema_2.some_sequence_2', 'UPDATE');"
            )
            self.assertTrue(cur.fetchone()[0], "User should have UPDATE on sequence")

            # Functions
            cur.execute(
                "SELECT has_function_privilege('pum_test_user', 'pum_test_data_schema_2.some_function_2()', 'EXECUTE');"
            )
            self.assertTrue(cur.fetchone()[0], "User should have EXECUTE on function")

            # Test that user inherits viewer permissions (via READ on schema_1)
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', 'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0], "User should have SELECT on schema_1 table")

    def test_grant_permissions_on_types(self) -> None:
        """Test that permissions are granted on custom types (enums and composite types)."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            cur = conn.cursor()

            # Test enum type in schema 1
            cur.execute(
                "SELECT has_type_privilege('pum_test_viewer', 'pum_test_data_schema_1.status_enum', 'USAGE');"
            )
            self.assertTrue(cur.fetchone()[0], "Viewer should have USAGE on status_enum")

            # Test composite type in schema 1
            cur.execute(
                "SELECT has_type_privilege('pum_test_viewer', 'pum_test_data_schema_1.address_type', 'USAGE');"
            )
            self.assertTrue(cur.fetchone()[0], "Viewer should have USAGE on address_type")

            # Test enum type in schema 2
            cur.execute(
                "SELECT has_type_privilege('pum_test_viewer', 'pum_test_data_schema_2.priority_enum', 'USAGE');"
            )
            self.assertTrue(cur.fetchone()[0], "Viewer should have USAGE on priority_enum")

            # Test composite type in schema 2
            cur.execute(
                "SELECT has_type_privilege('pum_test_viewer', 'pum_test_data_schema_2.contact_type', 'USAGE');"
            )
            self.assertTrue(cur.fetchone()[0], "Viewer should have USAGE on contact_type")

            # Test that user (write role) has ALL privileges
            cur.execute(
                "SELECT has_type_privilege('pum_test_user', 'pum_test_data_schema_1.status_enum', 'USAGE');"
            )
            self.assertTrue(cur.fetchone()[0], "User should have USAGE on status_enum")

    def test_default_privileges(self) -> None:
        """Test that default privileges work for newly created objects."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)
            cur = conn.cursor()

            # Create a new table in schema_1 after granting permissions
            cur.execute("""
                CREATE TABLE pum_test_data_schema_1.new_table (
                    id INT PRIMARY KEY,
                    data TEXT
                );
            """)

            # Create a new sequence
            cur.execute("CREATE SEQUENCE pum_test_data_schema_1.new_sequence START 1;")

            # Create a new function
            cur.execute("""
                CREATE OR REPLACE FUNCTION pum_test_data_schema_1.new_function()
                RETURNS INT AS $$
                BEGIN
                    RETURN 99;
                END;
                $$ LANGUAGE plpgsql;
            """)

            # Create a new type (enum)
            cur.execute("""
                CREATE TYPE pum_test_data_schema_1.new_status_enum AS ENUM ('draft', 'published');
            """)

            conn.commit()

            # Verify viewer has SELECT on the new table (due to default privileges)
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', 'pum_test_data_schema_1.new_table', 'SELECT');"
            )
            self.assertTrue(
                cur.fetchone()[0],
                "Viewer should have SELECT on newly created table via default privileges",
            )

            # Verify viewer has USAGE on the new type (due to default privileges)
            cur.execute(
                "SELECT has_type_privilege('pum_test_viewer', 'pum_test_data_schema_1.new_status_enum', 'USAGE');"
            )
            self.assertTrue(
                cur.fetchone()[0],
                "Viewer should have USAGE on newly created type via default privileges",
            )

            # Verify viewer has SELECT on the new sequence
            cur.execute(
                "SELECT has_sequence_privilege('pum_test_viewer', 'pum_test_data_schema_1.new_sequence', 'SELECT');"
            )
            self.assertTrue(
                cur.fetchone()[0],
                "Viewer should have SELECT on newly created sequence via default privileges",
            )

            # Verify viewer can execute the new function
            cur.execute(
                "SELECT has_function_privilege('pum_test_viewer', 'pum_test_data_schema_1.new_function()', 'EXECUTE');"
            )
            self.assertTrue(
                cur.fetchone()[0],
                "Viewer should have EXECUTE on newly created function via default privileges",
            )

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
