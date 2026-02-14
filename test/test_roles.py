import logging
import tempfile
import unittest
from pathlib import Path

import psycopg

from pum.pum_config import PumConfig
from pum.role_manager import RoleManager
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
            cur.execute("DROP ROLE IF EXISTS pum_test_user_lausanne;")
            cur.execute("DROP ROLE IF EXISTS pum_test_viewer_lausanne;")
            cur.execute("DROP ROLE IF EXISTS pum_test_intruder;")
            cur.execute("DROP ROLE IF EXISTS pum_test_target_user;")
            cur.execute("DROP ROLE IF EXISTS pum_test_login_user;")

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
            cur.execute("DROP ROLE IF EXISTS pum_test_user_lausanne;")
            cur.execute("DROP ROLE IF EXISTS pum_test_viewer_lausanne;")
            cur.execute("DROP ROLE IF EXISTS pum_test_intruder;")
            cur.execute("DROP ROLE IF EXISTS pum_test_target_user;")
            cur.execute("DROP ROLE IF EXISTS pum_test_login_user;")

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

    def test_create_specific_roles(self) -> None:
        """Test creating DB-specific roles with a suffix."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            rm.create_roles(
                connection=conn,
                suffix="lausanne",
                grant=True,
                commit=True,
            )

        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()

            # Verify specific roles were created
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer_lausanne', 'pum_test_user_lausanne');"
            )
            roles = cur.fetchall()
            self.assertEqual(len(roles), 2)
            self.assertIn(("pum_test_viewer_lausanne",), roles)
            self.assertIn(("pum_test_user_lausanne",), roles)

            # Verify generic roles were also created
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer', 'pum_test_user');"
            )
            roles = cur.fetchall()
            self.assertEqual(len(roles), 2)

            # Verify generic role inherits from specific role
            cur.execute(
                "SELECT pg_has_role('pum_test_viewer', 'pum_test_viewer_lausanne', 'MEMBER');"
            )
            self.assertTrue(cur.fetchone()[0], "Generic viewer should be member of specific viewer")

            cur.execute("SELECT pg_has_role('pum_test_user', 'pum_test_user_lausanne', 'MEMBER');")
            self.assertTrue(cur.fetchone()[0], "Generic user should be member of specific user")

    def test_create_specific_roles_with_permissions(self) -> None:
        """Test that permissions are granted to specific roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            rm.create_roles(
                connection=conn,
                suffix="lausanne",
                grant=True,
                commit=True,
            )

        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()

            # Specific viewer should have READ on schema_1
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer_lausanne', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0], "Specific viewer should have SELECT")

            # Specific user should have WRITE on schema_2
            cur.execute(
                "SELECT has_table_privilege('pum_test_user_lausanne', "
                "'pum_test_data_schema_2.some_table_2', 'INSERT');"
            )
            self.assertTrue(cur.fetchone()[0], "Specific user should have INSERT")

            # Generic viewer should inherit permissions from specific viewer
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(
                cur.fetchone()[0],
                "Generic viewer should have SELECT via inheritance from specific",
            )

    def test_drop_roles(self) -> None:
        """Test dropping generic roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            # Roles should exist
            cur = conn.cursor()
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer', 'pum_test_user') ORDER BY rolname;"
            )
            self.assertEqual(len(cur.fetchall()), 2)

            rm.drop_roles(connection=conn, commit=True)

            # Roles should be gone
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer', 'pum_test_user');"
            )
            self.assertEqual(len(cur.fetchall()), 0)

    def test_drop_roles_with_suffix(self) -> None:
        """Test dropping only suffixed roles while keeping generic ones."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            rm.create_roles(
                connection=conn,
                suffix="lausanne",
                grant=True,
                commit=True,
            )

            # All 4 roles should exist
            cur = conn.cursor()
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer', 'pum_test_user', "
                "'pum_test_viewer_lausanne', 'pum_test_user_lausanne');"
            )
            self.assertEqual(len(cur.fetchall()), 4)

            rm.drop_roles(connection=conn, suffix="lausanne", commit=True)

            # Suffixed roles should be gone
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer_lausanne', 'pum_test_user_lausanne');"
            )
            self.assertEqual(len(cur.fetchall()), 0)

            # Generic roles should still exist
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer', 'pum_test_user');"
            )
            self.assertEqual(len(cur.fetchall()), 2)

    def test_drop_single_role(self) -> None:
        """Test dropping a single role by name while keeping the other."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            cur = conn.cursor()
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer', 'pum_test_user') ORDER BY rolname;"
            )
            self.assertEqual(len(cur.fetchall()), 2)

            # Drop only pum_test_user
            rm.drop_roles(connection=conn, roles=["pum_test_user"], commit=True)

            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'pum_test_user';")
            self.assertIsNone(cur.fetchone(), "pum_test_user should be dropped")

            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'pum_test_viewer';")
            self.assertIsNotNone(cur.fetchone(), "pum_test_viewer should still exist")

    def test_drop_single_role_with_suffix(self) -> None:
        """Test dropping a single suffixed role while keeping the other."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            rm.create_roles(connection=conn, suffix="lausanne", grant=True, commit=True)

            cur = conn.cursor()
            # All 4 roles should exist
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer', 'pum_test_user', "
                "'pum_test_viewer_lausanne', 'pum_test_user_lausanne');"
            )
            self.assertEqual(len(cur.fetchall()), 4)

            # Drop only the suffixed viewer
            rm.drop_roles(
                connection=conn, roles=["pum_test_viewer"], suffix="lausanne", commit=True
            )

            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'pum_test_viewer_lausanne';")
            self.assertIsNone(cur.fetchone(), "pum_test_viewer_lausanne should be dropped")

            # The other 3 roles should still exist
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN "
                "('pum_test_viewer', 'pum_test_user', 'pum_test_user_lausanne');"
            )
            self.assertEqual(len(cur.fetchall()), 3)

    def test_revoke_single_role(self) -> None:
        """Test revoking permissions from a single role by name."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            cur = conn.cursor()

            # Both roles should have their permissions
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0])

            cur.execute(
                "SELECT has_table_privilege('pum_test_user', "
                "'pum_test_data_schema_2.some_table_2', 'INSERT');"
            )
            self.assertTrue(cur.fetchone()[0])

            # Revoke only pum_test_user permissions
            rm.revoke_permissions(connection=conn, roles=["pum_test_user"], commit=True)

            # pum_test_user should lose write access
            cur.execute(
                "SELECT has_table_privilege('pum_test_user', "
                "'pum_test_data_schema_2.some_table_2', 'INSERT');"
            )
            self.assertFalse(cur.fetchone()[0], "pum_test_user should lose INSERT after revoke")

            # pum_test_viewer should still have read access (untouched)
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0], "pum_test_viewer should still have SELECT")

    def test_revoke_removes_memberships(self) -> None:
        """Test that revoke_permissions also revokes role memberships."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            rm.create_roles(
                connection=conn,
                suffix="lausanne",
                grant=True,
                commit=True,
            )

            cur = conn.cursor()

            # Generic viewer should be a member of the specific viewer (inheritance)
            cur.execute(
                "SELECT pg_has_role('pum_test_viewer', 'pum_test_viewer_lausanne', 'MEMBER');"
            )
            self.assertTrue(cur.fetchone()[0], "Generic viewer should be member of specific viewer")

            # Generic viewer should have inherited SELECT via the membership
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0], "Generic viewer should have inherited SELECT")

            # Revoke permissions (including memberships) from the generic roles
            rm.revoke_permissions(connection=conn, commit=True)

            # Membership should be revoked
            cur.execute(
                "SELECT pg_has_role('pum_test_viewer', 'pum_test_viewer_lausanne', 'MEMBER');"
            )
            self.assertFalse(
                cur.fetchone()[0], "Membership should be revoked after revoke_permissions"
            )

            # Generic viewer should no longer have SELECT (inherited privilege gone)
            cur.execute(
                "SELECT has_table_privilege('pum_test_viewer', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertFalse(
                cur.fetchone()[0], "Generic viewer should lose inherited SELECT after revoke"
            )

    def test_grant_to(self) -> None:
        """Test granting configured roles to a target database user."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            cur = conn.cursor()
            # Create a target user
            cur.execute("CREATE ROLE pum_test_target_user LOGIN;")
            conn.commit()

            # Target user should NOT have SELECT yet
            cur.execute(
                "SELECT has_table_privilege('pum_test_target_user', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertFalse(cur.fetchone()[0])

            # Grant only the viewer role to the target user
            rm.grant_to(
                connection=conn,
                to="pum_test_target_user",
                roles=["pum_test_viewer"],
                commit=True,
            )

            # Target user should now have SELECT via membership
            cur.execute(
                "SELECT has_table_privilege('pum_test_target_user', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0], "Target user should inherit SELECT from viewer")

            # Target user should NOT have INSERT (not granted user role)
            cur.execute(
                "SELECT has_table_privilege('pum_test_target_user', "
                "'pum_test_data_schema_2.some_table_2', 'INSERT');"
            )
            self.assertFalse(
                cur.fetchone()[0], "Target user should NOT have INSERT (only viewer granted)"
            )

    def test_grant_to_with_suffix(self) -> None:
        """Test granting a suffixed role to a target database user."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            rm.create_roles(connection=conn, suffix="lausanne", grant=True, commit=True)

            cur = conn.cursor()
            cur.execute("CREATE ROLE pum_test_target_user LOGIN;")
            conn.commit()

            # Grant suffixed viewer to target
            rm.grant_to(
                connection=conn,
                to="pum_test_target_user",
                roles=["pum_test_viewer"],
                suffix="lausanne",
                commit=True,
            )

            # Target should be a member of the suffixed viewer
            cur.execute(
                "SELECT pg_has_role('pum_test_target_user', 'pum_test_viewer_lausanne', 'MEMBER');"
            )
            self.assertTrue(
                cur.fetchone()[0],
                "Target should be member of pum_test_viewer_lausanne",
            )

            # Target should have SELECT via the suffixed viewer
            cur.execute(
                "SELECT has_table_privilege('pum_test_target_user', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(
                cur.fetchone()[0],
                "Target should inherit SELECT from suffixed viewer",
            )

    def test_revoke_from(self) -> None:
        """Test revoking configured roles from a target database user."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            cur = conn.cursor()
            cur.execute("CREATE ROLE pum_test_target_user LOGIN;")
            conn.commit()

            # Grant viewer to target
            rm.grant_to(
                connection=conn,
                to="pum_test_target_user",
                roles=["pum_test_viewer"],
                commit=True,
            )

            # Confirm target has SELECT
            cur.execute(
                "SELECT has_table_privilege('pum_test_target_user', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertTrue(cur.fetchone()[0])

            # Revoke viewer from target
            rm.revoke_from(
                connection=conn,
                from_role="pum_test_target_user",
                roles=["pum_test_viewer"],
                commit=True,
            )

            # Target should no longer have SELECT
            cur.execute(
                "SELECT has_table_privilege('pum_test_target_user', "
                "'pum_test_data_schema_1.some_table_1', 'SELECT');"
            )
            self.assertFalse(
                cur.fetchone()[0],
                "Target should lose SELECT after revoke_from",
            )

    def test_roles_inventory_all_ok(self) -> None:
        """Test roles_inventory when roles are created with correct permissions."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)
            result = rm.roles_inventory(connection=conn)

        self.assertEqual(len(result.configured_roles), 2)
        self.assertEqual(result.missing_roles, [])
        for role_status in result.configured_roles:
            self.assertFalse(role_status.is_unknown)
            # all permissions should match
            self.assertTrue(all(sp.satisfied for sp in role_status.schema_permissions))

        # pum_test_user inherits from pum_test_viewer, so it should be a member
        user_status = next(r for r in result.roles if r.name == "pum_test_user")
        self.assertIn("pum_test_viewer", user_status.granted_to)
        # pum_test_viewer has no inheritance
        viewer_status = next(r for r in result.roles if r.name == "pum_test_viewer")
        self.assertEqual(viewer_status.granted_to, [])

    def test_roles_inventory_login_attribute(self) -> None:
        """Test that roles_inventory reports the LOGIN attribute correctly."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            # Create a LOGIN role that will appear as unknown
            cur = conn.cursor()
            cur.execute("CREATE ROLE pum_test_intruder LOGIN;")
            cur.execute("GRANT USAGE ON SCHEMA pum_test_data_schema_1 TO pum_test_intruder;")
            conn.commit()

            result = rm.roles_inventory(connection=conn)

        # Configured roles are created with NOLOGIN by default
        viewer = next(r for r in result.roles if r.name == "pum_test_viewer")
        self.assertFalse(viewer.login, "pum_test_viewer should not have LOGIN")

        user = next(r for r in result.roles if r.name == "pum_test_user")
        self.assertFalse(user.login, "pum_test_user should not have LOGIN")

        # The intruder was created with LOGIN
        intruder = next(r for r in result.unknown_roles if r.name == "pum_test_intruder")
        self.assertTrue(intruder.login, "pum_test_intruder should have LOGIN")

    def test_roles_inventory_missing(self) -> None:
        """Test roles_inventory when roles have not been created."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            # Don't create roles
            result = rm.roles_inventory(connection=conn)

        self.assertEqual(len(result.configured_roles), 0)
        self.assertEqual(len(result.missing_roles), 2)
        self.assertIn("pum_test_viewer", result.missing_roles)
        self.assertIn("pum_test_user", result.missing_roles)

    def test_roles_inventory_no_permissions(self) -> None:
        """Test roles_inventory when roles exist but have no permissions."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            # Create roles without granting permissions
            rm.create_roles(connection=conn, grant=False)
            result = rm.roles_inventory(connection=conn)

        self.assertEqual(result.missing_roles, [])
        for role_status in result.configured_roles:
            self.assertFalse(role_status.is_unknown)
            # At least one schema permission should not match
            self.assertFalse(
                all(sp.satisfied for sp in role_status.schema_permissions),
                f"Role {role_status.name} should not have matching permissions",
            )

    def test_roles_inventory_with_suffix(self) -> None:
        """Test roles_inventory automatically discovers suffixed roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            rm.create_roles(
                connection=conn,
                suffix="lausanne",
                grant=True,
                commit=True,
            )
            result = rm.roles_inventory(connection=conn)

        # Should find both generic and suffixed roles (4 total)
        self.assertEqual(len(result.configured_roles), 4)
        self.assertEqual(result.missing_roles, [])
        names = {r.name for r in result.configured_roles}
        self.assertIn("pum_test_viewer", names)
        self.assertIn("pum_test_user", names)
        self.assertIn("pum_test_viewer_lausanne", names)
        self.assertIn("pum_test_user_lausanne", names)

        # Generic viewer should be member of the specific viewer role
        by_name = {r.name: r for r in result.configured_roles}
        self.assertIn("pum_test_viewer_lausanne", by_name["pum_test_viewer"].granted_to)
        # Generic user should be member of the specific user role
        self.assertIn("pum_test_user_lausanne", by_name["pum_test_user"].granted_to)
        # Generic user also inherits from generic viewer (config inheritance)
        self.assertIn("pum_test_viewer", by_name["pum_test_user"].granted_to)

    def test_roles_inventory_unknown_roles(self) -> None:
        """Test that roles_inventory reports other roles with schema access."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            # Create an extra role not in the config and grant it access
            cur = conn.cursor()
            cur.execute("CREATE ROLE pum_test_intruder NOSUPERUSER;")
            cur.execute("GRANT USAGE ON SCHEMA pum_test_data_schema_1 TO pum_test_intruder;")
            conn.commit()

            result = rm.roles_inventory(connection=conn)

        # The configured roles' permissions should all match
        for r in result.configured_roles:
            self.assertTrue(
                all(sp.satisfied for sp in r.schema_permissions),
                f"Role {r.name} permissions should match",
            )
        unknown_names = {ur.name for ur in result.unknown_roles}
        self.assertIn("pum_test_intruder", unknown_names)
        # Superusers should not be listed by default
        for ur in result.unknown_roles:
            self.assertFalse(ur.superuser)

    def test_roles_inventory_include_superusers(self) -> None:
        """Test that include_superusers=True lists superusers as other roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            result_without = rm.roles_inventory(connection=conn)
            result_with = rm.roles_inventory(connection=conn, include_superusers=True)

        # With superusers included, there should be more (or equal) unknown roles
        self.assertGreaterEqual(len(result_with.unknown_roles), len(result_without.unknown_roles))

        # At least one superuser should appear (the current connection user)
        superuser_roles = [ur for ur in result_with.unknown_roles if ur.superuser]
        self.assertTrue(
            len(superuser_roles) > 0, "Expected at least one superuser in unknown roles"
        )

        # Each superuser unknown role must have the superuser flag set
        for ur in superuser_roles:
            self.assertTrue(ur.superuser)

    def test_roles_inventory_other_login_roles(self) -> None:
        """Test that other_login_roles lists login roles with no schema access."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            # Create a LOGIN role that has NO access to any configured schema
            cur = conn.cursor()
            cur.execute("CREATE ROLE pum_test_intruder LOGIN NOSUPERUSER;")
            conn.commit()

            result = rm.roles_inventory(connection=conn)

        # The login role should appear in other_login_roles
        self.assertIn("pum_test_intruder", result.other_login_roles)
        # It should NOT appear in unknown_roles (no schema access)
        unknown_names = {ur.name for ur in result.unknown_roles}
        self.assertNotIn("pum_test_intruder", unknown_names)

    def test_roles_inventory_other_login_roles_excluded_when_has_access(self) -> None:
        """Login roles with schema access should NOT appear in other_login_roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            # Create a LOGIN role WITH schema access
            cur = conn.cursor()
            cur.execute("CREATE ROLE pum_test_intruder LOGIN NOSUPERUSER;")
            cur.execute("GRANT USAGE ON SCHEMA pum_test_data_schema_1 TO pum_test_intruder;")
            conn.commit()

            result = rm.roles_inventory(connection=conn)

        # Should be in unknown_roles (has schema access), NOT in other_login_roles
        unknown_names = {ur.name for ur in result.unknown_roles}
        self.assertIn("pum_test_intruder", unknown_names)
        self.assertNotIn("pum_test_intruder", result.other_login_roles)

    def test_roles_inventory_other_login_roles_excludes_superusers(self) -> None:
        """Superuser login roles should NOT appear in other_login_roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)
            result = rm.roles_inventory(connection=conn)

            # Verify none of the other_login_roles are superusers
            for name in result.other_login_roles:
                cur = conn.cursor()
                cur.execute("SELECT rolsuper FROM pg_roles WHERE rolname = %s", (name,))
                row = cur.fetchone()
                self.assertFalse(row[0], f"{name} is a superuser but appeared in other_login_roles")

    def test_roles_inventory_grantee_roles(self) -> None:
        """Test that grantee_roles lists roles that are members of configured roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            # Create a login user and grant a configured role to it
            cur = conn.cursor()
            cur.execute("CREATE ROLE pum_test_target_user LOGIN NOSUPERUSER;")
            cur.execute("GRANT pum_test_viewer TO pum_test_target_user;")
            conn.commit()

            result = rm.roles_inventory(connection=conn)

        grantee_names = {gr.name for gr in result.grantee_roles}
        self.assertIn("pum_test_target_user", grantee_names)
        # The grantee should NOT appear in unknown_roles
        unknown_names = {ur.name for ur in result.unknown_roles}
        self.assertNotIn("pum_test_target_user", unknown_names)
        # The grantee should NOT appear in other_login_roles
        self.assertNotIn("pum_test_target_user", result.other_login_roles)
        # Check granted_to contains the configured role
        target = next(gr for gr in result.grantee_roles if gr.name == "pum_test_target_user")
        self.assertIn("pum_test_viewer", target.granted_to)

    def test_roles_inventory_intruder_not_grantee(self) -> None:
        """An unknown role with direct schema access (not via grant_to) is not a grantee."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            # Create a role with direct schema access, not a member of any configured role
            cur = conn.cursor()
            cur.execute("CREATE ROLE pum_test_intruder NOSUPERUSER;")
            cur.execute("GRANT USAGE ON SCHEMA pum_test_data_schema_1 TO pum_test_intruder;")
            conn.commit()

            result = rm.roles_inventory(connection=conn)

        # Should be in unknown_roles, NOT in grantee_roles
        unknown_names = {ur.name for ur in result.unknown_roles}
        grantee_names = {gr.name for gr in result.grantee_roles}
        self.assertIn("pum_test_intruder", unknown_names)
        self.assertNotIn("pum_test_intruder", grantee_names)

    def test_create_login_role(self) -> None:
        """Test create_login_role creates a role with LOGIN attribute."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            RoleManager.create_login_role(connection=conn, name="pum_test_login_user", commit=True)

            cur = conn.cursor()
            cur.execute(
                "SELECT rolcanlogin, rolsuper FROM pg_roles WHERE rolname = %s",
                ("pum_test_login_user",),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row, "Role pum_test_login_user should exist")
            self.assertTrue(row[0], "Role should have LOGIN")
            self.assertFalse(row[1], "Role should not be a superuser")

    def test_create_login_role_already_exists(self) -> None:
        """Test create_login_role raises when role already exists."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            RoleManager.create_login_role(connection=conn, name="pum_test_login_user", commit=True)
            with self.assertRaises(Exception):
                RoleManager.create_login_role(
                    connection=conn, name="pum_test_login_user", commit=True
                )

    def test_login_roles(self) -> None:
        """Test login_roles returns non-superuser login roles."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Create a login role
            RoleManager.create_login_role(connection=conn, name="pum_test_login_user", commit=True)

            result = RoleManager.login_roles(connection=conn)

        self.assertIn("pum_test_login_user", result)
        # Should be sorted
        self.assertEqual(result, sorted(result))
        # Should not contain superusers or pg_* roles
        for name in result:
            self.assertFalse(name.startswith("pg_"), f"{name} should be excluded")

    def test_login_roles_excludes_nologin(self) -> None:
        """Test login_roles excludes roles without LOGIN."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Create configured roles (NOLOGIN by default)
            rm.create_roles(connection=conn, grant=False, commit=True)

            result = RoleManager.login_roles(connection=conn)

        # Configured roles are NOLOGIN, should not appear
        self.assertNotIn("pum_test_viewer", result)
        self.assertNotIn("pum_test_user", result)

    def test_members_of(self) -> None:
        """Test members_of returns login members of a given role."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            rm.create_roles(connection=conn, grant=False, commit=True)

            # Create a login user and grant a configured role to it
            RoleManager.create_login_role(connection=conn, name="pum_test_login_user", commit=True)
            rm.grant_to(connection=conn, to="pum_test_login_user", commit=True)

            result = RoleManager.members_of(connection=conn, role_name="pum_test_viewer")

        # pum_test_login_user was granted all roles, so it should be a member of viewer
        self.assertIn("pum_test_login_user", result)
        # Should be sorted
        self.assertEqual(result, sorted(result))

    def test_members_of_empty(self) -> None:
        """Test members_of returns empty list when no members."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            rm.create_roles(connection=conn, grant=False, commit=True)

            result = RoleManager.members_of(connection=conn, role_name="pum_test_viewer")

        self.assertEqual(result, [])

    def test_members_of_excludes_nologin(self) -> None:
        """Test members_of only returns login roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            # pum_test_user inherits pum_test_viewer (NOLOGIN member)
            result = RoleManager.members_of(connection=conn, role_name="pum_test_viewer")

        # pum_test_user is NOLOGIN, should not appear
        self.assertNotIn("pum_test_user", result)


if __name__ == "__main__":
    unittest.main()
