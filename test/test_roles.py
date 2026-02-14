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
            cur.execute("DROP ROLE IF EXISTS pum_test_user_lausanne;")
            cur.execute("DROP ROLE IF EXISTS pum_test_viewer_lausanne;")
            cur.execute("DROP ROLE IF EXISTS pum_test_intruder;")

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

    def test_check_roles_all_ok(self) -> None:
        """Test check_roles when roles are created with correct permissions."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)
            result = rm.check_roles(connection=conn)

        self.assertTrue(result.complete, f"Expected result.complete, got roles: {result.roles}")
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

    def test_check_roles_missing(self) -> None:
        """Test check_roles when roles have not been created."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            # Don't create roles
            result = rm.check_roles(connection=conn)

        self.assertFalse(result.complete)
        self.assertEqual(len(result.configured_roles), 0)
        self.assertEqual(len(result.missing_roles), 2)
        self.assertIn("pum_test_viewer", result.missing_roles)
        self.assertIn("pum_test_user", result.missing_roles)

    def test_check_roles_no_permissions(self) -> None:
        """Test check_roles when roles exist but have no permissions."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn)
            # Create roles without granting permissions
            rm.create_roles(connection=conn, grant=False)
            result = rm.check_roles(connection=conn)

        self.assertFalse(result.complete, "Should not be complete without permissions")
        self.assertEqual(result.missing_roles, [])
        for role_status in result.configured_roles:
            self.assertFalse(role_status.is_unknown)
            # At least one schema permission should not match
            self.assertFalse(
                all(sp.satisfied for sp in role_status.schema_permissions),
                f"Role {role_status.name} should not have matching permissions",
            )

    def test_check_roles_with_suffix(self) -> None:
        """Test check_roles automatically discovers suffixed roles."""
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
            result = rm.check_roles(connection=conn)

        self.assertTrue(result.complete)
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

    def test_check_roles_unknown_roles(self) -> None:
        """Test that check_roles reports unknown roles with schema access."""
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

            result = rm.check_roles(connection=conn)

        # The configured roles' permissions should all match
        for r in result.configured_roles:
            self.assertTrue(
                all(sp.satisfied for sp in r.schema_permissions),
                f"Role {r.name} permissions should match",
            )
        # result.complete should still be True — unknown roles don't affect completeness
        self.assertTrue(result.complete)
        unknown_names = {ur.name for ur in result.unknown_roles}
        self.assertIn("pum_test_intruder", unknown_names)
        # Superusers should not be listed by default
        for ur in result.unknown_roles:
            self.assertFalse(ur.superuser)

    def test_check_roles_include_superusers(self) -> None:
        """Test that include_superusers=True lists superusers as unknown roles."""
        test_dir = Path("test") / "data" / "roles"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        rm = cfg.role_manager()
        with psycopg.connect(f"service={self.pg_service}") as conn:
            Upgrader(cfg).install(connection=conn, roles=True, grant=True)

            result_without = rm.check_roles(connection=conn)
            result_with = rm.check_roles(connection=conn, include_superusers=True)

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


if __name__ == "__main__":
    unittest.main()
