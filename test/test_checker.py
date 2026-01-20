import unittest
import logging
import psycopg
from pathlib import Path

from pum.checker import Checker


class TestChecker(unittest.TestCase):
    """Test the class Checker.

    2 pg_services related to 2 empty db, needed for test:
        pum_test
        pum_test_2
    """

    def tearDown(self):
        # First, terminate any active connections to the test databases
        # This prevents locks from Checker instances
        for service in (self.pg_service1, self.pg_service2):
            try:
                with psycopg.connect(f"service={service}") as conn:
                    conn.autocommit = True
                    cur = conn.cursor()
                    # Terminate other connections to allow clean drop
                    cur.execute("""
                        SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = current_database()
                          AND pid <> pg_backend_pid();
                    """)
                    cur.execute("DROP SCHEMA IF EXISTS pum_test_checker CASCADE;")
                    cur.execute("DROP SCHEMA IF EXISTS test_schema CASCADE;")
                    cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")
            except Exception as e:
                print(f"Warning: tearDown error for {service}: {e}")

    def setUp(self):
        """Set up the test environment."""
        logging.basicConfig(level=logging.INFO, format="%(message)s")

        self.maxDiff = 5000

        self.pg_service1 = "pum_test"
        self.pg_service2 = "pum_test_2"
        self.test_dir = Path("test") / "data" / "checker_test"

        # Clean databases
        for service in (self.pg_service1, self.pg_service2):
            with psycopg.connect(f"service={service}") as conn:
                cur = conn.cursor()
                cur.execute("DROP SCHEMA IF EXISTS pum_test_checker CASCADE;")
                cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")

        # Read and execute the schema SQL on first database
        schema_file = self.test_dir / "changelogs" / "1.0.0" / "schema.sql"
        with open(schema_file) as f:
            schema_sql = f.read()

        with psycopg.connect(f"service={self.pg_service1}") as conn:
            cur = conn.cursor()
            cur.execute(schema_sql)

    def _install_schema_on_db2(self):
        """Helper method to install schema on second database."""
        schema_file = self.test_dir / "changelogs" / "1.0.0" / "schema.sql"
        with open(schema_file) as f:
            schema_sql = f.read()

        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute(schema_sql)

    def test_databases_identical_after_both_installed(self):
        """Test that two databases are identical after both have the schema installed."""
        # Execute schema on second database as well
        self._install_schema_on_db2()

        # Now compare - should be identical (excluding public schema which may have PostGIS)
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        self.assertTrue(
            report.passed,
            f"Databases should be identical after both installed. Failed checks: {[r.name for r in report.check_results if not r.passed]}",
        )
        self.assertEqual(report.failed_checks, 0)
        self.assertEqual(report.total_differences, 0)

    def test_check_tables(self):
        """Test table comparison between databases."""
        # DB1 has tables from installation, DB2 is empty
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        # Should have differences since DB2 doesn't have the schema
        self.assertFalse(report.passed)

        # Find the tables check result
        tables_check = next((r for r in report.check_results if r.key == "tables"), None)
        self.assertIsNotNone(tables_check)
        self.assertFalse(tables_check.passed)
        self.assertGreater(len(tables_check.differences), 0)

        # Execute schema on second database
        self._install_schema_on_db2()

        # Now check again - should be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        tables_check = next((r for r in report.check_results if r.key == "tables"), None)
        self.assertTrue(tables_check.passed)

    def test_check_columns(self):
        """Test column comparison between databases."""
        # Execute schema on second database
        self._install_schema_on_db2()

        # Modify a column type in DB2
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute(
                "ALTER TABLE pum_test_checker.users ALTER COLUMN username TYPE VARCHAR(50);"
            )

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        columns_check = next((r for r in report.check_results if r.key == "columns"), None)
        self.assertIsNotNone(columns_check)
        self.assertFalse(columns_check.passed)

        # Fix the column
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute(
                "ALTER TABLE pum_test_checker.users ALTER COLUMN username TYPE VARCHAR(100);"
            )

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        columns_check = next((r for r in report.check_results if r.key == "columns"), None)
        self.assertTrue(columns_check.passed)

    def test_check_constraints(self):
        """Test constraint comparison between databases."""
        # Execute schema on second database
        self._install_schema_on_db2()

        # Remove a UNIQUE constraint in DB2 (Checker doesn't detect CHECK constraints)
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("ALTER TABLE pum_test_checker.users DROP CONSTRAINT users_username_key;")

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        constraints_check = next((r for r in report.check_results if r.key == "constraints"), None)
        self.assertIsNotNone(constraints_check)
        self.assertFalse(constraints_check.passed)

        # Add the constraint back
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute(
                "ALTER TABLE pum_test_checker.users ADD CONSTRAINT users_username_key UNIQUE (username);"
            )

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        constraints_check = next((r for r in report.check_results if r.key == "constraints"), None)
        self.assertTrue(constraints_check.passed)

    def test_check_views(self):
        """Test view comparison between databases."""
        # Execute schema on second database
        self._install_schema_on_db2()

        # Modify view definition in DB2
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("DROP VIEW pum_test_checker.active_products;")
            cur.execute("""
                CREATE VIEW pum_test_checker.active_products AS
                SELECT id, name FROM pum_test_checker.products WHERE in_stock = TRUE;
            """)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        views_check = next((r for r in report.check_results if r.key == "views"), None)
        self.assertIsNotNone(views_check)
        self.assertFalse(views_check.passed)

        # Fix the view
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("DROP VIEW pum_test_checker.active_products;")
            cur.execute("""
                CREATE VIEW pum_test_checker.active_products AS
                SELECT id, name, price FROM pum_test_checker.products WHERE in_stock = TRUE;
            """)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        views_check = next((r for r in report.check_results if r.key == "views"), None)
        self.assertTrue(views_check.passed)

    def test_check_sequences(self):
        """Test sequence comparison between databases."""
        # Execute schema on second database
        self._install_schema_on_db2()

        # Create an additional sequence in DB2
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("CREATE SEQUENCE pum_test_checker.extra_sequence START 100;")

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        sequences_check = next((r for r in report.check_results if r.key == "sequences"), None)
        self.assertIsNotNone(sequences_check)
        self.assertFalse(sequences_check.passed)

        # Remove the extra sequence
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SEQUENCE pum_test_checker.extra_sequence;")

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        sequences_check = next((r for r in report.check_results if r.key == "sequences"), None)
        self.assertTrue(sequences_check.passed)

    def test_check_indexes(self):
        """Test index comparison between databases."""
        # Execute schema on second database
        self._install_schema_on_db2()

        # Drop an index in DB2
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("DROP INDEX pum_test_checker.idx_products_name;")

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        indexes_check = next((r for r in report.check_results if r.key == "indexes"), None)
        self.assertIsNotNone(indexes_check)
        self.assertFalse(indexes_check.passed)

        # Recreate the index
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("CREATE INDEX idx_products_name ON pum_test_checker.products(name);")

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        indexes_check = next((r for r in report.check_results if r.key == "indexes"), None)
        self.assertTrue(indexes_check.passed)

    def test_check_triggers(self):
        """Test trigger comparison between databases."""
        # Execute schema on second database
        self._install_schema_on_db2()

        # Drop a trigger in DB2
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("DROP TRIGGER users_update_trigger ON pum_test_checker.users;")

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        triggers_check = next((r for r in report.check_results if r.key == "triggers"), None)
        self.assertIsNotNone(triggers_check)
        self.assertFalse(triggers_check.passed)

        # Recreate the trigger
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TRIGGER users_update_trigger
                BEFORE UPDATE ON pum_test_checker.users
                FOR EACH ROW
                EXECUTE FUNCTION pum_test_checker.update_timestamp();
            """)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        triggers_check = next((r for r in report.check_results if r.key == "triggers"), None)
        self.assertTrue(triggers_check.passed)

    def test_check_functions(self):
        """Test function comparison between databases."""
        # Execute schema on second database
        self._install_schema_on_db2()

        # Modify a function in DB2
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE OR REPLACE FUNCTION pum_test_checker.calculate_total(p_price NUMERIC, p_quantity INTEGER)
                RETURNS NUMERIC AS $$
                BEGIN
                    RETURN p_price * p_quantity * 1.1;  -- Changed calculation
                END;
                $$ LANGUAGE plpgsql IMMUTABLE;
            """)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        functions_check = next((r for r in report.check_results if r.key == "functions"), None)
        self.assertIsNotNone(functions_check)
        self.assertFalse(functions_check.passed)

        # Fix the function (use exact same formatting as original)
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            cur = conn.cursor()
            cur.execute("""
CREATE OR REPLACE FUNCTION pum_test_checker.calculate_total(p_price NUMERIC, p_quantity INTEGER)
RETURNS NUMERIC AS $$
BEGIN
    RETURN p_price * p_quantity;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
""")

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()

        functions_check = next((r for r in report.check_results if r.key == "functions"), None)
        self.assertTrue(functions_check.passed)

    def test_exclude_schema(self):
        """Test that excluded schemas are not checked."""
        # Execute schema on second database
        self._install_schema_on_db2()

        # Both databases should be identical (excluding public)
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        self.assertTrue(report.passed)

        # Now create a custom schema to test exclusion (not public since we always exclude it)
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            cur = conn.cursor()
            cur.execute("CREATE SCHEMA IF NOT EXISTS test_schema;")
            cur.execute("CREATE TABLE test_schema.extra_table (id INT PRIMARY KEY);")

        # Without excluding test_schema, should detect difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        self.assertFalse(report.passed)

        # With excluding both public and test_schema, should be identical
        checker = Checker(
            self.pg_service1, self.pg_service2, exclude_schema=["public", "test_schema"]
        )
        report = checker.run_checks()
        self.assertTrue(report.passed)

        # Clean up
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS test_schema CASCADE;")


if __name__ == "__main__":
    unittest.main()
