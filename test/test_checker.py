import unittest
import logging
import psycopg
from pathlib import Path

from pum.checker import Checker
from pum.pum_config import PumConfig
from pum.upgrader import Upgrader


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

        # Install version 1.0.0 on first database using Upgrader
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, max_version="1.0.0")

    def _install_version_on_db2(self, version="1.0.0"):
        """Helper method to install/upgrade schema on second database.

        Args:
            version: Target version to install/upgrade to.
        """
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, max_version=version)

    def test_databases_identical_after_both_installed(self):
        """Test that two databases are identical after both have the schema installed."""
        # Install same version on second database
        self._install_version_on_db2()

        # Now compare - should be identical (excluding public schema which may have PostGIS)
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        self.assertTrue(
            report.passed,
            f"Databases should be identical after both installed. Failed checks: {[r.name for r in report.check_results if not r.passed]}",
        )
        self.assertEqual(report.failed_checks, 0)
        self.assertEqual(report.total_differences, 0)

    def test_check_tables(self):
        """Test table comparison between databases."""
        # DB1 has 1.0.0, upgrade DB1 to 1.1.0 which adds a new table
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # DB2 is empty - should have differences
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        # Should have differences since DB2 doesn't have the schema
        self.assertFalse(report.passed)

        # Find the tables check result
        tables_check = next((r for r in report.check_results if r.key == "tables"), None)
        self.assertIsNotNone(tables_check)
        self.assertFalse(tables_check.passed)
        self.assertGreater(len(tables_check.differences), 0)

        # Install on second database to same version
        self._install_version_on_db2()
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Now check again - should be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        tables_check = next((r for r in report.check_results if r.key == "tables"), None)
        self.assertTrue(tables_check.passed)

    def test_check_columns(self):
        """Test column comparison between databases."""
        # Install same version on both databases first
        self._install_version_on_db2()

        # Upgrade DB1 to 1.1.0 which adds a column to products table
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        columns_check = next((r for r in report.check_results if r.key == "columns"), None)
        self.assertIsNotNone(columns_check)
        self.assertFalse(columns_check.passed)

        # Upgrade DB2 to match
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        columns_check = next((r for r in report.check_results if r.key == "columns"), None)
        self.assertTrue(columns_check.passed)

    def test_check_constraints(self):
        """Test constraint comparison between databases."""
        # Install same version on both databases first
        self._install_version_on_db2()

        # Upgrade DB1 to 1.1.0 which adds foreign key constraint
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        constraints_check = next((r for r in report.check_results if r.key == "constraints"), None)
        self.assertIsNotNone(constraints_check)
        self.assertFalse(constraints_check.passed)

        # Upgrade DB2 to match
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        constraints_check = next((r for r in report.check_results if r.key == "constraints"), None)
        self.assertTrue(constraints_check.passed)

    def test_check_constraint_definition_changes(self):
        """Test that constraint definition changes are detected.

        This test addresses the issue from https://github.com/qwat/qwat-data-model/pull/366
        where a constraint definition was changed from 'year > 1800' to 'year >= 1800'.
        The checker should detect such changes in constraint definitions, not just
        added/removed constraints.
        """
        # Clean up any previous test data
        for service in (self.pg_service1, self.pg_service2):
            with psycopg.connect(f"service={service}") as conn:
                cur = conn.cursor()
                cur.execute("DROP SCHEMA IF EXISTS pum_test_constraint_def CASCADE;")
                cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")

        # Use the constraint definition change test data
        test_dir = Path("test") / "data" / "constraint_definition_change"
        cfg = PumConfig(test_dir, pum={"module": "test_constraint_definition_change"})

        # Install version 1.0.0 on both databases with old constraint definition (year > 1800)
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, max_version="1.0.0")

        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, max_version="1.0.0")

        # At this point, both databases should be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        constraints_check = next((r for r in report.check_results if r.key == "constraints"), None)
        self.assertIsNotNone(constraints_check)
        self.assertTrue(
            constraints_check.passed,
            "Databases should be identical after both installed with 1.0.0",
        )

        # Now upgrade DB1 to 1.1.0 which changes the constraint definition
        # from 'year > 1800' to 'year >= 1800'
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Check should now detect the constraint definition difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        constraints_check = next((r for r in report.check_results if r.key == "constraints"), None)
        self.assertIsNotNone(constraints_check)
        self.assertFalse(
            constraints_check.passed, "Checker should detect constraint definition changes"
        )

        # Verify we have differences reported
        self.assertGreater(
            len(constraints_check.differences),
            0,
            "Should report differences in constraint definitions",
        )

        # Verify the differences mention the constraint
        differences_str = str(constraints_check.differences)
        self.assertIn(
            "pipe_year_check", differences_str, "Should report the pipe_year_check constraint"
        )

        # Now upgrade DB2 to match DB1
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Should now be identical again
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        constraints_check = next((r for r in report.check_results if r.key == "constraints"), None)
        self.assertTrue(
            constraints_check.passed, "Databases should be identical after both upgraded to 1.1.0"
        )

        # Cleanup
        for service in (self.pg_service1, self.pg_service2):
            with psycopg.connect(f"service={service}") as conn:
                cur = conn.cursor()
                cur.execute("DROP SCHEMA IF EXISTS pum_test_constraint_def CASCADE;")
                cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")

    def test_check_views(self):
        """Test view comparison between databases."""
        # Install same version on both databases first
        self._install_version_on_db2()

        # Upgrade DB1 to 1.1.0 which adds a new view
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        views_check = next((r for r in report.check_results if r.key == "views"), None)
        self.assertIsNotNone(views_check)
        self.assertFalse(views_check.passed)

        # Upgrade DB2 to match
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        views_check = next((r for r in report.check_results if r.key == "views"), None)
        self.assertTrue(views_check.passed)

    def test_check_sequences(self):
        """Test sequence comparison between databases."""
        # Install same version on both databases first
        self._install_version_on_db2()

        # Upgrade DB1 to 1.1.0 which adds invoice_sequence
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        sequences_check = next((r for r in report.check_results if r.key == "sequences"), None)
        self.assertIsNotNone(sequences_check)
        self.assertFalse(sequences_check.passed)

        # Upgrade DB2 to match
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        sequences_check = next((r for r in report.check_results if r.key == "sequences"), None)
        self.assertTrue(sequences_check.passed)

    def test_check_indexes(self):
        """Test index comparison between databases."""
        # Install same version on both databases first
        self._install_version_on_db2()

        # Upgrade DB1 to 1.1.0 which adds idx_orders_user_id index
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        indexes_check = next((r for r in report.check_results if r.key == "indexes"), None)
        self.assertIsNotNone(indexes_check)
        self.assertFalse(indexes_check.passed)

        # Upgrade DB2 to match
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        indexes_check = next((r for r in report.check_results if r.key == "indexes"), None)
        self.assertTrue(indexes_check.passed)

    def test_check_triggers(self):
        """Test trigger comparison between databases."""
        # Install same version on both databases first
        self._install_version_on_db2()

        # Upgrade DB1 to 1.1.0 which adds orders_update_trigger
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        triggers_check = next((r for r in report.check_results if r.key == "triggers"), None)
        self.assertIsNotNone(triggers_check)
        self.assertFalse(triggers_check.passed)

        # Upgrade DB2 to match
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        triggers_check = next((r for r in report.check_results if r.key == "triggers"), None)
        self.assertTrue(triggers_check.passed)

    def test_check_functions(self):
        """Test function comparison between databases."""
        # Install same version on both databases first
        self._install_version_on_db2()

        # Upgrade DB1 to 1.1.0 which adds get_order_count() function
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Check should detect the difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        functions_check = next((r for r in report.check_results if r.key == "functions"), None)
        self.assertIsNotNone(functions_check)
        self.assertFalse(functions_check.passed)

        # Upgrade DB2 to match
        with psycopg.connect(f"service={self.pg_service2}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # Should now be identical
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        functions_check = next((r for r in report.check_results if r.key == "functions"), None)
        self.assertTrue(functions_check.passed)

    def test_exclude_schema(self):
        """Test that excluded schemas are not checked."""
        # Install same version on both databases
        self._install_version_on_db2()

        # Both databases should be identical (excluding public)
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()
        self.assertTrue(report.passed)

        # Now create a custom schema to test exclusion (not public since we always exclude it)
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            cur = conn.cursor()
            cur.execute("CREATE SCHEMA IF NOT EXISTS test_schema;")
            cur.execute("CREATE TABLE test_schema.extra_table (id INT PRIMARY KEY);")

        # Without excluding test_schema, should detect difference
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()
        self.assertFalse(report.passed)

        # With excluding both public and test_schema, should be identical
        checker = Checker(
            self.pg_service1, self.pg_service2, exclude_schema=["public", "test_schema"]
        )
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()
        self.assertTrue(report.passed)

        # Clean up
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS test_schema CASCADE;")

    def test_report_generation(self):
        """Test that report generation works for all formats (text, HTML, JSON)."""
        from pum.report_generator import ReportGenerator
        import json

        # DB1 has 1.0.0, upgrade to 1.1.0 which adds differences
        cfg = PumConfig(self.test_dir, pum={"module": "test_checker"})
        with psycopg.connect(f"service={self.pg_service1}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.upgrade(connection=conn)

        # DB2 is empty - will have differences
        checker = Checker(self.pg_service1, self.pg_service2, exclude_schema=["public"])
        report = checker.run_checks()
        checker.conn1.close()
        checker.conn2.close()

        self.assertFalse(report.passed)
        self.assertGreater(report.total_differences, 0)

        # Test text output
        text_output = ReportGenerator.generate_text(report)
        self.assertIsInstance(text_output, str)
        self.assertIn("Tables", text_output)
        self.assertIn("Columns", text_output)
        self.assertIn("Constraints", text_output)

        # Test JSON output
        json_output = ReportGenerator.generate_json(report)
        self.assertIsInstance(json_output, str)

        # Verify JSON is valid and parseable
        json_data = json.loads(json_output)
        self.assertIn("pg_connection1", json_data)
        self.assertIn("pg_connection2", json_data)
        self.assertIn("timestamp", json_data)
        self.assertIn("passed", json_data)
        self.assertIn("check_results", json_data)
        self.assertFalse(json_data["passed"])
        self.assertIsInstance(json_data["check_results"], list)

        # Verify check results have expected structure
        for check in json_data["check_results"]:
            self.assertIn("name", check)
            self.assertIn("passed", check)
            self.assertIn("difference_count", check)
            self.assertIn("differences", check)
            self.assertIsInstance(check["differences"], list)

        # Test HTML output
        html_output = ReportGenerator.generate_html(report)
        self.assertIsInstance(html_output, str)
        self.assertIn("<!DOCTYPE html>", html_output)
        self.assertIn("Database Comparison Report", html_output)
        self.assertIn(self.pg_service1, html_output)
        self.assertIn(self.pg_service2, html_output)

        # Verify collapsible functionality exists
        self.assertIn("toggleCollapsible", html_output)
        self.assertIn("collapsible-toggle", html_output)
        self.assertIn("collapsible-content", html_output)

        # Verify structured formatting helpers exist
        self.assertIn("schema-object", html_output)
        self.assertIn("object-detail", html_output)


if __name__ == "__main__":
    unittest.main()
