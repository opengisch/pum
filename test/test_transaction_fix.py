import unittest
from pathlib import Path

import psycopg

from pum.pum_config import PumConfig
from pum.schema_migrations import SchemaMigrations


class TestTransactionFix(unittest.TestCase):
    """Test that verifies the transaction fix prevents 'idle in transaction' state."""

    def setUp(self) -> None:
        """Set up the test environment."""
        self.pg_service = "pum_test"

    def test_schema_migrations_exists_transaction_state(self) -> None:
        """Test that schema_migrations.exists() doesn't leave connection in 'idle in transaction'."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir, pum={"module": "test_single_changelog"})
        sm = SchemaMigrations(cfg)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Clean up any existing table
            with conn.transaction():
                conn.execute("DROP TABLE IF EXISTS public.pum_migrations")

            # Initial state should be IDLE
            self.assertEqual(conn.info.transaction_status.name, "IDLE")

            # Call exists() which executes a query
            sm.exists(conn)

            # After the call, connection should still be IDLE (not "IDLE_IN_TRANSACTION")
            # This will FAIL if the transaction block is not used in exists()
            transaction_status = conn.info.transaction_status.name
            self.assertEqual(
                transaction_status,
                "IDLE",
                f"Connection in '{transaction_status}' state after exists() - should use transaction block",
            )

    def test_schema_migrations_baseline_transaction_state(self) -> None:
        """Test that schema_migrations.baseline() doesn't leave connection in 'idle in transaction'."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir, pum={"module": "test_single_changelog"})
        sm = SchemaMigrations(cfg)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Create the migrations table with some data
            with conn.transaction():
                sm.create(conn)
                sm.set_baseline(conn, "1.0.0", commit=False)

            # Initial state should be IDLE
            self.assertEqual(conn.info.transaction_status.name, "IDLE")

            # Call baseline() which executes a query
            sm.baseline(conn)

            # After the call, connection should still be IDLE
            transaction_status = conn.info.transaction_status.name
            self.assertEqual(
                transaction_status,
                "IDLE",
                f"Connection in '{transaction_status}' state after baseline() - should use transaction block",
            )

            # Clean up
            with conn.transaction():
                conn.execute("DROP TABLE IF EXISTS public.pum_migrations")

    def test_multiple_queries_without_transaction_causes_idle_in_transaction(self) -> None:
        """Test that demonstrates the problem: queries without transaction blocks cause 'idle in transaction'."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Start with IDLE state
            self.assertEqual(conn.info.transaction_status.name, "IDLE")

            # Execute a query WITHOUT using a transaction block
            # This simulates the old buggy behavior
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()

            # Now the connection is stuck in INTRANS (idle in transaction)
            transaction_status = conn.info.transaction_status.name
            self.assertEqual(
                transaction_status,
                "INTRANS",
                "Without transaction blocks, connection gets stuck in INTRANS (idle in transaction)",
            )

            # Need to explicitly commit to get back to IDLE
            conn.commit()
            self.assertEqual(conn.info.transaction_status.name, "IDLE")


if __name__ == "__main__":
    unittest.main()
