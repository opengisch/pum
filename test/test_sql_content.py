import unittest

import psycopg

from pum.sql_content import SqlContent


class TestSqlContent(unittest.TestCase):
    """Test the SqlContent class and CursorResult."""

    def setUp(self) -> None:
        """Set up the test environment."""
        self.pg_service = "pum_test"

    def test_cursor_result_fetchall(self) -> None:
        """Test CursorResult.fetchall() returns all results."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            result = SqlContent("SELECT 1 AS a, 2 AS b UNION SELECT 3, 4").execute(conn)

            # Test fetchall returns all rows
            rows = result.fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0], (1, 2))
            self.assertEqual(rows[1], (3, 4))

    def test_cursor_result_fetchone(self) -> None:
        """Test CursorResult.fetchone() returns rows one at a time."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            result = SqlContent("SELECT 1 AS a, 2 AS b UNION SELECT 3, 4 ORDER BY a").execute(conn)

            # Test fetchone returns rows sequentially
            row1 = result.fetchone()
            self.assertEqual(row1, (1, 2))

            row2 = result.fetchone()
            self.assertEqual(row2, (3, 4))

            # Test fetchone returns None when no more rows
            row3 = result.fetchone()
            self.assertIsNone(row3)

    def test_cursor_result_fetchmany(self) -> None:
        """Test CursorResult.fetchmany() returns specified number of rows."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            result = SqlContent("SELECT generate_series(1, 5) AS n").execute(conn)

            # Test fetchmany with size=2
            rows1 = result.fetchmany(2)
            self.assertEqual(len(rows1), 2)
            self.assertEqual(rows1[0], (1,))
            self.assertEqual(rows1[1], (2,))

            # Test fetchmany with size=2 again
            rows2 = result.fetchmany(2)
            self.assertEqual(len(rows2), 2)
            self.assertEqual(rows2[0], (3,))
            self.assertEqual(rows2[1], (4,))

            # Test fetchmany returns remaining rows when fewer than size
            rows3 = result.fetchmany(2)
            self.assertEqual(len(rows3), 1)
            self.assertEqual(rows3[0], (5,))

            # Test fetchmany returns empty list when no more rows
            rows4 = result.fetchmany(2)
            self.assertEqual(rows4, [])

    def test_cursor_result_description(self) -> None:
        """Test CursorResult.description returns column information."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            result = SqlContent("SELECT 1 AS a, 'hello' AS b").execute(conn)

            # Test description is accessible
            self.assertIsNotNone(result.description)
            self.assertEqual(len(result.description), 2)
            self.assertEqual(result.description[0][0], "a")
            self.assertEqual(result.description[1][0], "b")

    def test_cursor_result_rowcount(self) -> None:
        """Test CursorResult.rowcount returns number of rows."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            result = SqlContent("SELECT 1 UNION SELECT 2 UNION SELECT 3").execute(conn)

            # Test rowcount is accessible
            self.assertEqual(result.rowcount, 3)

    def test_cursor_result_no_results(self) -> None:
        """Test CursorResult handles DDL statements with no results."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Create temporary table
            result = SqlContent("CREATE TEMP TABLE test_temp (id INT)").execute(conn)

            # Test fetchall returns empty list for DDL
            rows = result.fetchall()
            self.assertEqual(rows, [])

            # Test fetchone returns None for DDL
            row = result.fetchone()
            self.assertIsNone(row)

    def test_cursor_result_internal_attributes(self) -> None:
        """Test CursorResult internal attributes for backward compatibility."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            result = SqlContent("SELECT 1 AS a, 2 AS b").execute(conn)

            # Test internal attributes used by existing code
            self.assertIsNotNone(result._pum_results)
            self.assertEqual(len(result._pum_results), 1)
            self.assertEqual(result._pum_results[0], (1, 2))

            self.assertIsNotNone(result._pum_description)
            self.assertEqual(result._pum_description[0][0], "a")

    def test_transaction_prevents_idle_in_transaction(self) -> None:
        """Test that using transaction blocks prevents 'idle in transaction' state."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Execute a query within a transaction block
            with conn.transaction():
                result = SqlContent("SELECT 1 AS test_value").execute(conn)
                value = result.fetchone()[0]
                self.assertEqual(value, 1)

            # After the transaction block exits, check the connection state
            # The connection should be idle (not "idle in transaction")
            info = conn.info
            self.assertEqual(info.transaction_status.name, "IDLE")

            # Execute another query in a new transaction
            with conn.transaction():
                result = SqlContent("SELECT 2 AS test_value").execute(conn)
                value = result.fetchone()[0]
                self.assertEqual(value, 2)

            # Again, verify connection is idle (not "idle in transaction")
            self.assertEqual(conn.info.transaction_status.name, "IDLE")

    def test_transaction_rollback_on_error(self) -> None:
        """Test that transactions properly rollback on errors."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Create a temp table
            with conn.transaction():
                SqlContent("CREATE TEMP TABLE test_rollback (id INT PRIMARY KEY)").execute(conn)
                SqlContent("INSERT INTO test_rollback VALUES (1)").execute(conn)

            # Try to insert duplicate key in a transaction (should fail and rollback)
            try:
                with conn.transaction():
                    SqlContent("INSERT INTO test_rollback VALUES (2)").execute(conn)
                    # This should fail due to duplicate key
                    SqlContent("INSERT INTO test_rollback VALUES (1)").execute(conn)
            except psycopg.errors.UniqueViolation:
                pass  # Expected error

            # Verify connection is idle after failed transaction
            self.assertEqual(conn.info.transaction_status.name, "IDLE")

            # Verify only the first insert succeeded (second transaction rolled back)
            with conn.transaction():
                result = SqlContent("SELECT COUNT(*) FROM test_rollback").execute(conn)
                count = result.fetchone()[0]
                # Should only have 1 row (the initial insert), not 2
                self.assertEqual(count, 1)

    def test_nested_savepoints_with_transaction(self) -> None:
        """Test that transaction blocks work with savepoints."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            with conn.transaction():
                SqlContent("CREATE TEMP TABLE test_savepoint (id INT)").execute(conn)
                SqlContent("INSERT INTO test_savepoint VALUES (1)").execute(conn)

                # Create a savepoint
                try:
                    with conn.transaction():
                        SqlContent("INSERT INTO test_savepoint VALUES (2)").execute(conn)
                        # Force an error to test rollback to savepoint
                        raise ValueError("Intentional error")
                except ValueError:
                    pass  # Savepoint should rollback

                # Insert after savepoint rollback
                SqlContent("INSERT INTO test_savepoint VALUES (3)").execute(conn)

            # Verify the results: should have 1 and 3, but not 2
            with conn.transaction():
                result = SqlContent("SELECT id FROM test_savepoint ORDER BY id").execute(conn)
                rows = result.fetchall()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0][0], 1)
                self.assertEqual(rows[1][0], 3)


if __name__ == "__main__":
    unittest.main()
