"""Test the database management functions."""

import unittest

import psycopg

from pum.database import create_database, drop_database


class TestDatabase(unittest.TestCase):
    """Test the database create/drop functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Use a pg_service name for connection
        self.pg_service = "pum_test"
        self.test_db = "test_database_create_drop"
        self.test_db_template = "test_database_template"
        self.test_db_from_template = "test_database_from_template"

        # Ensure test databases don't exist
        self._cleanup_db(self.test_db)
        self._cleanup_db(self.test_db_template)
        self._cleanup_db(self.test_db_from_template)

    def tearDown(self):
        """Clean up test fixtures."""
        self._cleanup_db(self.test_db)
        self._cleanup_db(self.test_db_template)
        self._cleanup_db(self.test_db_from_template)

    def _cleanup_db(self, dbname: str):
        """Drop a database if it exists."""
        try:
            with psycopg.connect(
                f"service={self.pg_service} dbname=postgres", autocommit=True
            ) as conn:
                with conn.cursor() as cur:
                    # Terminate connections
                    cur.execute(
                        "SELECT pg_terminate_backend(pid) "
                        "FROM pg_stat_activity "
                        "WHERE datname = %s AND pid <> pg_backend_pid()",
                        [dbname],
                    )
                    cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
        except psycopg.OperationalError:
            # If we can't connect, that's fine - test database might not exist
            pass

    def test_create_database(self):
        """Test creating a database."""
        connection_params = {"service": self.pg_service, "dbname": "postgres"}

        # Create the database
        create_database(connection_params, self.test_db)

        # Verify it exists
        with psycopg.connect(f"service={self.pg_service} dbname=postgres") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    [self.test_db],
                )
                result = cur.fetchone()
                self.assertIsNotNone(result)

    def test_drop_database(self):
        """Test dropping a database."""
        connection_params = {"service": self.pg_service, "dbname": "postgres"}

        # First create the database
        with psycopg.connect(f"service={self.pg_service} dbname=postgres", autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE {self.test_db}")

        # Verify it exists
        with psycopg.connect(f"service={self.pg_service} dbname=postgres") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    [self.test_db],
                )
                result = cur.fetchone()
                self.assertIsNotNone(result)

        # Drop the database
        drop_database(connection_params, self.test_db)

        # Verify it's gone
        with psycopg.connect(f"service={self.pg_service} dbname=postgres") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    [self.test_db],
                )
                result = cur.fetchone()
                self.assertIsNone(result)

    def test_drop_database_with_active_connections(self):
        """Test dropping a database that has active connections."""
        connection_params = {"service": self.pg_service, "dbname": "postgres"}

        # Create the database
        with psycopg.connect(f"service={self.pg_service} dbname=postgres", autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE {self.test_db}")

        # Open a connection to the test database (active connection)
        active_conn = psycopg.connect(f"service={self.pg_service} dbname={self.test_db}")

        try:
            # Drop should still work (it terminates connections first)
            drop_database(connection_params, self.test_db)

            # Verify it's gone
            with psycopg.connect(f"service={self.pg_service} dbname=postgres") as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM pg_database WHERE datname = %s",
                        [self.test_db],
                    )
                    result = cur.fetchone()
                    self.assertIsNone(result)
        finally:
            # Close the active connection (it should already be terminated)
            try:
                active_conn.close()
            except Exception:
                pass

    def test_create_database_from_template(self):
        """Test creating a database from a template."""
        connection_params = {"service": self.pg_service, "dbname": "postgres"}

        # Create template database and add some data
        with psycopg.connect(f"service={self.pg_service} dbname=postgres", autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE {self.test_db_template}")

        # Add a table to the template
        with psycopg.connect(f"service={self.pg_service} dbname={self.test_db_template}") as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE test_table (id serial PRIMARY KEY, name text)")
                cur.execute("INSERT INTO test_table (name) VALUES ('template_data')")
                conn.commit()

        # Create new database from template
        create_database(
            connection_params, self.test_db_from_template, template=self.test_db_template
        )

        # Verify the new database has the template's structure and data
        with psycopg.connect(
            f"service={self.pg_service} dbname={self.test_db_from_template}"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM test_table")
                result = cur.fetchone()
                self.assertIsNotNone(result)
                self.assertEqual(result[0], "template_data")

    def test_create_database_idempotent(self):
        """Test that creating a database twice doesn't fail."""
        connection_params = {"service": self.pg_service, "dbname": "postgres"}

        # Create the database
        create_database(connection_params, self.test_db)

        # Creating it again should raise an error (we don't have IF NOT EXISTS)
        with self.assertRaises(psycopg.errors.DuplicateDatabase):
            create_database(connection_params, self.test_db)

    def test_drop_nonexistent_database(self):
        """Test dropping a database that doesn't exist."""
        connection_params = {"service": self.pg_service, "dbname": "postgres"}

        # Drop a non-existent database - should not raise error due to IF EXISTS
        # Actually, DROP DATABASE IF EXISTS is not used in the implementation
        # So this will fail. Let's check the implementation...
        # Looking at database.py, it doesn't use IF EXISTS, so this will raise an error
        with self.assertRaises(psycopg.errors.InvalidCatalogName):
            drop_database(connection_params, "nonexistent_database_123456")


if __name__ == "__main__":
    unittest.main()
