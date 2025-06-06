import os
import tempfile
import unittest

import psycopg

from pum.dumper import DumpFormat, Dumper
from pum.exceptions import PgRestoreFailed


class TestDumper(unittest.TestCase):
    def setUp(self):
        # Use a pg_service name for connection
        self.pg_service = "pum_test"
        self.test_db = "test_dumper_db"
        self.dump_file = tempfile.NamedTemporaryFile(delete=False)
        self.dump_file.close()
        # Create test database
        with psycopg.connect(f"service={self.pg_service} dbname=postgres", autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {self.test_db}")
                cur.execute(f"CREATE DATABASE {self.test_db}")
                # cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {self.test_db} TO CURRENT_USER")

    def tearDown(self):
        # Remove test database and dump file
        with psycopg.connect(f"service={self.pg_service} dbname=postgres", autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {self.test_db}")
        if os.path.exists(self.dump_file.name):
            os.unlink(self.dump_file.name)

    def test_dumper_dump_and_restore(self):
        # Connect to test database and create a table
        with psycopg.connect(
            f"service={self.pg_service} dbname={self.test_db}", autocommit=True
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE test_table (id serial PRIMARY KEY, name text)")
                cur.execute("INSERT INTO test_table (name) VALUES ('foo'), ('bar')")

        # Dump the database
        dumper = Dumper(
            pg_service=self.pg_service,
            dump_path=self.dump_file.name,
        )
        dumper.pg_dump(dbname=self.test_db, format=DumpFormat.CUSTOM)

        # Drop the table to test restore
        with psycopg.connect(f"service={self.pg_service} dbname={self.test_db}") as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE test_table")

        # Restore the database
        dumper.pg_restore(dbname=self.test_db)

        # Check that the table and data are restored
        with psycopg.connect(f"service={self.pg_service} dbname={self.test_db}") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM test_table ORDER BY id")
                rows = cur.fetchall()
                self.assertEqual(rows, [("foo",), ("bar",)])

    def test_restore_fails_with_invalid_dump(self):
        # Write invalid SQL to dump file
        with open(self.dump_file.name, "w") as f:
            f.write("INVALID SQL;")
        dumper = Dumper(
            pg_service=self.pg_service,
            dump_path=self.dump_file.name,
        )
        with self.assertRaises(PgRestoreFailed):
            dumper.pg_restore()


if __name__ == "__main__":
    unittest.main()
