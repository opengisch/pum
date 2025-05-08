import logging
import tempfile
import unittest
import os
from pathlib import Path

import psycopg

from pum.config import PumConfig
from pum.schema_migrations import SchemaMigrations
from pum.upgrader import Upgrader
from pum.migration_parameter_definition import MigrationParameterDefintion


class TestUpgrader(unittest.TestCase):
    """
    Test the class Upgrader.
    """

    def tearDown(self):
        self.cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
        self.cur.execute("DROP SCHEMA IF EXISTS pum_custom_migrations_schema CASCADE;")
        self.cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")
        self.conn.commit()
        self.conn.close()

        self.tmpdir.cleanup()
        self.tmp = None

    def setUp(self):
        logging.basicConfig(level=logging.INFO, format="%(message)s")

        self.maxDiff = 5000

        self.pg_service = "pum_test"
        self.conn = psycopg.connect(f"service={self.pg_service}")
        self.cur = self.conn.cursor()
        self.cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
        self.cur.execute("DROP SCHEMA IF EXISTS pum_custom_migrations_schema CASCADE;")
        self.cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")
        self.conn.commit()

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = self.tmpdir.name

    def test_install_single_changelog(self):
        cfg = PumConfig()
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir=str(Path("test") / "data" / "single_changelog"),
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))
        self.assertEqual(sm.baseline(self.conn), "1.2.3")
        self.assertEqual(sm.migration_details(self.conn), sm.migration_details(self.conn, "1.2.3"))
        self.assertEqual(sm.migration_details(self.conn)["version"], "1.2.3")
        self.assertEqual(
            sm.migration_details(self.conn)["changelog_files"],
            [
                str(
                    Path("test")
                    / "data"
                    / "single_changelog"
                    / "changelogs"
                    / "1.2.3"
                    / "create_northwind.sql"
                )
            ],
        )

    @unittest.skipIf(
        os.name == "nt" and os.getenv("CI") == "true",
        "Test not supported on Windows CI (postgis not installed)",
    )
    def test_parameters(self):
        cfg = PumConfig.from_yaml(str(Path("test") / "data" / "parameters" / ".pum-config.yaml"))
        self.assertEqual(
            cfg.parameters()["SRID"],
            MigrationParameterDefintion(
                name="SRID",
                type_="integer",
                default=2056,
                description="SRID for the geometry column",
            ),
        )
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir=str(Path("test") / "data" / "parameters"),
            parameters={"SRID": 2056},
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))
        self.assertEqual(sm.migration_details(self.conn)["parameters"], {"SRID": 2056})
        self.cur.execute("SELECT Find_SRID('pum_test_data', 'some_table', 'geom');")
        srid = self.cur.fetchone()[0]
        self.assertEqual(srid, 2056)

    def test_install_custom_directory(self):
        cfg = PumConfig.from_yaml(
            str(Path("test") / "data" / "custom_directory" / ".pum-config.yaml")
        )
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir=str(Path("test") / "data" / "custom_directory"),
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))

    def test_install_custom_migration_table(self):
        cfg = PumConfig.from_yaml(
            str(Path("test") / "data" / "custom_migration_schema" / ".pum-config.yaml")
        )
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir=str(Path("test") / "data" / "custom_migration_schema"),
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))

    def test_install_complex_files_content(self):
        cfg = PumConfig()
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir=str(Path("test") / "data" / "complex_files_content"),
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))

    def test_install_multiple_changelogs(self):
        cfg = PumConfig()
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir=str(Path("test") / "data" / "multiple_changelogs"),
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))
        self.assertEqual(sm.baseline(self.conn), "2.0.0")
        self.assertEqual(
            sm.migration_details(self.conn),
            sm.migration_details(self.conn, "2.0.0"),
        )
        self.assertEqual(sm.migration_details(self.conn)["version"], "2.0.0")
        self.assertEqual(
            sm.migration_details(self.conn)["changelog_files"],
            [
                str(
                    Path("test")
                    / "data"
                    / "multiple_changelogs"
                    / "changelogs"
                    / "2.0.0"
                    / "create_second_table.sql"
                ),
                str(
                    Path("test")
                    / "data"
                    / "multiple_changelogs"
                    / "changelogs"
                    / "2.0.0"
                    / "create_third_table.sql"
                ),
            ],
        )

    def test_invalid_changelog(self):
        cfg = PumConfig()
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
            dir=str(Path("test") / "data" / "invalid_changelog"),
        )
        with self.assertRaises(Exception) as context:
            upgrader.install()
        self.assertTrue(
            "SQL contains forbidden transaction statement: BEGIN;" in str(context.exception)
        )


if __name__ == "__main__":
    unittest.main()
