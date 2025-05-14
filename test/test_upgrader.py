import logging
import tempfile
import unittest
import os
from pathlib import Path

import psycopg

from pum.config import PumConfig
from pum.schema_migrations import SchemaMigrations
from pum.upgrader import Upgrader
from pum.migration_parameter import MigrationParameterDefinition
from pum.exceptions import PumHookError


class TestUpgrader(unittest.TestCase):
    """
    Test the class Upgrader.
    """

    def tearDown(self):
        self.cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
        self.cur.execute("DROP SCHEMA IF EXISTS pum_custom_migrations_schema CASCADE;")
        self.cur.execute("DROP SCHEMA IF EXISTS pum_test_app CASCADE;")
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
        self.cur.execute("DROP SCHEMA IF EXISTS pum_test_app CASCADE;")
        self.cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")
        self.conn.commit()

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = self.tmpdir.name

    def test_install_single_changelog(self):
        test_dir = Path("test") / "data" / "single_changelog"
        changelog_file = test_dir / "changelogs" / "1.2.3" / "create_northwind.sql"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))
        self.assertEqual(sm.baseline(self.conn), "1.2.3")
        self.assertEqual(sm.migration_details(self.conn), sm.migration_details(self.conn, "1.2.3"))
        self.assertEqual(sm.migration_details(self.conn)["version"], "1.2.3")
        self.assertEqual(
            sm.migration_details(self.conn)["changelog_files"],
            [str(changelog_file)],
        )

    @unittest.skipIf(
        os.name == "nt" and os.getenv("CI") == "true",
        "Test not supported on Windows CI (postgis not installed)",
    )
    def test_parameters(self):
        test_dir = Path("test") / "data" / "parameters"
        config_path = test_dir / ".pum.yaml"
        cfg = PumConfig.from_yaml(str(config_path))
        self.assertEqual(
            cfg.parameters()["SRID"],
            MigrationParameterDefinition(
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
            parameters={"SRID": 2056},
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))
        self.assertEqual(sm.migration_details(self.conn)["parameters"], {"SRID": 2056})
        self.cur.execute("SELECT Find_SRID('pum_test_data', 'some_table', 'geom');")
        srid = self.cur.fetchone()[0]
        self.assertEqual(srid, 2056)

    def test_install_custom_directory(self):
        test_dir = Path("test") / "data" / "custom_directory"
        config_path = test_dir / ".pum.yaml"
        cfg = PumConfig.from_yaml(str(config_path))
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))

    def test_install_custom_migration_table(self):
        test_dir = Path("test") / "data" / "custom_migration_schema"
        config_path = test_dir / ".pum.yaml"
        cfg = PumConfig.from_yaml(str(config_path))
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))

    def test_install_complex_files_content(self):
        complex_dir = Path("test") / "data" / "complex_files_content"
        cfg = PumConfig(complex_dir)
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
        )
        upgrader.install()
        self.assertTrue(sm.exists(self.conn))

    def test_install_multiple_changelogs(self):
        test_dir = Path("test") / "data" / "multiple_changelogs"
        changelog_file_1 = test_dir / "changelogs" / "2.0.0" / "create_second_table.sql"
        changelog_file_2 = test_dir / "changelogs" / "2.0.0" / "create_third_table.sql"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(
            pg_service=self.pg_service,
            config=cfg,
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
            [str(changelog_file_1), str(changelog_file_2)],
        )

    def test_install_multiple_changelogs_max_version(self):
        test_dir = Path("test") / "data" / "multiple_changelogs"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(pg_service=self.pg_service, config=cfg)
        upgrader.install(max_version="1.2.4")
        self.assertTrue(sm.exists(self.conn))
        self.assertEqual(sm.baseline(self.conn), "1.2.4")

    def test_invalid_changelog(self):
        test_dir = Path("test") / "data" / "invalid_changelog"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(pg_service=self.pg_service, config=cfg)
        with self.assertRaises(Exception) as context:
            upgrader.install()
        self.assertTrue(
            "SQL contains forbidden transaction statement: BEGIN;" in str(context.exception)
        )

    def test_pre_post_sql_files(self):
        test_dir = Path("test") / "data" / "pre_post_sql_files"
        cfg = PumConfig.from_yaml(str(test_dir / ".pum.yaml"))
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(pg_service=self.pg_service, config=cfg)
        upgrader.install(max_version="1.2.3")
        self.assertTrue(sm.exists(self.conn))
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema = 'pum_test_app' AND table_name = 'some_view');"
        )
        exists = cursor.fetchone()[0]
        self.assertTrue(exists)

        def test_pre_post_sql_code(self):
            test_dir = Path("test") / "data" / "pre_post_sql_code"
            cfg = PumConfig.from_yaml(str(test_dir / ".pum.yaml"))
            sm = SchemaMigrations(cfg)
            self.assertFalse(sm.exists(self.conn))
            upgrader = Upgrader(pg_service=self.pg_service, config=cfg)
            upgrader.install(max_version="1.2.3")
            self.assertTrue(sm.exists(self.conn))
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema = 'pum_test_app' AND table_name = 'some_view');"
            )
            exists = cursor.fetchone()[0]
            self.assertTrue(exists)

    def test_pre_post_python(self):
        test_dir = Path("test") / "data" / "pre_post_python"
        cfg = PumConfig.from_yaml(str(test_dir / ".pum.yaml"))
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        upgrader = Upgrader(pg_service=self.pg_service, config=cfg)
        upgrader.install(max_version="1.2.3")
        self.assertTrue(sm.exists(self.conn))
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema = 'pum_test_app' AND table_name = 'some_view');"
        )
        exists = cursor.fetchone()[0]
        self.assertTrue(exists)

    def test_pre_post_python_parameters(self):
        test_dir = Path("test") / "data" / "pre_post_python_parameters"
        cfg = PumConfig.from_yaml(str(test_dir / ".pum.yaml"))
        sm = SchemaMigrations(cfg)
        self.assertFalse(sm.exists(self.conn))
        with self.assertRaises(PumHookError):
            upgrader = Upgrader(pg_service=self.pg_service, config=cfg)
            upgrader.install(max_version="1.2.3")
        upgrader = Upgrader(
            pg_service=self.pg_service, config=cfg, parameters={"my_comment": "how cool"}
        )
        upgrader.install(max_version="1.2.3")
        self.assertTrue(sm.exists(self.conn))
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT obj_description(('pum_test_app.some_view'::regclass)::oid, 'pg_class');"
        )
        comment = cursor.fetchone()[0]
        self.assertEqual(comment, "how cool")


if __name__ == "__main__":
    unittest.main()
