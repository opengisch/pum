import logging
import os
import tempfile
import unittest
from pathlib import Path

import psycopg

from pum.pum_config import PumConfig
from pum.exceptions import PumDependencyError, PumException, PumHookError
from pum.parameter import ParameterDefinition
from pum.schema_migrations import SchemaMigrations
from pum.upgrader import Upgrader
from pum.sql_content import SqlContent


class TestUpgrader(unittest.TestCase):
    """Test the class Upgrader."""

    def tearDown(self) -> None:
        """Clean up the test environment."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_custom_migrations_schema CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_test_app CASCADE;")
            cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")

        self.tmpdir.cleanup()
        self.tmp = None

    def setUp(self) -> None:
        """Set up the test environment."""
        logging.basicConfig(level=logging.INFO, format="%(message)s")

        self.maxDiff = 5000

        self.pg_service = "pum_test"

        with psycopg.connect(f"service={self.pg_service}") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS pum_test_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_custom_migrations_schema CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS pum_test_app CASCADE;")
            cur.execute("DROP TABLE IF EXISTS public.pum_migrations;")

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = self.tmpdir.name

    def test_install_single_changelog(self) -> None:
        """Test the installation of a single changelog."""
        test_dir = Path("test") / "data" / "single_changelog"
        changelog_file = test_dir / "changelogs" / "1.2.3" / "single_changelog.sql"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))
            self.assertEqual(sm.baseline(conn), "1.2.3")
            self.assertEqual(sm.migration_details(conn), sm.migration_details(conn, "1.2.3"))
            self.assertEqual(sm.migration_details(conn)["version"], "1.2.3")
            self.assertEqual(sm.migration_details(conn)["beta_testing"], False)
            self.assertEqual(
                sm.migration_details(conn)["changelog_files"],
                [str(changelog_file)],
            )

    def test_install_beta_testing(self) -> None:
        """Test the installation as beta testing."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(connection=conn, beta_testing=True)
            self.assertTrue(sm.exists(conn))
            self.assertEqual(sm.baseline(conn), "1.2.3")
            self.assertEqual(sm.migration_details(conn), sm.migration_details(conn, "1.2.3"))
            self.assertEqual(sm.migration_details(conn)["beta_testing"], True)

    @unittest.skipIf(
        os.name == "nt" and os.getenv("CI") == "true",
        "Test not supported on Windows CI (postgis not installed)",
    )
    def test_parameters(self) -> None:
        """Test the installation of parameters."""
        test_dir = Path("test") / "data" / "parameters"
        config_path = test_dir / ".pum.yaml"
        cfg = PumConfig.from_yaml(config_path)
        self.assertEqual(len(cfg.parameters()), 3)
        self.assertEqual(
            cfg.parameter("SRID"),
            ParameterDefinition(
                name="SRID",
                type="integer",
                default=2056,
                description="SRID for the geometry column",
            ),
        )
        self.assertEqual(
            cfg.parameter("default_text_value"),
            ParameterDefinition(
                name="default_text_value",
                type="text",
                default="hi there",
                description="The default text value",
            ),
        )
        self.assertEqual(
            cfg.parameter("default_integer_value"),
            ParameterDefinition(
                name="default_integer_value",
                type="integer",
                default=1874,
                description="The default integer value",
            ),
        )
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(
                connection=conn,
                parameters={
                    "SRID": 2056,
                    "default_text_value": "hello world",
                    "default_integer_value": 1806,
                },
            )
            self.assertTrue(sm.exists(conn))
            self.assertEqual(
                sm.migration_details(conn)["parameters"],
                {
                    "SRID": 2056,
                    "default_text_value": "hello world",
                    "default_integer_value": 1806,
                },
            )
            cur = conn.cursor()
            cur.execute("SELECT Find_SRID('pum_test_data', 'some_table', 'geom');")
            srid = cur.fetchone()[0]
            self.assertEqual(srid, 2056)

    @unittest.skipIf(
        os.name == "nt" and os.getenv("CI") == "true",
        "Test not supported on Windows CI (postgis not installed)",
    )
    def test_parameters_injection(self) -> None:
        """Test the installation of parameters with SQL injection."""
        test_dir = Path("test") / "data" / "parameters"
        config_path = test_dir / ".pum.yaml"
        cfg = PumConfig.from_yaml(config_path)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(
                connection=conn,
                parameters={
                    "SRID": 2056,
                    "default_text_value": "); DROP TABLE pum_test_data.some_table2; CREATE TABLE pum_test_data.some_table3( id INT PRIMARY KEY",
                    "default_integer_value": 1806,
                },
            )
            # Assert that pum_test_data.some_table2 exists (i.e., SQL injection did not drop it)
            cur = conn.cursor()
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pum_test_data' AND table_name = 'some_table2');"
            )
            exists = cur.fetchone()[0]
            self.assertTrue(exists)

    def test_install_custom_directory(self) -> None:
        """Test the installation of a custom directory."""
        test_dir = Path("test") / "data" / "custom_directory"
        config_path = test_dir / ".pum.yaml"
        cfg = PumConfig.from_yaml(config_path)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))

    def test_install_custom_migration_table(self) -> None:
        """Test the installation of a custom migration table."""
        test_dir = Path("test") / "data" / "custom_migration_schema"
        config_path = test_dir / ".pum.yaml"
        cfg = PumConfig.from_yaml(config_path)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))
            cur = SqlContent(
                "SELECT table_schema FROM information_schema.tables WHERE table_name = 'pum_migrations';"
            ).execute(connection=conn)
            self.assertEqual(cur.fetchone()[0], "pum_custom_migrations_schema")

    def test_install_complex_files_content(self) -> None:
        """Test the installation of complex files content."""
        complex_dir = Path("test") / "data" / "complex_files_content"
        cfg = PumConfig(complex_dir)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))

    def test_install_multiple_changelogs(self) -> None:
        """Test the installation of multiple changelogs."""
        test_dir = Path("test") / "data" / "multiple_changelogs"
        changelog_file_1 = test_dir / "changelogs" / "2.0.0" / "create_second_table.sql"
        changelog_file_2 = test_dir / "changelogs" / "2.0.0" / "create_third_table.sql"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))
            self.assertEqual(sm.baseline(conn), "2.0.0")
            self.assertEqual(
                sm.migration_details(conn),
                sm.migration_details(conn, "2.0.0"),
            )
            self.assertEqual(sm.migration_details(conn)["version"], "2.0.0")
            self.assertEqual(
                sm.migration_details(conn)["changelog_files"],
                [str(changelog_file_1), str(changelog_file_2)],
            )

    def test_install_multiple_changelogs_max_version(self) -> None:
        """Test the installation of multiple changelogs with max_version."""
        test_dir = Path("test") / "data" / "multiple_changelogs"
        cfg = PumConfig(test_dir)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, max_version="1.2.4")
            self.assertTrue(sm.exists(conn))
            self.assertEqual(sm.baseline(conn), "1.2.4")

    def test_invalid_changelog_commit(self) -> None:
        """Test the invalid changelog."""
        test_dir = Path("test") / "data" / "invalid_changelog_commit"
        cfg = PumConfig(base_path=test_dir, validate=False)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            with self.assertRaises(Exception) as context:
                upgrader.install(connection=conn)
            self.assertTrue(
                "SQL contains forbidden transaction statement:" in str(context.exception)
            )

    def test_invalid_changelog_search_path(self) -> None:
        """Test the invalid changelog."""
        test_dir = Path("test") / "data" / "invalid_changelog_search_path"
        cfg = PumConfig(base_path=test_dir, validate=False)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            with self.assertRaises(Exception) as context:
                upgrader.install(connection=conn)
            self.assertTrue(
                "SQL contains forbidden transaction statement:" in str(context.exception)
            )

    def test_pre_post_sql_files(self) -> None:
        """Test the pre and post SQL files."""
        test_dir = Path("test") / "data" / "pre_post_sql_files"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, max_version="1.2.3")
            self.assertTrue(sm.exists(conn))
            cursor = conn.cursor()
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.views "
                "WHERE table_schema = 'pum_test_app' AND table_name = 'some_view');"
            )
            exists = cursor.fetchone()[0]
            self.assertTrue(exists)

    def test_pre_post_sql_code(self) -> None:
        """Test the pre and post hooks with SQL code."""
        test_dir = Path("test") / "data" / "pre_post_sql_code"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, max_version="1.2.3")
            self.assertTrue(sm.exists(conn))
            cursor = conn.cursor()
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.views "
                "WHERE table_schema = 'pum_test_app' AND table_name = 'some_view');"
            )
            exists = cursor.fetchone()[0]
            self.assertTrue(exists)

    def test_pre_post_python(self) -> None:
        """Test the pre and post python hooks."""
        test_dir = Path("test") / "data" / "pre_post_python"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))
            cursor = conn.cursor()
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.views "
                "WHERE table_schema = 'pum_test_app' AND table_name = 'some_view');"
            )
            exists = cursor.fetchone()[0]
            self.assertTrue(exists)

    def test_pre_post_python_parameters(self) -> None:
        """Test the pre and post python hooks with parameters."""
        test_dir = Path("test") / "data" / "pre_post_python_parameters"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            with self.assertRaises(PumHookError):
                upgrader = Upgrader(config=cfg)
                upgrader.install(connection=conn, max_version="1.2.3")
            conn.rollback()

        with psycopg.connect(f"service={self.pg_service}") as conn:
            upgrader = Upgrader(
                config=cfg,
            )
            upgrader.install(
                connection=conn, max_version="1.2.3", parameters={"my_comment": "how cool"}
            )
            self.assertTrue(sm.exists(conn))
            cursor = conn.cursor()
            cursor.execute(
                "SELECT obj_description(('pum_test_app.some_view'::regclass)::oid, 'pg_class');"
            )
            comment = cursor.fetchone()[0]
            self.assertEqual(comment, "how cool")

    def test_pre_post_python_local_import(self) -> None:
        """Test the pre and post python hooks with local import."""
        test_dir = Path("test") / "data" / "pre_post_python_local_import"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))
            cursor = conn.cursor()
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.views "
                "WHERE table_schema = 'pum_test_app' AND table_name = 'some_view');"
            )
            exists = cursor.fetchone()[0]
            self.assertTrue(exists)

    def test_demo_data(self) -> None:
        """Test the installation of demo data."""
        test_dir = Path("test") / "data" / "demo_data"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn)
            with self.assertRaises(PumException):
                upgrader.install_demo_data(connection=conn, name="nope, nothing here fella")
            upgrader.install_demo_data(connection=conn, name="some cool demo dataset")
            self.assertTrue(sm.exists(conn))
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM pum_test_data.some_table;")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 4)

    def test_demo_data_multi(self) -> None:
        """Test the installation of demo data."""
        test_dir = Path("test") / "data" / "demo_data_multi"
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml")
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn)
            with self.assertRaises(PumException):
                upgrader.install_demo_data(connection=conn, name="nope, nothing here fella")
            upgrader.install_demo_data(connection=conn, name="some cool demo dataset")
            self.assertTrue(sm.exists(conn))
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM pum_test_data.some_table;")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 4)

    def test_dependencies(self) -> None:
        """Test the installation of dependencies."""
        test_dir = Path("test") / "data" / "dependencies"
        with self.assertRaises(PumDependencyError):
            PumConfig.from_yaml(test_dir / ".pum.yaml")
        cfg = PumConfig.from_yaml(test_dir / ".pum.yaml", install_dependencies=True)
        sm = SchemaMigrations(cfg)
        with psycopg.connect(f"service={self.pg_service}") as conn:
            self.assertFalse(sm.exists(conn))
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn)
            self.assertTrue(sm.exists(conn))


if __name__ == "__main__":
    unittest.main()
