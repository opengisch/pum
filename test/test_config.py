import unittest
from pathlib import Path

from packaging.version import parse as parse_version

from pum.pum_config import PumConfig
from pum.exceptions import PumConfigError
from pum.hook import HookHandler
import os
import platform


class TestConfig(unittest.TestCase):
    """Test the class PumConfig."""

    def test_version(self) -> None:
        """Test version."""
        cfg = PumConfig(
            base_path=Path("test") / "data" / "single_changelog",
            pum={"module": "test_single_changelog"},
        )
        changelogs = cfg.changelogs()
        self.assertEqual(len(changelogs), 1)
        self.assertEqual(changelogs[0].version, parse_version("1.2.3"))

        cfg = PumConfig(
            base_path=Path("test") / "data" / "multiple_changelogs",
            pum={"module": "test_multiple_changelogs"},
        )
        changelogs = cfg.changelogs()
        self.assertEqual(len(changelogs), 4)
        self.assertEqual(changelogs[0].version, parse_version("1.2.3"))
        self.assertEqual(changelogs[1].version, parse_version("1.2.4"))
        self.assertEqual(changelogs[2].version, parse_version("1.3.0"))
        self.assertEqual(changelogs[3].version, parse_version("2.0.0"))

        last_version_result = cfg.last_version()
        self.assertEqual(last_version_result, parse_version("2.0.0"))

        last_version_result = cfg.last_version(
            min_version="1.2.4",
            max_version="1.3.0",
        )
        self.assertEqual(last_version_result, parse_version("1.3.0"))

        last_version_result = cfg.last_version(
            max_version="1.3.0",
        )
        self.assertEqual(last_version_result, parse_version("1.3.0"))

        last_version_result = cfg.last_version(
            max_version="1.0.0",
        )
        self.assertIsNone(last_version_result)

        last_version_result = cfg.last_version(
            min_version="1.2.3",
        )
        self.assertEqual(last_version_result, parse_version("2.0.0"))

        last_version_result = cfg.last_version(
            min_version="2.1.0",
        )
        self.assertIsNone(last_version_result)

    def test_hooks(self) -> None:
        """Test hooks."""
        cfg = PumConfig.from_yaml(Path("test") / "data" / "pre_post_sql_files" / ".pum.yaml")

        self.assertEqual(
            cfg.create_app_handlers()[0],
            HookHandler(
                base_path=Path(".").absolute(),
                file="test/data/pre_post_sql_files/create_app/create_view.sql",
            ),
        )
        self.assertEqual(
            cfg.drop_app_handlers()[0],
            HookHandler(
                base_path=Path(".").absolute(),
                file="test/data/pre_post_sql_files/drop_app/drop_view.sql",
            ),
        )

    def test_invalid_hooks_parameters(self) -> None:
        """Test invalid hooks parameters."""
        with self.assertRaises(PumConfigError):
            PumConfig.from_yaml(
                Path("test") / "data" / "pre_post_python_parameters_broken" / ".pum.yaml"
            )
        PumConfig.from_yaml(
            Path("test") / "data" / "pre_post_python_parameters_broken" / ".pum.yaml",
            validate=False,
        )

    def test_invalid_changelog(self) -> None:
        """Test invalid changelog."""
        with self.assertRaises(PumConfigError):
            PumConfig(
                base_path=Path("test") / "data" / "invalid_changelog_commit",
                validate=True,
                pum={"module": "test_invalid_changelog_commit"},
            )
        PumConfig(
            base_path=Path("test") / "data" / "invalid_changelog_commit",
            validate=False,
            pum={"module": "test_invalid_changelog_commit"},
        )

    def test_invalid_changelog_parameters(self) -> None:
        """Test invalid changelog parameters."""
        PumConfig.from_yaml(Path("test") / "data" / "parameters" / ".pum.yaml", validate=True)
        with self.assertRaises(PumConfigError):
            PumConfig(
                base_path=Path("test") / "data" / "parameters",
                validate=True,
                pum={"module": "test_parameters"},
            )

    def test_minimum_version(self) -> None:
        if os.environ.get("CI") and platform.system().lower().startswith("win"):
            self.skipTest("Skipped on Windows in CI")

        with self.assertRaises(PumConfigError):
            PumConfig(
                base_path=Path("test") / "data" / "single_changelog",
                pum={"minimum_version": "9.9.9", "module": "test_single_changelog"},
                validate=True,
            )
        PumConfig(
            base_path=Path("test") / "data" / "single_changelog",
            pum={"minimum_version": "0.1", "module": "test_single_changelog"},
        )

    def test_roles(self) -> None:
        """Test roles."""
        cfg = PumConfig(
            base_path=Path("test") / "data" / "single_changelog",
            pum={"module": "test_single_changelog"},
            roles=[
                {
                    "name": "viewer",
                    "permissions": [
                        {
                            "type": "read",
                            "schemas": ["public", "pum_test_app"],
                        }
                    ],
                    "description": "Viewer role with read permissions.",
                },
                {
                    "name": "user",
                    "permissions": [
                        {
                            "type": "write",
                            "schemas": ["public", "pum_test_app"],
                        }
                    ],
                    "description": "User role with read and write permissions.",
                },
            ],
        )
        self.assertIsNotNone(cfg.config.roles)

    def test_invalid_config(self) -> None:
        """Test invalid configuration."""
        base_path = Path("test") / "data" / "single_changelog"
        PumConfig(base_path=base_path, pum={"module": "test_single_changelog"})

        with self.assertRaises(PumConfigError):
            PumConfig(
                base_path=base_path,
                invalid_key="You shall not pass!",
                pum={"module": "test_single_changelog"},
            )

    def test_legacy_config_format(self) -> None:
        """Test backward compatibility with legacy migration_hooks/pre/post format."""
        # Test 1: Legacy config with migration_hooks and pre/post field names
        cfg_legacy = PumConfig.from_yaml(
            Path("test") / "data" / "legacy_migration_hooks" / "legacy" / ".pum.yaml"
        )

        # Should have converted pre→drop and post→create
        self.assertEqual(len(cfg_legacy.drop_app_handlers()), 2)
        self.assertEqual(len(cfg_legacy.create_app_handlers()), 1)

        # Test 2: New config format still works
        cfg_new = PumConfig.from_yaml(
            Path("test") / "data" / "legacy_migration_hooks" / "new" / ".pum.yaml"
        )

        self.assertEqual(len(cfg_new.drop_app_handlers()), 1)
        self.assertEqual(len(cfg_new.create_app_handlers()), 1)

    def test_cleanup_hook_imports_version_switch(self) -> None:
        """Test that cleanup_hook_imports prevents conflicts when switching module versions.

        This test simulates the QGIS plugin scenario where a user switches from one
        module version to another, which can cause import conflicts if old modules
        are still cached in sys.modules.
        """
        import sys
        from unittest.mock import Mock

        # Clear any view modules from sys.modules
        modules_to_remove = [
            key for key in list(sys.modules.keys()) if "view" in key or "helper_v" in key
        ]
        for module in modules_to_remove:
            del sys.modules[module]

        # Load version 1
        v1_path = Path("test") / "data" / "hook_version_switch" / "v1"
        cfg_v1 = PumConfig(
            base_path=v1_path,
            pum={"module": "test_version_switch_v1"},
            application={"create": [{"file": "app/create_hook.py"}]},
            validate=False,  # Skip validation to focus on hook loading
        )

        # Get handlers for v1
        handlers_v1 = cfg_v1.create_app_handlers()
        self.assertEqual(len(handlers_v1), 1)

        # Execute v1 hook to ensure it works
        mock_conn = Mock()
        handlers_v1[0].execute(connection=mock_conn, parameters={})

        # Verify v1 modules are in sys.modules
        self.assertTrue(
            any("helper_v1" in mod for mod in sys.modules), "helper_v1 should be in sys.modules"
        )

        # Clean up v1
        cfg_v1.cleanup_hook_imports()

        # Verify v1 modules are removed
        self.assertFalse(
            any("helper_v1" in mod for mod in sys.modules),
            "helper_v1 should be removed from sys.modules after cleanup",
        )

        # Load version 2 - this should work without conflicts
        v2_path = Path("test") / "data" / "hook_version_switch" / "v2"
        cfg_v2 = PumConfig(
            base_path=v2_path,
            pum={"module": "test_version_switch_v2"},
            application={"create": [{"file": "app/create_hook.py"}]},
            validate=False,
        )

        # Get handlers for v2
        handlers_v2 = cfg_v2.create_app_handlers()
        self.assertEqual(len(handlers_v2), 1)

        # Execute v2 hook - should work without import conflicts
        handlers_v2[0].execute(connection=mock_conn, parameters={})

        # Verify v2 modules are in sys.modules
        self.assertTrue(
            any("helper_v2" in mod for mod in sys.modules), "helper_v2 should be in sys.modules"
        )

        # Clean up v2
        cfg_v2.cleanup_hook_imports()

        # Verify v2 modules are removed
        self.assertFalse(
            any("helper_v2" in mod for mod in sys.modules),
            "helper_v2 should be removed from sys.modules after cleanup",
        )

    def test_parameter_type_string_conversion(self) -> None:
        """Test that ParameterType enums convert to string values correctly.

        This is important for external tools (like QGIS plugins) that need to
        check parameter types as strings.
        """
        cfg = PumConfig.from_yaml(
            Path("test") / "data" / "parameters" / ".pum.yaml",
            validate=False,
        )
        params = cfg.parameters()

        # Verify parameters are loaded
        self.assertEqual(len(params), 3)

        # Test that str() returns the value, not the enum representation
        param_int = next(p for p in params if p.name == "SRID")
        self.assertEqual(str(param_int.type), "integer")
        self.assertEqual(f"{param_int.type}", "integer")

        param_int2 = next(p for p in params if p.name == "default_integer_value")
        self.assertEqual(str(param_int2.type), "integer")

        param_text = next(p for p in params if p.name == "default_text_value")
        self.assertEqual(str(param_text.type), "text")
