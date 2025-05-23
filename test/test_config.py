import unittest
from pathlib import Path

from packaging.version import parse as parse_version

from pum.config import PumConfig
from pum.exceptions import PumConfigError
from pum.hook import Hook, HookType


class TestConfig(unittest.TestCase):
    """Test the class PumConfig."""

    def test_version(self) -> None:
        """Test version."""
        cfg = PumConfig(dir=Path("test") / "data" / "single_changelog")
        changelogs = cfg.list_changelogs()
        self.assertEqual(len(changelogs), 1)
        self.assertEqual(changelogs[0].version, parse_version("1.2.3"))

        cfg = PumConfig(dir=Path("test") / "data" / "multiple_changelogs")
        changelogs = cfg.list_changelogs()
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
            cfg.post_hooks,
            [
                Hook(
                    HookType.POST,
                    "test/data/pre_post_sql_files/post/create_view.sql",
                )
            ],
        )
        self.assertEqual(
            cfg.pre_hooks,
            [
                Hook(
                    HookType.PRE,
                    "test/data/pre_post_sql_files/pre/drop_view.sql",
                )
            ],
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
            PumConfig(dir=Path("test") / "data" / "invalid_changelog", validate=True)
        PumConfig(dir=Path("test") / "data" / "invalid_changelog", validate=False)

    def test_invalid_changelog_parameters(self) -> None:
        """Test invalid changelog parameters."""
        PumConfig.from_yaml(Path("test") / "data" / "parameters" / ".pum.yaml", validate=True)
        with self.assertRaises(PumConfigError):
            PumConfig(dir=Path("test") / "data" / "parameters", validate=True)
