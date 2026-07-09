import unittest
import tempfile
from pathlib import Path

from pum.changelog import Changelog, APP_ONLY_RELEASE_FILE
from pum.pum_config import PumConfig
from pum.exceptions import PumInvalidChangelog


class TestChangelog(unittest.TestCase):
    """Test the class Changelog."""

    def test_invalid_changelog(self) -> None:
        """Test invalid changelog."""
        cfg = PumConfig(
            Path("test") / "data" / "invalid_changelog_commit",
            validate=False,
            pum={"module": "test_invalid_changelog_commit"},
        )
        with self.assertRaises(PumInvalidChangelog):
            for changelog in cfg.changelogs():
                changelog.validate()

    def test_app_only_release(self) -> None:
        """Test that a changelog with an APP_ONLY_RELEASE marker and no SQL files is valid."""
        cfg = PumConfig(
            Path("test") / "data" / "app_only_release",
            pum={"module": "test_app_only_release"},
        )
        changelogs = cfg.changelogs()
        self.assertEqual(len(changelogs), 2)
        self.assertFalse(changelogs[0].is_app_only())
        self.assertTrue(changelogs[1].is_app_only())
        self.assertEqual(changelogs[1].files(), [])
        self.assertTrue(changelogs[1].validate())

    def test_empty_changelog_without_marker(self) -> None:
        """Test that a changelog without SQL files and without marker is invalid."""
        with tempfile.TemporaryDirectory() as tmp:
            version_dir = Path(tmp) / "1.0.0"
            version_dir.mkdir()
            with self.assertRaises(PumInvalidChangelog) as ctx:
                Changelog(version_dir).validate()
            self.assertIn(APP_ONLY_RELEASE_FILE, str(ctx.exception))

    def test_app_only_release_with_sql_files(self) -> None:
        """Test that a changelog with both the marker and SQL files is invalid."""
        with tempfile.TemporaryDirectory() as tmp:
            version_dir = Path(tmp) / "1.0.0"
            version_dir.mkdir()
            (version_dir / APP_ONLY_RELEASE_FILE).touch()
            (version_dir / "01_something.sql").write_text("SELECT 1;")
            with self.assertRaises(PumInvalidChangelog) as ctx:
                Changelog(version_dir).validate()
            self.assertIn(APP_ONLY_RELEASE_FILE, str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
