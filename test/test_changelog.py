import unittest
from pathlib import Path

from pum.pum_config import PumConfig
from pum.exceptions import PumInvalidChangelog


class TestChangelog(unittest.TestCase):
    """Test the class Changelog."""

    def test_invalid_changelog(self) -> None:
        """Test invalid changelog."""
        cfg = PumConfig(Path("test") / "data" / "invalid_changelog_commit", validate=False)
        with self.assertRaises(PumInvalidChangelog):
            for changelog in cfg.changelogs():
                changelog.validate()


if __name__ == "__main__":
    unittest.main()
