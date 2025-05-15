import unittest
from pathlib import Path


from pum.config import PumConfig
from pum.exceptions import PumInvalidChangelog


class TestChangelog(unittest.TestCase):
    """
    Test the class Changelog.
    """

    def test_invalid_changelog(self):
        cfg = PumConfig(Path("test") / "data" / "invalid_changelog")
        with self.assertRaises(PumInvalidChangelog):
            cfg.validate_changelogs()


if __name__ == "__main__":
    unittest.main()
