import unittest
from pathlib import Path


from pum.config import PumConfig
from pum.exceptions import PumInvalidChangelog


class TestChangelog(unittest.TestCase):
    """
    Test the class Changelog.
    """

    def test_invalid_changelog(self):
        cfg = PumConfig(Path("test") / "data" / "invalid_changelog", validate=False)
        with self.assertRaises(PumInvalidChangelog):
            for changelog in cfg.list_changelogs():
                changelog.validate()


if __name__ == "__main__":
    unittest.main()
