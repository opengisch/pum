import unittest
from pathlib import Path


from pum.config import PumConfig
from pum.changelog_utils import list_changelogs, last_version
from packaging.version import parse as parse_version


class TestChangelog(unittest.TestCase):
    """
    Test the class Upgrader.
    """

    def test_changelog(self):
        cfg = PumConfig(dir=Path("test") / "data" / "single_changelog")
        changelogs = list_changelogs(config=cfg)
        self.assertEqual(len(changelogs), 1)
        self.assertEqual(changelogs[0].version, parse_version("1.2.3"))

        cfg = PumConfig(dir=Path("test") / "data" / "multiple_changelogs")
        changelogs = list_changelogs(config=cfg)
        self.assertEqual(len(changelogs), 4)
        self.assertEqual(changelogs[0].version, parse_version("1.2.3"))
        self.assertEqual(changelogs[1].version, parse_version("1.2.4"))
        self.assertEqual(changelogs[2].version, parse_version("1.3.0"))
        self.assertEqual(changelogs[3].version, parse_version("2.0.0"))

        last_version_result = last_version(config=cfg)
        self.assertEqual(last_version_result, parse_version("2.0.0"))

        last_version_result = last_version(
            config=cfg,
            min_version="1.2.4",
            max_version="1.3.0",
        )
        self.assertEqual(last_version_result, parse_version("1.3.0"))

        last_version_result = last_version(
            config=cfg,
            max_version="1.3.0",
        )
        self.assertEqual(last_version_result, parse_version("1.3.0"))

        last_version_result = last_version(
            config=cfg,
            max_version="1.0.0",
        )
        self.assertIsNone(last_version_result)

        last_version_result = last_version(
            config=cfg,
            min_version="1.2.3",
        )
        self.assertEqual(last_version_result, parse_version("2.0.0"))

        last_version_result = last_version(
            config=cfg,
            min_version="2.1.0",
        )
        self.assertIsNone(last_version_result)


if __name__ == "__main__":
    unittest.main()
