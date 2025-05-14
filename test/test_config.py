import unittest
from pathlib import Path


from pum.config import PumConfig
from pum.migration_hook import MigrationHook, MigrationHookType


class TestConfig(unittest.TestCase):
    """
    Test the class Upgrader.
    """

    def test_changelog(self):
        cfg = PumConfig.from_yaml(Path("test") / "data" / "pre_post_sql_files" / ".pum.yaml")

        self.assertEqual(
            cfg.post_hooks,
            [
                MigrationHook(
                    MigrationHookType.POST, "test/data/pre_post_sql_files/post/create_view.sql"
                )
            ],
        )
        self.assertEqual(
            cfg.pre_hooks,
            [
                MigrationHook(
                    MigrationHookType.PRE, "test/data/pre_post_sql_files/pre/drop_view.sql"
                )
            ],
        )
