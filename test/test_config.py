import unittest
from pathlib import Path


from pum.config import PumConfig
from pum.migration_hooks import MigrationHook, MigrationHookType


class TestConfig(unittest.TestCase):
    """
    Test the class Upgrader.
    """

    def test_changelog(self):
        cfg = PumConfig.from_yaml(Path("test") / "data" / "pre_post_sql" / ".pum.yaml")

        self.assertEqual(
            cfg.post_hooks,
            [MigrationHook(MigrationHookType.POST, "post/create_view.sql")],
        )
        self.assertEqual(cfg.pre_hooks, [MigrationHook(MigrationHookType.PRE, "pre/drop_view.sql")])
