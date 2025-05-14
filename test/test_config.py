import unittest
from pathlib import Path


from pum.config import PumConfig
from pum.migration_hooks import MigrationHook, HookType


class TestConfig(unittest.TestCase):
    """
    Test the class Upgrader.
    """

    def test_changelog(self):
        cfg = PumConfig.from_yaml(Path("test") / "data" / "pre_post_sql" / ".pum.yaml")

        self.assertEqual(
            cfg.migration_hooks_post, [MigrationHook(HookType.POST, "post/create_view.sql")]
        )
        self.assertEqual(
            cfg.migration_hooks_pre, [MigrationHook(HookType.PRE, "pre/drop_view.sql")]
        )
