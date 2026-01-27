"""Test module for hook functionality."""

import unittest
from pathlib import Path

from pum.hook import HookHandler


class TestHooks(unittest.TestCase):
    """Test the hook functionality."""

    def test_hook_with_sibling_imports(self) -> None:
        """Test that a hook file can import from a sibling directory.

        This test verifies that PUM hook files can properly import from sibling
        directories. The test uses a hook file in app/ that imports from view/,
        which is a sibling directory.
        """
        test_dir = Path("test") / "data" / "hook_sibling_imports"
        hook_file = test_dir / "app" / "create_hook.py"

        # Create hook handler - this should not raise an error
        handler = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))

        # Verify the hook was loaded correctly
        self.assertIsNotNone(handler.hook_instance)
        self.assertTrue(hasattr(handler.hook_instance, "run_hook"))

    def test_hook_with_local_imports(self) -> None:
        """Test that a hook file can import from its own directory.

        This test verifies that PUM hook files can properly import from their
        own directory. The test uses a hook file in app/ that imports from
        local_helper.py in the same directory.
        """
        test_dir = Path("test") / "data" / "hook_local_imports"
        hook_file = test_dir / "app" / "create_hook.py"

        # Create hook handler - this should not raise an error
        handler = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))

        # Verify the hook was loaded correctly
        self.assertIsNotNone(handler.hook_instance)
        self.assertTrue(hasattr(handler.hook_instance, "run_hook"))


if __name__ == "__main__":
    unittest.main()
