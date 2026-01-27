"""Test module for hook functionality."""

import unittest
from pathlib import Path
from unittest.mock import Mock

from pum.hook import HookHandler


class TestHooks(unittest.TestCase):
    """Test the hook functionality."""

    def test_hook_with_sibling_imports(self) -> None:
        """Test that a hook file can import from a sibling directory.

        This test verifies that PUM hook files can properly import from sibling
        directories. The test uses a hook file in app/ that imports from view/,
        which is a sibling directory.
        """
        import sys

        test_dir = Path("test") / "data" / "hook_sibling_imports"
        hook_file = test_dir / "app" / "create_hook.py"

        # Clear any previously imported modules to ensure fresh import
        modules_to_remove = [key for key in sys.modules if "view.helper" in key or "view" == key]
        for module in modules_to_remove:
            del sys.modules[module]

        # Create hook handler - this should not raise an error
        handler = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))

        # Verify the hook was loaded correctly
        self.assertIsNotNone(handler.hook_instance)
        self.assertTrue(hasattr(handler.hook_instance, "run_hook"))

        # At this point sys.path should not contain the hook paths anymore
        test_dir_str = str(test_dir.resolve())
        app_dir_str = str((test_dir / "app").resolve())
        self.assertNotIn(test_dir_str, sys.path, "base_path should have been removed from sys.path")
        self.assertNotIn(app_dir_str, sys.path, "parent dir should have been removed from sys.path")

        # Execute the hook to ensure imports work at runtime
        # This is where the bug should manifest - view.helper won't be importable
        mock_conn = Mock()
        handler.execute(connection=mock_conn, parameters={})

    def test_hook_with_dynamic_sibling_imports(self) -> None:
        """Test that a hook file can dynamically import from a sibling directory at runtime.

        This test verifies that PUM hook files can properly import from sibling
        directories even when the import happens inside run_hook (not at module load time).
        """
        import sys

        test_dir = Path("test") / "data" / "hook_sibling_imports"
        hook_file = test_dir / "app" / "dynamic_import_hook.py"

        # Clear any previously imported modules to ensure fresh import
        modules_to_remove = [key for key in sys.modules if "view.helper" in key or "view" == key]
        for module in modules_to_remove:
            del sys.modules[module]

        # Create hook handler
        handler = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))

        # Verify the hook was loaded correctly
        self.assertIsNotNone(handler.hook_instance)
        self.assertTrue(hasattr(handler.hook_instance, "run_hook"))

        # Execute the hook - this should fail because sys.path modifications were removed
        mock_conn = Mock()
        handler.execute(connection=mock_conn, parameters={})

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

        # Execute the hook to ensure imports work at runtime
        mock_conn = Mock()
        handler.execute(connection=mock_conn, parameters={})


if __name__ == "__main__":
    unittest.main()
