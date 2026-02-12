"""Test module for hook functionality."""

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

from pum.hook import HookHandler


class TestHooks(unittest.TestCase):
    """Test the hook functionality."""

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

    def test_hook_with_dynamic_imports(self) -> None:
        """Test that a hook file can import modules dynamically inside run_hook.

        This test verifies that PUM hook files can properly import modules
        during hook execution (inside run_hook), not just at module load time.
        This is critical because hooks may need to import modules based on runtime
        conditions or call functions that themselves import modules.

        This was the bug that wasn't caught by existing tests - all previous tests
        used top-level imports which happen during __init__, not during execute().

        Also tests that hooks can be executed multiple times with consistent behavior.
        """
        test_dir = Path("test") / "data" / "hook_dynamic_import"
        hook_file = test_dir / "app" / "create_hook.py"

        # Create hook handler - this should not raise an error
        handler = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))

        # Verify the hook was loaded correctly
        self.assertIsNotNone(handler.hook_instance)
        self.assertTrue(hasattr(handler.hook_instance, "run_hook"))

        # Execute the hook - this is where the dynamic import happens
        # Without proper sys.path management during execute(), this will fail
        mock_conn = Mock()
        handler.execute(connection=mock_conn, parameters={})

        # Execute again to verify it works multiple times
        handler.execute(connection=mock_conn, parameters={})

    def test_hook_with_optional_arg(self) -> None:
        """Test that a hook with an optional argument (default value) passes validation
        even when the argument is not declared as a config parameter."""
        test_dir = Path("test") / "data" / "hook_optional_arg"
        hook_file = test_dir / "create_app" / "create_hook.py"

        handler = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))

        # lang_code has a default value, so it should not be required
        self.assertIn("lang_code", handler.parameter_args)
        self.assertNotIn("lang_code", handler.required_parameter_args)

        # Validation should pass without lang_code in parameters
        handler.validate(parameters={})

        # Execution should also work without lang_code
        mock_conn = Mock()
        handler.execute(connection=mock_conn, parameters={})
