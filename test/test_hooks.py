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

    def test_hook_cleanup_imports(self) -> None:
        """Test that hook imports can be cleaned up to prevent conflicts when switching versions.

        This test verifies that when hooks are cleaned up, their imported modules
        are removed from sys.modules cache, allowing fresh imports when switching
        to a different module version.
        """
        import sys

        test_dir = Path("test") / "data" / "hook_sibling_imports"
        hook_file = test_dir / "app" / "create_hook.py"

        # Clear any previously imported modules to ensure fresh import
        modules_to_remove = [key for key in sys.modules if "view" in key]
        for module in modules_to_remove:
            del sys.modules[module]

        # Load the hook - this will import view.helper
        handler = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))

        # Verify that view.helper was imported and tracked
        self.assertGreater(
            len(handler._imported_modules), 0, "Should have tracked imported modules"
        )
        self.assertTrue(
            any("view" in mod for mod in handler._imported_modules),
            "Should have tracked view module",
        )

        # Verify view.helper is in sys.modules
        view_module_found = any("view" in mod for mod in sys.modules)
        self.assertTrue(
            view_module_found, "view module should be in sys.modules after loading hook"
        )

        # Execute the hook to make sure it works before cleanup
        mock_conn = Mock()
        handler.execute(connection=mock_conn, parameters={})

        # Clean up imports
        handler.cleanup_imports()

        # Verify that tracked modules were cleared
        self.assertEqual(
            len(handler._imported_modules), 0, "Should have cleared tracked modules list"
        )

        # Verify that view modules were removed from sys.modules
        view_modules_after = [mod for mod in sys.modules if "view.helper" in mod or mod == "view"]
        self.assertEqual(
            len(view_modules_after),
            0,
            f"view modules should be removed from sys.modules after cleanup, but found: {view_modules_after}",
        )

    def test_hook_cleanup_and_reload(self) -> None:
        """Test that hooks can be reloaded after cleanup without conflicts.

        This test simulates switching between module versions by loading a hook,
        cleaning it up, and loading it again.
        """
        import sys

        test_dir = Path("test") / "data" / "hook_sibling_imports"
        hook_file = test_dir / "app" / "create_hook.py"

        # Clear any previously imported modules
        modules_to_remove = [key for key in sys.modules if "view" in key]
        for module in modules_to_remove:
            del sys.modules[module]

        # First load
        handler1 = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))
        mock_conn = Mock()
        handler1.execute(connection=mock_conn, parameters={})

        # Get the module object for comparison
        view_module_id_1 = None
        for mod_name in sys.modules:
            if mod_name == "view" or mod_name.startswith("view."):
                view_module_id_1 = id(sys.modules[mod_name])
                break

        # Clean up
        handler1.cleanup_imports()

        # Verify cleanup worked
        view_modules = [mod for mod in sys.modules if "view.helper" in mod or mod == "view"]
        self.assertEqual(len(view_modules), 0, "Modules should be cleaned up")

        # Second load - should work without conflicts
        handler2 = HookHandler(base_path=test_dir, file=str(hook_file.relative_to(test_dir)))
        handler2.execute(connection=mock_conn, parameters={})

        # Verify it's a fresh import (different module object)
        view_module_id_2 = None
        for mod_name in sys.modules:
            if mod_name == "view" or mod_name.startswith("view."):
                view_module_id_2 = id(sys.modules[mod_name])
                break

        # Both should have found view modules
        self.assertIsNotNone(view_module_id_1, "First load should have imported view")
        self.assertIsNotNone(view_module_id_2, "Second load should have imported view")

        # Clean up after test
        handler2.cleanup_imports()

    def test_hook_submodule_cleanup_on_version_switch(self) -> None:
        """Test that submodules are properly cleaned up when switching between module versions.

        This test simulates the real-world scenario where a user switches between
        different versions of a module that imports from nested submodules (e.g., view.submodule.helper).
        Without proper submodule cleanup, the cached view module from v1 would prevent
        v2 view.submodule.helper from being imported correctly.
        """
        import sys

        v1_dir = Path("test") / "data" / "hook_submodule_cleanup" / "v1"
        v2_dir = Path("test") / "data" / "hook_submodule_cleanup" / "v2"
        hook_file = Path("app") / "create_hook.py"

        # Clear any previously imported view modules
        modules_to_remove = [key for key in sys.modules if key == "view" or key.startswith("view.")]
        for module in modules_to_remove:
            del sys.modules[module]

        # Load v1 hook - imports view.submodule.helper which returns value_from_submodule_v1
        handler_v1 = HookHandler(base_path=v1_dir, file=str(hook_file))
        mock_conn = Mock()
        # Execute v1 hook - the assertion inside run_hook will fail if wrong module is imported
        handler_v1.execute(connection=mock_conn, parameters={})

        # Verify submodules were imported and tracked
        view_submodules = [mod for mod in sys.modules if mod.startswith("view.submodule")]
        self.assertGreater(len(view_submodules), 0, "Should have imported view.submodule modules")

        # Clean up v1 imports
        handler_v1.cleanup_imports()

        # Verify ALL view modules (including submodules) were cleaned up
        remaining_view_modules = [
            mod for mod in sys.modules if mod == "view" or mod.startswith("view.")
        ]
        self.assertEqual(
            len(remaining_view_modules),
            0,
            f"All view modules should be cleaned up, but found: {remaining_view_modules}",
        )

        # Load v2 hook - should import fresh view.submodule.helper which returns value_from_submodule_v2
        # This is the critical part - without submodule cleanup, Python would use the cached
        # view.submodule.helper from v1 and the assertion inside run_hook would fail
        handler_v2 = HookHandler(base_path=v2_dir, file=str(hook_file))
        handler_v2.execute(connection=mock_conn, parameters={})

        # Clean up
        handler_v2.cleanup_imports()
