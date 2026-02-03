"""Hook for version 2 with dynamic imports."""

import psycopg

from pum.hook import HookBase


class Hook(HookBase):
    """Test hook for version 2 with dynamic imports."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook for version 2 with dynamic import.

        Args:
            connection: The database connection.

        Returns:
            The value from version 2 helper.
        """
        # Dynamic import inside run_hook
        from helper_dynamic_v2 import get_dynamic_value_v2

        value = get_dynamic_value_v2()
        assert value == "dynamic_value_v2", f"Expected 'dynamic_value_v2', got '{value}'"
        return value
