"""Hook for version 1 with dynamic imports."""

import psycopg

from pum.hook import HookBase


class Hook(HookBase):
    """Test hook for version 1 with dynamic imports."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook for version 1 with dynamic import.

        Args:
            connection: The database connection.

        Returns:
            The value from version 1 helper.
        """
        # Dynamic import inside run_hook
        from helper_dynamic_v1 import get_dynamic_value_v1

        value = get_dynamic_value_v1()
        assert value == "dynamic_value_v1", f"Expected 'dynamic_value_v1', got '{value}'"
        return value
