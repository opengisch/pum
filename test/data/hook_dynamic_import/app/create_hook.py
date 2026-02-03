"""Hook that imports dynamically inside run_hook."""

import psycopg

from pum.hook import HookBase


class Hook(HookBase):
    """Test hook that imports dynamically at runtime."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook with a dynamic import inside the method.

        This simulates the case where a module is imported during hook execution,
        not at module load time. This is common when hooks need to import
        different modules based on runtime conditions.

        Args:
            connection: The database connection.

        Returns:
            The value from the dynamically imported module.
        """
        # Import happens HERE, during execution, not at module load time
        from helper import get_dynamic_value

        value = get_dynamic_value()
        assert value == "dynamic_value", f"Expected 'dynamic_value', got '{value}'"
        return value
