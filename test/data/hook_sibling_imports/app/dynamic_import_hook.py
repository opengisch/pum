"""Hook that dynamically imports from a sibling directory at runtime."""

import psycopg

from pum.hook import HookBase


class Hook(HookBase):
    """Test hook that dynamically imports from sibling package during run_hook."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook and dynamically import from sibling directory.

        Args:
            connection: The database connection.

        Returns:
            The value from the dynamically imported sibling module.
        """
        # This import happens at runtime, not at module load time
        from view.helper import get_value

        value = get_value()
        assert value == "test_value"
        return value
