"""Hook that imports from a sibling directory."""

import psycopg

from pum.hook import HookBase
from view.helper import get_value


class Hook(HookBase):
    """Test hook that imports from sibling package."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook and return the value from sibling import.

        Args:
            connection: The database connection.

        Returns:
            The value from the sibling module.
        """
        value = get_value()
        assert value == "test_value"
        return value
