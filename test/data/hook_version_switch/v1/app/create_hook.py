"""Hook for version 1 of the module."""

import psycopg

from pum.hook import HookBase
from view.helper_v1 import get_value_v1


class Hook(HookBase):
    """Test hook for version 1."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook for version 1.

        Args:
            connection: The database connection.

        Returns:
            The value from version 1 helper.
        """
        value = get_value_v1()
        assert value == "value_v1"
        return value
