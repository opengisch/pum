"""Hook for version 2 of the module."""

import psycopg

from pum.hook import HookBase
from view.helper_v2 import get_value_v2


class Hook(HookBase):
    """Test hook for version 2."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook for version 2.

        Args:
            connection: The database connection.

        Returns:
            The value from version 2 helper.
        """
        value = get_value_v2()
        assert value == "value_v2"
        return value
