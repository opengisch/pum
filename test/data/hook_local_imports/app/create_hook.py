"""Hook that imports from the same directory."""

import psycopg

from pum.hook import HookBase
from local_helper import local_function


class Hook(HookBase):
    """Test hook that imports from the same directory."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook and return the value from local import.

        Args:
            connection: The database connection.

        Returns:
            The value from the local module.
        """
        value = local_function()
        assert value == "local_value"
        return value
