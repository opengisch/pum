"""Hook that imports from a sibling directory."""

import psycopg

from pum.hook import HookBase
from view.helper import get_value


class Hook(HookBase):
    """Test hook that imports from sibling package."""

    def run_hook(self, connection: psycopg.Connection, parameters: dict | None = None) -> str:
        """Run the hook and return the value from sibling import.

        Args:
            connection: The database connection.
            parameters: Parameters to bind to the SQL statement. Defaults to None.

        Returns:
            The value from the sibling module.
        """
        value = get_value()
        assert value == "test_value"
        return value
