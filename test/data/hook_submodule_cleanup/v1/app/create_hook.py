"""Hook for version 1 that imports from a nested submodule."""

import psycopg

from pum.hook import HookBase
from view.submodule.helper import get_value


class Hook(HookBase):
    """Test hook for version 1 with submodule imports."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook for version 1."""
        value = get_value()
        assert value == "value_from_submodule_v1", (
            f"Expected 'value_from_submodule_v1', got '{value}'"
        )
        return value
