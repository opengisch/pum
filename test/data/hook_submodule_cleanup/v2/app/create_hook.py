"""Hook for version 2 that imports from a nested submodule."""

import psycopg

from pum.hook import HookBase
from view.submodule.helper import get_value


class Hook(HookBase):
    """Test hook for version 2 with submodule imports."""

    def run_hook(self, connection: psycopg.Connection) -> str:
        """Run the hook for version 2."""
        value = get_value()
        assert value == "value_from_submodule_v2", (
            f"Expected 'value_from_submodule_v2', got '{value}'"
        )
        return value
