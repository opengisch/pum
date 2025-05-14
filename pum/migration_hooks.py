from enum import Enum
from pathlib import Path


class HookType(Enum):
    PRE = "pre"
    POST = "post"


class MigrationHook:
    """
    Base class for migration hooks.
    """

    def __init__(self, type: str | HookType, file: str | Path | None = None):
        """
        Initialize a MigrationHook instance.

        Args:
            type (str): The type of the hook (e.g., "pre", "post").
            file (str): The file path of the hook.
        """
        self.type = type if isinstance(type, HookType) else HookType(type)
        self.file = file

    def __repr__(self):
        return f"<{self.type.value} hook: {self.file}>"

    def __eq__(self, other):
        if not isinstance(other, MigrationHook):
            return NotImplemented
        return self.type == other.type and self.file == other.file
