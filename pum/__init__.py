import importlib
import logging
from typing import Any, TYPE_CHECKING

# Custom SQL logging level (more verbose than DEBUG)
# Register with: logging.addLevelName(SQL, 'SQL')
SQL = 5

# Configure default logging for API usage (not CLI)
# CLI will override this with its own configuration
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

if TYPE_CHECKING:
    from .changelog import Changelog
    from .checker import Checker
    from .dependency_handler import DependencyHandler
    from .dumper import Dumper, DumpFormat
    from .feedback import Feedback, LogFeedback, SilentFeedback
    from .hook import HookBase, HookHandler
    from .parameter import ParameterDefinition, ParameterType
    from .pum_config import PumConfig
    from .role_manager import Permission, PermissionType, Role, RoleManager
    from .schema_migrations import SchemaMigrations
    from .sql_content import SqlContent, CursorResult
    from .upgrader import Upgrader

__all__ = [
    "Checker",
    "Changelog",
    "CursorResult",
    "DependencyHandler",
    "Dumper",
    "DumpFormat",
    "Feedback",
    "HookBase",
    "HookHandler",
    "LogFeedback",
    "ParameterDefinition",
    "ParameterType",
    "Permission",
    "PermissionType",
    "PumConfig",
    "Role",
    "RoleManager",
    "SchemaMigrations",
    "SilentFeedback",
    "SQL",
    "SqlContent",
    "Upgrader",
]


_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "Checker": ("pum.checker", "Checker"),
    "Changelog": ("pum.changelog", "Changelog"),
    "CursorResult": ("pum.sql_content", "CursorResult"),
    "DependencyHandler": ("pum.dependency_handler", "DependencyHandler"),
    "Dumper": ("pum.dumper", "Dumper"),
    "DumpFormat": ("pum.dumper", "DumpFormat"),
    "HookBase": ("pum.hook", "HookBase"),
    "HookHandler": ("pum.hook", "HookHandler"),
    "ParameterDefinition": ("pum.parameter", "ParameterDefinition"),
    "ParameterType": ("pum.parameter", "ParameterType"),
    "Permission": ("pum.role_manager", "Permission"),
    "PermissionType": ("pum.role_manager", "PermissionType"),
    "PumConfig": ("pum.pum_config", "PumConfig"),
    "Role": ("pum.role_manager", "Role"),
    "RoleManager": ("pum.role_manager", "RoleManager"),
    "SchemaMigrations": ("pum.schema_migrations", "SchemaMigrations"),
    "SqlContent": ("pum.sql_content", "SqlContent"),
    "Upgrader": ("pum.upgrader", "Upgrader"),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, symbol_name = _LAZY_IMPORTS[name]
    module = importlib.import_module(module_name)
    value = getattr(module, symbol_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(_LAZY_IMPORTS.keys())))
