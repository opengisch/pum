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
    from .database import create_database, drop_database
    from .dependency_handler import DependencyHandler
    from .dumper import Dumper, DumpFormat
    from .feedback import Feedback, LogFeedback, SilentFeedback
    from .hook import HookBase, HookHandler
    from .parameter import ParameterDefinition, ParameterType
    from .pum_config import PumConfig
    from .role_manager import (
        Permission,
        PermissionType,
        Role,
        RoleManager,
        RoleInventory,
        RoleStatus,
        SchemaPermissionStatus,
    )
    from .schema_migrations import SchemaMigrations
    from .sql_content import SqlContent, CursorResult
    from .upgrader import Upgrader

__all__ = [
    "Checker",
    "Changelog",
    "create_database",
    "CursorResult",
    "DependencyHandler",
    "drop_database",
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
    "RoleInventory",
    "RoleManager",
    "RoleStatus",
    "SchemaPermissionStatus",
    "SchemaMigrations",
    "SilentFeedback",
    "SQL",
    "SqlContent",
    "Upgrader",
]


# Use relative imports so that when pum is bundled inside another package
# (e.g. oqtopus.libs.pum), the lazy imports resolve from the same package tree
# rather than picking up a different pum installed elsewhere on sys.path.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "Checker": (".checker", "Checker"),
    "Changelog": (".changelog", "Changelog"),
    "create_database": (".database", "create_database"),
    "CursorResult": (".sql_content", "CursorResult"),
    "DependencyHandler": (".dependency_handler", "DependencyHandler"),
    "drop_database": (".database", "drop_database"),
    "Dumper": (".dumper", "Dumper"),
    "DumpFormat": (".dumper", "DumpFormat"),
    "HookBase": (".hook", "HookBase"),
    "Feedback": (".feedback", "Feedback"),
    "HookHandler": (".hook", "HookHandler"),
    "LogFeedback": (".feedback", "LogFeedback"),
    "ParameterDefinition": (".parameter", "ParameterDefinition"),
    "ParameterType": (".parameter", "ParameterType"),
    "Permission": (".role_manager", "Permission"),
    "PermissionType": (".role_manager", "PermissionType"),
    "PumConfig": (".pum_config", "PumConfig"),
    "Role": (".role_manager", "Role"),
    "RoleInventory": (".role_manager", "RoleInventory"),
    "RoleManager": (".role_manager", "RoleManager"),
    "RoleStatus": (".role_manager", "RoleStatus"),
    "SchemaPermissionStatus": (".role_manager", "SchemaPermissionStatus"),
    "SchemaMigrations": (".schema_migrations", "SchemaMigrations"),
    "SilentFeedback": (".feedback", "SilentFeedback"),
    "SqlContent": (".sql_content", "SqlContent"),
    "Upgrader": (".upgrader", "Upgrader"),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, symbol_name = _LAZY_IMPORTS[name]
    module = importlib.import_module(module_name, package=__package__)
    value = getattr(module, symbol_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(_LAZY_IMPORTS.keys())))
