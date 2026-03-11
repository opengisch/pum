import importlib
import logging
from typing import Any

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


# Use relative imports so that when pum is bundled inside another package
# (e.g. oqtopus.libs.pum), the lazy imports resolve from the same package tree
# rather than picking up a different pum installed elsewhere on sys.path.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "Checker": (".checker", "Checker"),
    "Changelog": (".changelog", "Changelog"),
    "configure_database_connect_access": (".database", "configure_database_connect_access"),
    "create_database": (".database", "create_database"),
    "get_database_connect_access": (".database", "get_database_connect_access"),
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

__all__ = sorted(list(_LAZY_IMPORTS.keys()) + ["SQL"])  # pyright: ignore[reportUnsupportedDunderAll]


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
