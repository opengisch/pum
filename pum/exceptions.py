# Base exception for all PUM errors
class PumException(Exception):
    """Base class for all exceptions raised by PUM.

    .. versionadded:: 1.0.0
    """


class PumDependencyError(PumException):
    """Exception when dependency are not resolved.

    .. versionadded:: 1.0.0
    """


# --- Configuration and Validation Errors ---


class PumConfigError(PumException):
    """Exception raised for errors in the PUM configuration.

    .. versionadded:: 1.0.0
    """


class PumInvalidChangelog(PumException):
    """Exception raised for invalid changelog.

    .. versionadded:: 1.0.0
    """


# --- Schema Migration Errors ---
class PumSchemaMigrationError(PumException):
    """Exception raised for errors related to schema migrations.

    .. versionadded:: 1.0.0
    """


class PumSchemaMigrationNoBaselineError(PumSchemaMigrationError):
    """Exception raised when no baseline version is found in the migration table.

    .. versionadded:: 1.0.0
    """


# --- Hook Errors ---


class PumHookError(PumException):
    """Exception raised for errors by an invalid hook.

    .. versionadded:: 1.0.0
    """


# --- Changelog/SQL Errors ---


class PumSqlError(PumException):
    """Exception raised for SQL-related errors in PUM.

    .. versionadded:: 1.0.0
    """


# --- Dump/Restore Errors (for dumper.py, if needed) ---


class PgDumpCommandError(PumException):
    """Exception raised for invalid pg_dump command.

    .. versionadded:: 1.0.0
    """


class PgDumpFailed(PumException):
    """Exception raised when pg_dump fails.

    .. versionadded:: 1.0.0
    """


class PgRestoreCommandError(PumException):
    """Exception raised for invalid pg_restore command.

    .. versionadded:: 1.0.0
    """


class PgRestoreFailed(PumException):
    """Exception raised when pg_restore fails.

    .. versionadded:: 1.0.0
    """
