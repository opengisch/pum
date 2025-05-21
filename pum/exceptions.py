# Base exception for all PUM errors
class PumException(Exception):
    """Base class for all exceptions raised by PUM."""

    pass


# --- Configuration and Validation Errors ---


class PumConfigError(PumException):
    """Exception raised for errors in the PUM configuration."""

    pass


class PumInvalidChangelog(PumException):
    """Exception raised for invalid changelog."""

    pass


# --- Hook Errors ---


class PumHookError(PumException):
    """Exception raised for errors by an invalid hook."""

    pass


# --- Changelog/SQL Errors ---


class PumSqlError(PumException):
    """Exception raised for SQL-related errors in PUM."""

    pass


# --- Dump/Restore Errors (for dumper.py, if needed) ---


class PgDumpCommandError(PumException):
    """Exception raised for invalid pg_dump command."""

    pass


class PgDumpFailed(PumException):
    """Exception raised when pg_dump fails."""

    pass


class PgRestoreCommandError(PumException):
    """Exception raised for invalid pg_restore command."""

    pass


class PgRestoreFailed(PumException):
    """Exception raised when pg_restore fails."""

    pass
