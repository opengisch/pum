# Base exception for all PUM errors
class PumException(Exception):
    """Base class for all exceptions raised by PUM."""


class PumDependencyError(PumException):
    """Exception when dependency are not resolved"""


# --- Configuration and Validation Errors ---


class PumConfigError(PumException):
    """Exception raised for errors in the PUM configuration."""


class PumInvalidChangelog(PumException):
    """Exception raised for invalid changelog."""


# --- Hook Errors ---


class PumHookError(PumException):
    """Exception raised for errors by an invalid hook."""


# --- Changelog/SQL Errors ---


class PumSqlError(PumException):
    """Exception raised for SQL-related errors in PUM."""


# --- Dump/Restore Errors (for dumper.py, if needed) ---


class PgDumpCommandError(PumException):
    """Exception raised for invalid pg_dump command."""


class PgDumpFailed(PumException):
    """Exception raised when pg_dump fails."""


class PgRestoreCommandError(PumException):
    """Exception raised for invalid pg_restore command."""


class PgRestoreFailed(PumException):
    """Exception raised when pg_restore fails."""
