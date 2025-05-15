class PumException(Exception):
    """Base class for all exceptions raised by PUM."""

    pass


class PumSqlException(PumException):
    """Exception raised for SQL-related errors in PUM.

    Attributes:
        message (str): Explanation of the error.
    """

    def __init__(self, message):
        super().__init__(message)


class PumInvalidChangelog(PumException):
    """Exception raised for invalid changelog."""

    pass


class PumConfigError(PumException):
    """Exception raised for errors in the PUM configuration."""

    pass


class PumMigrationError(PumException):
    """Exception raised for errors in the PUM migration process."""

    pass


class PumVersionError(PumException):
    """Exception raised for version-related errors in PUM."""

    pass


class PumHookError(PumException):
    """Exception raised for errors by an invalid hook."""

    pass


class PumInvalidSqlFile(PumException):
    """Exception raised for invalid SQL files."""

    pass
