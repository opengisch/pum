class PgDumpCommandError(Exception):
    pass


class PgDumpFailed(Exception):
    pass


class PgRestoreCommandError(Exception):
    pass


class PgRestoreFailed(Warning):
    pass


class PumException(Exception):
    """Base class for all exceptions raised by PUM."""

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
