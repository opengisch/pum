
class PgDumpCommandError(Exception):
    pass


class PgDumpFailed(Exception):
    pass


class PgRestoreCommandError(Exception):
    pass


class PgRestoreFailed(Warning):
    pass