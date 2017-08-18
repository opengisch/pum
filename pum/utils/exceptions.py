class TableNotFoundError(Exception):
    """raise this when a db table is not found"""
    pass


class DbConnectionError(Exception):
    """raise this when is not possible to connect to database"""
    pass


class PgDumpError(Exception):
    """raise this when pg_dump stops with an error"""
    pass


class PgRestoreError(Exception):
    """raise this when pg_restore stops with an error"""
    pass
