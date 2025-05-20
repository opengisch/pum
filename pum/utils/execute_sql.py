import logging
from pathlib import Path

import psycopg
from .sql_chunks_from_file import sql_chunks_from_file

from ..exceptions import PumSqlException

logger = logging.getLogger(__name__)


def execute_sql(
    connection: psycopg.Connection,
    sql: str | psycopg.sql.SQL | Path,
    parameters: dict | None = None,
    commit: bool = False,
) -> psycopg.Cursor:
    """
    Execute a SQL statement with optional parameters.

    Args:
        connection: The database connection to execute the SQL statement.
        sql: The SQL statement to execute or a path to a SQL file.
        parameters: Parameters to bind to the SQL statement. Defaults to ().
        commit: Whether to commit the transaction. Defaults to False.
    """
    cursor = connection.cursor()
    if isinstance(sql, Path):
        logger.debug(
            f"Executing SQL from file: {sql} with parameters: {parameters}",
        )
        sql_code = sql_chunks_from_file(sql)
    elif isinstance(sql, str):
        sql_code = [psycopg.sql.SQL(sql)]
    else:
        sql_code = [sql]

    for statement in sql_code:
        try:
            if parameters:
                statement = statement.format(**parameters)
            cursor.execute(statement)
        except (psycopg.errors.SyntaxError, psycopg.errors.ProgrammingError) as e:
            raise PumSqlException(
                f"SQL execution failed for the following code: {statement} {e}"
            ) from e
    if commit:
        connection.commit()

    return cursor
