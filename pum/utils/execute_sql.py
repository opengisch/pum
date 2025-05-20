import logging
from pathlib import Path

import psycopg
from .sql_chunks_from_file import sql_chunks_from_file

from ..exceptions import PumSqlException

logger = logging.getLogger(__name__)


def prepare_sql(
    sql: str | psycopg.sql.SQL | Path, parameters: dict | None = None
) -> list[psycopg.sql.SQL]:
    """
    Prepare SQL for execution.

    Args:
        sql: The SQL statement to execute or a path to a SQL file.
        parameters: Parameters to bind to the SQL statement. Defaults to ().

    Returns:
        list: A list of prepared SQL statements.

    Raises:
        PumSqlException: If SQL preparation fails.
    """
    if isinstance(sql, Path):
        logger.debug(
            f"Preparing SQL from file: {sql} with parameters: {parameters}",
        )
        sql_code = sql_chunks_from_file(sql)
    elif isinstance(sql, str):
        sql_code = [psycopg.sql.SQL(sql)]
    else:
        sql_code = [sql]

    def format_sql(statement: psycopg.sql.SQL, parameters: dict | None = None) -> psycopg.sql.SQL:
        try:
            return statement.format(**parameters)
        except TypeError:
            # if parameters is None, we can ignore this error
            return statement
        except KeyError as e:
            raise PumSqlException(
                f"SQL preparation failed for the following code: missing parameter: {statement} {e}"
            ) from e

    sql_code = [format_sql(statement, parameters) for statement in sql_code]

    return sql_code


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

    sql_code = prepare_sql(sql, parameters)

    for statement in sql_code:
        try:
            statement = statement.as_string(connection)
        except (psycopg.errors.SyntaxError, psycopg.errors.ProgrammingError) as e:
            raise PumSqlException(
                f"SQL execution failed for the following code: {statement} {e}"
            ) from e
        cursor.execute(statement)

    if commit:
        connection.commit()

    return cursor
