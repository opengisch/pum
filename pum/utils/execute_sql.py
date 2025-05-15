import logging
from pathlib import Path

from psycopg import Connection, Cursor
from psycopg.errors import SyntaxError
from .sql_chunks_from_file import sql_chunks_from_file

from ..exceptions import PumSqlException

logger = logging.getLogger(__name__)


def execute_sql(
    connection: Connection,
    sql: str | Path,
    parameters: dict | None = None,
    commit: bool = False,
) -> Cursor:
    """
    Execute a SQL statement with optional parameters.

    Args:
        connection (Connection): The database connection to execute the SQL statement.
        sql (str | Path): The SQL statement to execute or a path to a SQL file.
        parameters (dict, optional): Parameters to bind to the SQL statement. Defaults to ().
        commit (bool, optional): Whether to commit the transaction. Defaults to False.
    Raises:
        RuntimeError: If the SQL execution fails.
    """
    cursor = connection.cursor()
    if isinstance(sql, Path):
        logger.debug(
            f"Executing SQL from file: {sql} with parameters: {parameters}",
        )
        sql_code = sql_chunks_from_file(sql, parameters=parameters)
    else:
        sql_code = [sql]

    for statement in sql_code:
        try:
            cursor.execute(statement)
        except SyntaxError as e:
            logger.debug(f"Error executing SQL: {statement}")
            raise PumSqlException(f"SQL execution failed for the following code: {sql} {e}") from e
    if commit:
        connection.commit()

    return cursor
