import logging
from pathlib import Path

from psycopg import Connection, Cursor
from psycopg.errors import SyntaxError

from pum.exceptions import PumSqlException

logger = logging.getLogger(__name__)


def execute_sql(
    conn: Connection, sql: str | Path, params: tuple = (), commit: bool = False
) -> Cursor:
    """
    Execute a SQL statement with optional parameters.

    Args:
        conn (Connection): The database connection to execute the SQL statement.
        sql (str | Path): The SQL statement to execute or a path to a SQL file.
        params (tuple, optional): Parameters to bind to the SQL statement. Defaults to ().
        commit (bool, optional): Whether to commit the transaction. Defaults to False.
    Raises:
        RuntimeError: If the SQL execution fails.
    """
    cursor = conn.cursor()
    try:
        sql_code = sql
        if isinstance(sql, Path):
            logger.debug(
                f"Executing SQL from file: {sql}",
            )
            with open(sql) as file:
                sql_code = file.read()
        cursor.execute(sql_code, params)
    except SyntaxError as e:
        raise PumSqlException(
            f"SQL execution failed for the following code: {sql} {e}"
        ) from e
    if commit:
        conn.commit()

    return cursor
