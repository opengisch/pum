from pathlib import Path

import psycopg.cursor

from pum.exceptions import PumSqlException


def execute_sql(
    cursor: psycopg.cursor, sql: str | Path, params: tuple = (), commit: bool = False
) -> None:
    """
    Execute a SQL statement with optional parameters.

    Args:
        cursor (psycopg.cursor): The database cursor to execute the SQL statement.
        sql (str): The SQL statement to execute or a path to a SQL file.
        params (tuple, optional): Parameters to bind to the SQL statement. Defaults to ().
        commit (bool, optional): Whether to commit the transaction. Defaults to False.
    Raises:
        RuntimeError: If the SQL execution fails.
    """
    try:
        sql_code = sql
        if type(sql) == Path:
            log(
                f"Executing SQL from file: {sql}",
                MessageType.INFO,
            )
            with open(sql) as file:
                sql_code = file.read()
        cursor.execute(sql_code, params)
    except psycopg.errors.SyntaxError as e:
        raise PumSqlException(
            f"SQL execution failed for the following code: {sql} {e}"
        ) from e
    if commit:
        cursor.connection.commit()
