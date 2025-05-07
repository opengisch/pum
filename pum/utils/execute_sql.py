import logging
from pathlib import Path

from jinja2 import Template

from psycopg import Connection, Cursor
from psycopg.errors import SyntaxError

from pum.exceptions import PumSqlException

logger = logging.getLogger(__name__)


def execute_sql(
    conn: Connection,
    sql: str | Path,
    parameters: dict | None = None,
    commit: bool = False,
) -> Cursor:
    """
    Execute a SQL statement with optional parameters.

    Args:
        conn (Connection): The database connection to execute the SQL statement.
        sql (str | Path): The SQL statement to execute or a path to a SQL file.
        parameters (dict, optional): Parameters to bind to the SQL statement. Defaults to ().
        commit (bool, optional): Whether to commit the transaction. Defaults to False.
    Raises:
        RuntimeError: If the SQL execution fails.
    """
    cursor = conn.cursor()
    try:
        if isinstance(sql, Path):
            logger.debug(
                f"Executing SQL from file: {sql}",
            )
            with open(sql) as file:
                if parameters:
                    sql_code = Template(file.read()).render(**parameters)
                else:
                    sql_code = file.read()
                sql_code = sql_code.split(";")
        else:
            sql_code = [sql]

        for statement in sql_code:
            cursor.execute(statement)

    except SyntaxError as e:
        raise PumSqlException(f"SQL execution failed for the following code: {sql} {e}") from e
    if commit:
        conn.commit()

    return cursor
