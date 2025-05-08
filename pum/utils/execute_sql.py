import logging
from pathlib import Path

from psycopg import Connection, Cursor
from psycopg.errors import SyntaxError

from ..exceptions import PumSqlException
import re

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
    if isinstance(sql, Path):
        logger.debug(
            f"Executing SQL from file: {sql} with parameters: {parameters}",
        )
        with open(sql) as file:
            sql_content = file.read()
            # Remove SQL comments
            def remove_sql_comments(sql):
                # Remove multiline comments (/* ... */)
                sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
                # Remove single-line comments (-- ...)
                sql = re.sub(r'(?m)(^|;)\s*--.*?(\r\n|\r|\n)', r'\1', sql)
                return sql
            sql_content = remove_sql_comments(sql_content)
            if parameters:
                for key, value in parameters.items():
                    sql_content = sql_content.replace(f"{{{{ {key} }}}}", str(value))
                sql_code = sql_content
            else:
                sql_code = sql_content

            def split_sql_statements(sql):
                pattern = r'(?:[^;\'"]|\'[^\']*\'|"[^"]*")*;'
                matches = re.finditer(pattern, sql, re.DOTALL)
                statements = []
                last_end = 0
                for match in matches:
                    end = match.end()
                    statements.append(sql[last_end : end - 1].strip())
                    last_end = end
                if last_end < len(sql):
                    statements.append(sql[last_end:].strip())
                return [stmt for stmt in statements if stmt]

            sql_code = split_sql_statements(sql_code)
    else:
        sql_code = [sql]

    for statement in sql_code:
        try:
            cursor.execute(statement)
        except SyntaxError as e:
            logger.debug(f"Error executing SQL: {statement}")
            raise PumSqlException(f"SQL execution failed for the following code: {sql} {e}") from e
    if commit:
        conn.commit()

    return cursor
