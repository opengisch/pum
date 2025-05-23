import logging
import re
from pathlib import Path

import psycopg

from .exceptions import PumSqlError

logger = logging.getLogger(__name__)


def sql_chunks_from_file(file: str | Path) -> list[psycopg.sql.SQL]:
    """Read SQL from a file, remove comments, and split into chunks.

    Args:
        file (str | Path): Path to the SQL file.

    Returns:
        list: List of SQL statements.

    Raises:
        PumInvalidSqlFile: If the SQL file contains forbidden transaction statements.

    """
    file = Path(file) if not isinstance(file, Path) else file
    sql_code = []
    with Path.open(file) as file:
        sql_content = file.read()

        # Remove SQL comments
        def remove_sql_comments(sql: str) -> str:
            """Remove SQL comments from the SQL string.

            Args:
                sql (str): The SQL string to process.

            Returns:
                str: The SQL string without comments.

            """
            # Remove multiline comments (/* ... */)
            sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
            # Remove single-line comments (-- ...)
            sql = re.sub(r"(?m)(^|;)\s*--.*?(\r\n|\r|\n)", r"\1", sql)
            return sql

        sql_content = remove_sql_comments(sql_content)

        # Check for forbidden transaction statements
        forbidden_statements = ["BEGIN;", "COMMIT;"]
        for forbidden in forbidden_statements:
            if re.search(rf"\b{forbidden[:-1]}\b\s*;", sql_content, re.IGNORECASE):
                raise PumSqlError(f"SQL contains forbidden transaction statement: {forbidden}")

        def split_sql_statements(sql: str) -> list[str]:
            """Split SQL statements by semicolon, ignoring those inside quotes and $$BODY$$/$BODY$ blocks."""
            # Find all $$BODY$$ ... $$BODY$$ and $BODY$ ... $BODY$ blocks and replace them with placeholders
            body_blocks = []
            # Regex for both $$BODY$$ and $BODY$
            body_pattern = r"(\$\$BODY\$\$.*?\$\$BODY\$\$|\$BODY\$.*?\$BODY\$)"

            def body_replacer(match):
                body_blocks.append(match.group(0))
                return f"__BODY_BLOCK_{len(body_blocks) - 1}__"

            sql_wo_body = re.sub(body_pattern, body_replacer, sql, flags=re.DOTALL)

            # Split outside of BODY blocks (ignoring semicolons in quotes)
            pattern = r'(?:[^;\'\"]|\'[^\']*\'|"[^"]*")*;'
            matches = re.finditer(pattern, sql_wo_body, re.DOTALL)
            statements = []
            last_end = 0
            for match in matches:
                end = match.end()
                statements.append(sql_wo_body[last_end : end - 1].strip())
                last_end = end
            if last_end < len(sql_wo_body):
                statements.append(sql_wo_body[last_end:].strip())

            # Restore BODY blocks
            def restore_body(stmt):
                for i, body in enumerate(body_blocks):
                    stmt = stmt.replace(f"__BODY_BLOCK_{i}__", body)
                return stmt

            return [restore_body(stmt) for stmt in statements if stmt]

        sql_code = split_sql_statements(sql_content)

        # if we want to remove new lines from the SQL code, we need to handle comments starting with --
        # and remove them before removing new lines
        # sql_code = [re.sub(r"[\r\n]+", " ", stmt) for stmt in sql_code]
        sql_code = [psycopg.sql.SQL(stmt) for stmt in sql_code]

    return sql_code


class SqlContent:
    """Class to handle SQL content preparation and execution."""

    def __init__(self, sql: str | psycopg.sql.SQL | Path) -> None:
        """Initialize the SqlContent class.

        Args:
            sql: The SQL statement to execute or a path to a SQL file.

        """
        self.sql = sql

    def validate(self, parameters: dict | None = None) -> bool:
        """Validate the SQL content.
        This is done by checking if the SQL content is not empty.

        Args:
            parameters: The parameters to pass to the SQL files.

        Returns:
            bool: True if valid, False otherwise.

        """
        if not self.sql:
            raise PumSqlError("SQL content is empty.")
        self._prepare_sql(parameters)
        return True

    def execute(
        self,
        connection: psycopg.Connection,
        parameters: dict | None = None,
        *,
        commit: bool = False,
    ) -> psycopg.Cursor:
        """Execute a SQL statement with optional parameters.

        Args:
            connection: The database connection to execute the SQL statement.
            parameters: Parameters to bind to the SQL statement. Defaults to ().
            commit: Whether to commit the transaction. Defaults to False.

        """
        cursor = connection.cursor()

        for sql_code in self._prepare_sql(parameters):
            try:
                statement = sql_code.as_string(connection)
            except (psycopg.errors.SyntaxError, psycopg.errors.ProgrammingError) as e:
                raise PumSqlError(
                    f"SQL execution failed for the following code: {statement} {e}"
                ) from e
            try:
                cursor.execute(statement)
            except (psycopg.errors.SyntaxError, psycopg.errors.ProgrammingError) as e:
                raise PumSqlError(
                    f"SQL execution failed for the following code: {statement} {e}"
                ) from e
        if commit:
            connection.commit()

        return cursor

    def _prepare_sql(self, parameters: dict | None = None) -> list[psycopg.sql.SQL]:
        """Prepare SQL for execution.

        Args:
            sql: The SQL statement to execute or a path to a SQL file.
            parameters: Parameters to bind to the SQL statement. Defaults to ().

        Returns:
            list: A list of prepared SQL statements.

        Raises:
            PumSqlError: If SQL preparation fails.

        """
        if isinstance(self.sql, Path):
            logger.debug(
                f"Preparing SQL from file: {self.sql} with parameters: {parameters}",
            )
            sql_code = sql_chunks_from_file(self.sql)
        elif isinstance(self.sql, str):
            sql_code = [psycopg.sql.SQL(self.sql)]
        else:
            sql_code = [self.sql]

        def format_sql(
            statement: psycopg.sql.SQL, parameters: dict | None = None
        ) -> psycopg.sql.SQL:
            try:
                return statement.format(**parameters)
            except TypeError:
                # if parameters is None, we can ignore this error
                return statement
            except KeyError as e:
                raise PumSqlError(
                    f"SQL preparation failed for the following code: missing parameter: {statement} {e}"
                ) from e

        return [format_sql(statement, parameters) for statement in sql_code]
