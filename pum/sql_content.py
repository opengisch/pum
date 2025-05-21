import logging
from pathlib import Path
import psycopg
from .exceptions import PumSqlError
import re


logger = logging.getLogger(__name__)


def sql_chunks_from_file(file: str | Path) -> list[psycopg.sql.SQL]:
    """
    Read SQL from a file, remove comments, and split into chunks.

    Args:
        file (str | Path): Path to the SQL file.
    Returns:
        list: List of SQL statements.
    Raises:
        PumInvalidSqlFile: If the SQL file contains forbidden transaction statements.
    """
    file = Path(file) if not isinstance(file, Path) else file
    sql_code = []
    with open(file) as file:
        sql_content = file.read()

        # Remove SQL comments
        def remove_sql_comments(sql):
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

        sql_code = split_sql_statements(sql_content)

        sql_code = [re.sub(r"[\r\n]+", " ", stmt) for stmt in sql_code]
        sql_code = [psycopg.sql.SQL(stmt) for stmt in sql_code]

    return sql_code


class SqlContent:
    """
    Class to handle SQL content preparation and execution.
    """

    def __init__(self, sql: str | psycopg.sql.SQL | Path):
        """
        Initialize the SqlContent class.

        Args:
            sql: The SQL statement to execute or a path to a SQL file.
        """
        self.sql = sql

    def validate(self, parameters: dict | None = None) -> bool:
        """
        Validate the SQL content.
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
        commit: bool = False,
    ) -> psycopg.Cursor:
        """
        Execute a SQL statement with optional parameters.

        Args:
            connection: The database connection to execute the SQL statement.
            parameters: Parameters to bind to the SQL statement. Defaults to ().
            commit: Whether to commit the transaction. Defaults to False.
        """
        cursor = connection.cursor()

        sql_code = self._prepare_sql(parameters)

        for statement in sql_code:
            try:
                statement = statement.as_string(connection)
            except (psycopg.errors.SyntaxError, psycopg.errors.ProgrammingError) as e:
                raise PumSqlError(
                    f"SQL execution failed for the following code: {statement} {e}"
                ) from e
            cursor.execute(statement)

        if commit:
            connection.commit()

        return cursor

    def _prepare_sql(self, parameters: dict | None = None) -> list[psycopg.sql.SQL]:
        """
        Prepare SQL for execution.

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

        sql_code = [format_sql(statement, parameters) for statement in sql_code]

        return sql_code
