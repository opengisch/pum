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
        forbidden_statements = (
            (
                r"\bBEGIN\b\s*;",
                "BEGIN; COMMIT; is not authroized in executed SQL since connections are handled by PUM.",
            ),
            (
                r"\bCOMMIT\b\s*;",
                "BEGIN; COMMIT; is not authroized in executed SQL since connections are handled by PUM.",
            ),
            (
                r"SELECT +pg_catalog.set_config.*search_path.*;",
                "Setting of search path is not authorized in executed SQL as it breaks PostGIS installation",
            ),
        )
        for forbidden, message in forbidden_statements:
            if re.search(forbidden, sql_content, re.IGNORECASE):
                raise PumSqlError(f"SQL contains forbidden transaction statement: {message}")

        def split_sql_statements(sql: str) -> list[str]:
            """
            Split SQL statements by semicolon, ignoring those inside single/double quotes, dollar-quoted blocks, and DO/BODY blocks.
            Do NOT split on semicolons inside string literals (e.g. COMMENT ON ... IS '...;...'),
            and do NOT split on semicolons inside dollar-quoted blocks (e.g. $$...;...$$, $BODY$...;...$BODY$),
            but DO split on semicolons that are not inside a string, block, or quote, even if the statement contains a SQL comment with a semicolon.
            """
            # Step 1: Replace all dollar-quoted blocks with placeholders
            body_blocks = []
            block_pattern = (
                r"(\$\$BODY\$\$.*?\$\$BODY\$\$"
                r"|\$BODY\$.*?\$BODY\$"
                r"|\$\$DO\$\$.*?\$\$DO\$\$"
                r"|\$DO\$.*?\$DO\$"
                r"|\$[A-Za-z0-9_]*\$.*?\$[A-Za-z0-9_]*\$"  # generic $tag$...$tag$
                r"|\$\$.*?\$\$"  # generic $$...$$
                r")"
            )

            def block_replacer(match):
                body_blocks.append(match.group(0))
                return f"__BLOCK_{len(body_blocks) - 1}__"

            sql_wo_blocks = re.sub(
                block_pattern, block_replacer, sql, flags=re.DOTALL | re.IGNORECASE
            )

            # Step 2: Split by semicolon, but only when not inside a string or comment
            statements = []
            current = []
            in_single = False
            in_double = False
            in_line_comment = False
            i = 0
            while i < len(sql_wo_blocks):
                c = sql_wo_blocks[i]
                next2 = sql_wo_blocks[i : i + 2]
                if in_line_comment:
                    current.append(c)
                    if c == "\n":
                        in_line_comment = False
                    i += 1
                    continue
                if not in_single and not in_double and next2 == "--":
                    in_line_comment = True
                    current.append("--")
                    i += 2
                    continue
                if not in_double and c == "'":
                    in_single = not in_single
                    current.append(c)
                    i += 1
                    continue
                if not in_single and c == '"':
                    in_double = not in_double
                    current.append(c)
                    i += 1
                    continue
                if not in_single and not in_double and not in_line_comment and c == ";":
                    statements.append("".join(current).strip())
                    current = []
                    i += 1
                    continue
                current.append(c)
                i += 1
            if current:
                statements.append("".join(current).strip())

            # Step 3: Restore dollar-quoted blocks
            def restore_blocks(stmt):
                for idx, block in enumerate(body_blocks):
                    stmt = stmt.replace(f"__BLOCK_{idx}__", block)
                return stmt

            return [restore_blocks(stmt) for stmt in statements if stmt]

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
        if not isinstance(sql, (str, psycopg.sql.SQL, Path)):
            raise PumSqlError(
                f"SQL must be a string, psycopg.sql.SQL object or a Path object, not {type(sql)}."
            )
        self.sql = sql

    def validate(self, parameters: dict | None) -> bool:
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
        *,
        parameters: dict | None = None,
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
                    f"SQL preparation failed for the following code: {statement} {e}"
                ) from e
            try:
                logger.debug(f"Executing SQL statement: {statement}")
                cursor.execute(statement)
            except (psycopg.errors.SyntaxError, psycopg.errors.ProgrammingError) as e:
                raise PumSqlError(
                    f"SQL execution failed for the following code: {statement} {e}"
                ) from e
        if commit:
            connection.commit()

        return cursor

    def _prepare_sql(self, parameters: dict | None) -> list[psycopg.sql.SQL]:
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
            logger.info(
                f"Checking SQL from file: {self.sql} with parameters: {parameters}",
            )
            sql_code = sql_chunks_from_file(self.sql)
        elif isinstance(self.sql, str):
            sql_code = [psycopg.sql.SQL(self.sql)]
        else:
            sql_code = [self.sql]

        def format_sql(
            statement: psycopg.sql.SQL, parameters: dict | None = None
        ) -> psycopg.sql.SQL:
            for key, value in (parameters or {}).items():
                if (
                    not isinstance(value, psycopg.sql.Literal)
                    and not isinstance(value, psycopg.sql.Identifier)
                    and not isinstance(value, psycopg.sql.Composed)
                ):
                    raise PumSqlError(
                        f"Invalid parameter type for key '{key}': {type(value)}. "
                        "Parameters must be psycopg.sql.Literal or psycopg.sql.Identifier."
                    )
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

    @staticmethod
    def prepare_parameters(parameters: dict | None):
        """
        Prepares a dictionary of parameters for use in SQL queries by converting each value to a psycopg.sql.Literal.

        Args:
            parameters: A dictionary of parameters to be converted, or None.

        Returns:
            dict: A new dictionary with the same keys as `parameters`, where each value is wrapped in psycopg.sql.Literal.
        """
        parameters_literals = {}
        if parameters:
            for key, value in parameters.items():
                parameters_literals[key] = psycopg.sql.Literal(value)
        return parameters_literals
