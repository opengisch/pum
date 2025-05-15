from pathlib import Path
from ..exceptions import PumInvalidSqlFile
import re


def sql_chunks_from_file(file: str | Path, parameters=None):
    """
    Read SQL from a file, remove comments, and split into chunks.

    Args:
        file (str | Path): Path to the SQL file.
        parameters (dict, optional): Dictionary of parameters to replace in the SQL.
            Defaults to None.
    Returns:
        list: List of SQL statements.
    Raises:
        PumInvalidSqlFile: If the SQL file contains forbidden transaction statements.
    """
    file = Path(file) if not isinstance(file, Path) else file
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
                raise PumInvalidSqlFile(
                    f"SQL contains forbidden transaction statement: {forbidden}"
                )

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
