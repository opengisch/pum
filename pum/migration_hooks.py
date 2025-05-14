from enum import Enum
from pathlib import Path
from psycopg import Connection
from .utils.execute_sql import execute_sql
import logging

logger = logging.getLogger(__name__)


class MigrationHookType(Enum):
    """
    Enum for migration hook types.
    """

    PRE = "pre"
    POST = "post"


class MigrationHook:
    """
    Base class for migration hooks.
    """

    def __init__(self, type: str | MigrationHookType, file: str | Path | None = None):
        """
        Initialize a MigrationHook instance.

        Args:
            type (str): The type of the hook (e.g., "pre", "post").
            file (str): The file path of the hook.
        """
        self.type = type if isinstance(type, MigrationHookType) else MigrationHookType(type)
        self.file = file

    def __repr__(self):
        return f"<{self.type.value} hook: {self.file}>"

    def __eq__(self, other):
        if not isinstance(other, MigrationHook):
            return NotImplemented
        return self.type == other.type and self.file == other.file

    def execute_sql(
        self,
        conn: Connection,
        dir: str | Path = ".",
        commit: bool = False,
        parameters: dict | None = None,
    ):
        """
        Execute the SQL file associated with the migration hook.

        Args:
            conn: The database connection.
            commit: Whether to commit the transaction after executing the SQL.
            dir: The root directory of the project.
            parameters (dict, optional): Parameters to bind to the SQL statement. Defaults to ().
        """

        logger.info(
            f"Executing {self.type.value} hook from file: {self.file} with parameters: {parameters}",
        )

        if self.file is None:
            raise ValueError("No file specified for the migration hook.")

        path = Path(dir) / self.file
        execute_sql(conn=conn, sql=path, commit=False, parameters=parameters)
        if commit:
            conn.commit()
