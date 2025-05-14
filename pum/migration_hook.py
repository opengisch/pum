from enum import Enum
from pathlib import Path
from psycopg import Connection
from .utils.execute_sql import execute_sql
import logging
from .exceptions import PumHookError
import inspect
import importlib.util

logger = logging.getLogger(__name__)


class MigrationHookType(Enum):
    """
    Enum for migration hook types.

    Attributes:
        PRE (str): Pre-migration hook.
        POST (str): Post-migration hook.
    """

    PRE = "pre"
    POST = "post"


class MigrationHook:
    """
    Base class for migration hooks.
    """

    def __init__(
        self, type: str | MigrationHookType, file: str | Path | None = None, code: str | None = None
    ):
        """
        Initialize a MigrationHook instance.

        Args:
            type (str): The type of the hook (e.g., "pre", "post").
            file (str): The file path of the hook.
            code (str): The SQL code for the hook.
        """
        if file and code:
            raise ValueError("Cannot specify both file and code. Choose one.")

        self.type = type if isinstance(type, MigrationHookType) else MigrationHookType(type)
        self.file = file
        self.code = code

    def __repr__(self):
        return f"<{self.type.value} hook: {self.file}>"

    def __eq__(self, other):
        if not isinstance(other, MigrationHook):
            return NotImplemented
        return self.type == other.type and self.file == other.file

    def execute(
        self,
        connection: Connection,
        dir: str | Path = ".",
        commit: bool = False,
        parameters: dict | None = None,
    ):
        """
        Execute the migration hook.
        This method executes the SQL code or the Python file specified in the hook.

        Args:
            connection: The database connection.
            commit: Whether to commit the transaction after executing the SQL.
            dir: The root directory of the project.
            parameters (dict, optional): Parameters to bind to the SQL statement. Defaults to ().
        """

        logger.info(
            f"Executing {self.type.value} hook from file: {self.file} or SQL code with parameters: {parameters}",
        )

        if self.file is None and self.code is None:
            raise ValueError("No file or SQL code specified for the migration hook.")

        if self.file:
            path = Path(dir) / self.file
            if path.suffix == ".sql":
                execute_sql(connection=connection, sql=path, commit=False, parameters=parameters)
            elif path.suffix == ".py":
                if path.is_file():
                    spec = importlib.util.spec_from_file_location(path.stem, path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    if hasattr(module, "run_hook"):
                        run_hook = getattr(module, "run_hook")
                        if callable(run_hook):
                            # Get the list of argument names for run_hook
                            arg_names = list(inspect.signature(run_hook).parameters.keys())
                            if "connection" not in arg_names:
                                raise PumHookError(
                                    f"Hook function 'run_hook' in {path} must accept 'connection' as an argument."
                                )
                            for arg in arg_names:
                                if arg == "connection":
                                    continue
                                if not parameters or arg not in parameters.keys():
                                    raise PumHookError(
                                        f"Hook function 'run_hook' in {path} has an unexpected argument '{arg}' which is not specified in the parameters."
                                    )
                            if parameters:
                                run_hook(connection=connection, **parameters)
                            else:
                                run_hook(connection=connection)
                        else:
                            raise PumHookError(
                                f"Hook function 'run_hook' in {path} is not callable."
                            )
                    else:
                        raise PumHookError(f"Hook function 'run_hook' not found in {path}.")
            else:
                raise PumHookError(
                    f"Unsupported file type for migration hook: {path.suffix}. Only .sql and .py files are supported."
                )
        elif self.code:
            execute_sql(connection=connection, sql=self.code, commit=False, parameters=parameters)

        if commit:
            connection.commit()
