import importlib.util
import inspect
import logging
import sys
import psycopg
import copy

from pathlib import Path

from .exceptions import PumHookError, PumSqlError
from .sql_content import SqlContent
import abc

logger = logging.getLogger(__name__)


class HookBase(abc.ABC):
    """Base class for Python migration hooks.
    This class defines the interface for migration hooks that can be implemented in Python.
    It requires the implementation of the `run_hook` method, which will be called during the migration process.
    It can call the execute method to run SQL statements with the provided connection and parameters.
    """

    def __init__(self) -> None:
        """Initialize the HookBase class."""
        self._parameters: dict | None = None

    def _prepare(self, connection: psycopg.Connection, parameters: dict | None = None) -> None:
        """Prepare the hook with the given connection and parameters.
        Args:
            connection: The database connection.
            parameters: Parameters to bind to the SQL statement. Defaults to None.
        Note:
            Parameters are stored as a deep copy, any modification will not be used when calling execute.
        """
        self._connection = connection
        self._parameters = copy.deepcopy(parameters)

    @abc.abstractmethod
    def run_hook(self, connection: psycopg.Connection, parameters: dict | None = None) -> None:
        """Run the migration hook.
        Args:
            connection: The database connection.
            parameters: Parameters to bind to the SQL statement. Defaults to None.

        Note:
            Parameters are given as a deep copy, any modification will not be used when calling execute.
        """
        raise NotImplementedError("The run_hook method must be implemented in the subclass.")

    def execute(
        self,
        sql: str | psycopg.sql.SQL | Path,
    ) -> None:
        """Execute the migration hook with the provided SQL and parameters for the migration.
        This is not committing the transaction and will be handled afterwards.

        Args:
            connection: The database connection.
            sql: The SQL statement to execute or a path to a SQL file..
        """
        parameters_literals = SqlContent.prepare_parameters(self._parameters)
        SqlContent(sql).execute(
            connection=self._connection, parameters=parameters_literals, commit=False
        )

    execute.__isfinal__ = True


class HookHandler:
    """Handler for migration hooks.
    This class manages the execution of migration hooks, which can be either SQL files or Python functions."""

    def __init__(
        self,
        *,
        file: str | Path | None = None,
        code: str | None = None,
        base_path: Path | None = None,
    ) -> None:
        """Initialize a Hook instance.

        Args:
            type: The type of the hook (e.g., "pre", "post").
            file: The file path of the hook.
            code: The SQL code for the hook.

        """
        if file and code:
            raise ValueError("Cannot specify both file and code. Choose one.")

        self.file = file
        self.code = code
        self.hook_instance = None

        if file:
            if isinstance(file, str):
                self.file = Path(file)
            if not self.file.is_absolute():
                if base_path is None:
                    raise ValueError("Base path must be provided for relative file paths.")
                self.file = base_path.absolute() / self.file
            if not self.file.exists():
                raise PumHookError(f"Hook file {self.file} does not exist.")
            if not self.file.is_file():
                raise PumHookError(f"Hook file {self.file} is not a file.")

        if self.file and self.file.suffix == ".py":
            # Support local imports in hook files by adding parent dir to sys.path
            parent_dir = str(self.file.parent.resolve())
            sys_path_modified = False
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
                sys_path_modified = True
            try:
                spec = importlib.util.spec_from_file_location(self.file.stem, self.file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
            finally:
                if sys_path_modified:
                    sys.path.remove(parent_dir)
            # Check that the module contains a class named Hook inheriting from HookBase
            hook_class = getattr(module, "Hook", None)
            if not hook_class or not inspect.isclass(hook_class):
                raise PumHookError(
                    f"Python hook file {self.file} must define a class named 'Hook'."
                )
            if not issubclass(hook_class, HookBase):
                raise PumHookError(f"Class 'Hook' in {self.file} must inherit from HookBase.")
            if not hasattr(hook_class, "run_hook"):
                raise PumHookError(f"Hook function 'run_hook' not found in {self.file}.")

            self.hook_instance = hook_class()
            arg_names = list(inspect.signature(hook_class.run_hook).parameters.keys())
            if "connection" not in arg_names:
                raise PumHookError(
                    f"Hook function 'run_hook' in {self.file} must accept 'connection' as an argument."
                )
            self.parameter_args = [arg for arg in arg_names if arg not in ("self", "connection")]

    def __repr__(self) -> str:
        """Return a string representation of the Hook instance."""
        return f"<hook: {self.file}>"

    def __eq__(self, other: "HookHandler") -> bool:
        """Check if two Hook instances are equal."""
        if not isinstance(other, HookHandler):
            return NotImplemented
        return (not self.file or self.file == other.file) and (
            not self.code or self.code == other.code
        )

    def validate(self, parameters: dict) -> None:
        """Check if the parameters match the expected parameter definitions.
        This is only effective for Python hooks for now.

        Args:
            parameters (dict): The parameters to check.

        Raises:
            PumHookError: If the parameters do not match the expected definitions.

        """
        if self.file and self.file.suffix == ".py":
            for parameter_arg in self.parameter_args:
                if parameter_arg not in parameters:
                    raise PumHookError(
                        f"Hook function 'run_hook' in {self.file} has an unexpected argument "
                        f"'{parameter_arg}' which is not specified in the parameters."
                    )

        if self.file and self.file.suffix == ".sql":
            SqlContent(self.file).validate(parameters=parameters)

    def execute(
        self,
        connection: psycopg.Connection,
        *,
        commit: bool = False,
        parameters: dict | None = None,
    ) -> None:
        """Execute the migration hook.
        This method executes the SQL code or the Python file specified in the hook.

        Args:
            connection: The database connection.
            commit: Whether to commit the transaction after executing the SQL.
            parameters (dict, optional): Parameters to bind to the SQL statement. Defaults to ().

        """
        logger.info(
            f"Executing hook from file: {self.file} or SQL code with parameters: {parameters}",
        )

        parameters_literals = SqlContent.prepare_parameters(parameters)

        if self.file is None and self.code is None:
            raise ValueError("No file or SQL code specified for the migration hook.")

        if self.file:
            if self.file.suffix == ".sql":
                SqlContent(self.file).execute(
                    connection=connection, commit=False, parameters=parameters_literals
                )
            elif self.file.suffix == ".py":
                for parameter_arg in self.parameter_args:
                    if not parameters or parameter_arg not in self.parameter_args:
                        raise PumHookError(
                            f"Hook function 'run_hook' in {self.file} has an unexpected "
                            f"argument '{parameter_arg}' which is not specified in the parameters."
                        )

                _hook_parameters = {}
                if parameters:
                    for key, value in parameters.items():
                        if key in self.parameter_args:
                            _hook_parameters[key] = value
                self.hook_instance._prepare(connection=connection, parameters=parameters)
                try:
                    if _hook_parameters:
                        self.hook_instance.run_hook(connection=connection, **_hook_parameters)
                    else:
                        self.hook_instance.run_hook(connection=connection)
                except PumSqlError as e:
                    raise PumHookError(f"Error executing Python hook from {self.file}: {e}") from e

            else:
                raise PumHookError(
                    f"Unsupported file type for migration hook: {self.file.suffix}. Only .sql and .py files are supported."
                )
        elif self.code:
            SqlContent(self.code).execute(connection, parameters=parameters_literals, commit=False)

        if commit:
            connection.commit()
