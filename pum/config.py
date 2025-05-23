from pathlib import Path

import yaml
from packaging.version import parse as parse_version

from .changelog import Changelog
from .exceptions import PumConfigError, PumException, PumHookError, PumInvalidChangelog, PumSqlError
from .hook import Hook, HookType
from .parameter import ParameterDefinition


class PumConfig:
    """A class to hold configuration settings."""

    def __init__(self, dir: str | Path, validate: bool = True, **kwargs: dict) -> None:  # noqa: C901, FBT001, FBT002, PLR0912
        """Initialize the configuration with key-value pairs.

        Args:
            dir: The directory where the changelogs are located.
            validate: Whether to validate the changelogs and hooks.
            **kwargs: Key-value pairs representing configuration settings.

        Raises:
            PumConfigError: If the configuration is invalid.

        """
        # self.pg_restore_exe: str | None = kwargs.get("pg_restore_exe") or os.getenv(
        #     "PG_RESTORE_EXE"
        # )
        # self.pg_dump_exe: str | None = kwargs.get("pg_dump_exe") or os.getenv("PG_DUMP_EXE")

        self.dir = dir if isinstance(dir, Path) else Path(dir)
        if not self.dir.is_dir():
            raise PumConfigError(f"Directory `{self.dir}` does not exist.")

        self.pum_migrations_table: str = (
            f"{(kwargs.get('pum_migrations_schema') or 'public')}.pum_migrations"
        )
        self.changelogs_directory: str = kwargs.get("changelogs_directory", "changelogs")

        # parameters
        self.parameter_definitions = {}
        for p in kwargs.get("parameters") or ():
            if isinstance(p, dict):
                name = p.get("name")
                type_ = p.get("type")
                default = p.get("default")
                description = p.get("description")
                self.parameter_definitions[name] = ParameterDefinition(
                    name=name,
                    type_=type_,
                    default=default,
                    description=description,
                )
            elif isinstance(p, ParameterDefinition):
                self.parameter_definitions[p.name] = p
            else:
                raise PumConfigError(
                    "parameters must be a list of dictionaries or ParameterDefintion instances"
                )

        # Migration hooks
        self.pre_hooks = []
        self.post_hooks = []
        migration_hooks = kwargs.get("migration_hooks", {})
        pre_hook_defintions = migration_hooks.get("pre", [])
        post_hook_defintions = migration_hooks.get("post", [])
        for hook_type, hook_definitions in (
            (HookType.PRE, pre_hook_defintions),
            (HookType.POST, post_hook_defintions),
        ):
            if hook_definitions:
                for hook_definition in hook_definitions:
                    hook = None
                    if not isinstance(hook_definition, dict):
                        raise PumConfigError("hook must be a list of key-value pairs")
                    if isinstance(hook_definition.get("file"), str):
                        path = Path(hook_definition.get("file"))
                        if not path.is_absolute():
                            path = self.dir / path
                        if not path.exists():
                            raise PumConfigError(f"hook file {path} does not exist")
                        hook = Hook(type_=hook_type, file=path)
                    elif isinstance(hook_definition.get("code"), str):
                        hook = Hook(type_=hook_type, code=hook_definition.get("code"))
                    else:
                        raise PumConfigError("invalid hook configuration")
                    assert isinstance(hook, Hook)
                    if hook_type == HookType.PRE:
                        self.pre_hooks.append(hook)
                    elif hook_type == HookType.POST:
                        self.post_hooks.append(hook)
                    else:
                        raise PumConfigError(f"Invalid hook type: {hook_type}")

        if validate:
            try:
                self.validate()
            except (PumInvalidChangelog, PumHookError) as e:
                raise PumConfigError(
                    f"Configuration is invalid: {e}. You can disable the validation when constructing the config."
                ) from e

    def parameters(self) -> dict[str, ParameterDefinition]:
        """Get all migration parameters as a dictionary.

        Returns:
            dict[str, ParameterDefintion]: A dictionary of migration parameters.
            The keys are parameter names, and the values are ParameterDefintion instances.

        """
        return self.parameter_definitions

    def parameter(self, name: str) -> ParameterDefinition:
        """Get a specific migration parameter by name.

        Args:
            name: The name of the parameter.

        Returns:
            ParameterDefintion: The migration parameter definition.

        Raises:
            PumConfigError: If the parameter name does not exist.

        """
        try:
            return self.parameter_definitions[name]
        except KeyError:
            raise PumConfigError(f"Parameter '{name}' not found in configuration.") from KeyError

    @classmethod
    def from_yaml(cls, file_path: str | Path, *, validate: bool = True) -> "PumConfig":
        """Create a PumConfig instance from a YAML file.

        Args:
            file_path: The path to the YAML file.
            validate: Whether to validate the changelogs and hooks.

        Returns:
            PumConfig: An instance of the PumConfig class.

        Raises:
            FileNotFoundError: If the file does not exist.
            yaml.YAMLError: If there is an error parsing the YAML file.

        """
        with Path.open(file_path) as file:
            data = yaml.safe_load(file)

        if "dir" in data:
            raise PumConfigError("dir not allowed in configuration instead.")

        dir_ = Path(file_path).parent
        return cls(dir=dir_, validate=validate, **data)

    def last_version(
        self, min_version: str | None = None, max_version: str | None = None
    ) -> str | None:
        """Return the last version of the changelogs.
        The changelogs are sorted by version.

        Args:
            min_version (str | None): The version to start from (inclusive).
            max_version (str | None): The version to end at (inclusive).

        Returns:
            str | None: The last version of the changelogs. If no changelogs are found, None is returned.

        """
        changelogs = self.list_changelogs(min_version, max_version)
        if not changelogs:
            return None
        if min_version:
            changelogs = [c for c in changelogs if c.version >= parse_version(min_version)]
        if max_version:
            changelogs = [c for c in changelogs if c.version <= parse_version(max_version)]
        if not changelogs:
            return None
        return changelogs[-1].version

    def list_changelogs(
        self, min_version: str | None = None, max_version: str | None = None
    ) -> list:
        """Return a list of changelogs.
        The changelogs are sorted by version.

        Args:
            min_version (str | None): The version to start from (inclusive).
            max_version (str | None): The version to end at (inclusive).

        Returns:
            list: A list of changelogs. Each changelog is represented by a Changelog object.

        """
        path = self.dir / self.changelogs_directory
        if not path.is_dir():
            raise PumException(f"Changelogs directory `{path}` does not exist.")
        if not path.iterdir():
            raise PumException(f"Changelogs directory `{path}` is empty.")

        changelogs = [Changelog(d) for d in path.iterdir() if d.is_dir()]

        if min_version:
            changelogs = [c for c in changelogs if c.version >= parse_version(min_version)]
        if max_version:
            changelogs = [c for c in changelogs if c.version <= parse_version(max_version)]

        changelogs.sort(key=lambda c: c.version)
        return changelogs

    def validate(self) -> None:
        """Validate the chanbgelogs and hooks."""
        parameters = {}
        for parameter in self.parameter_definitions.values():
            parameters[parameter.name] = parameter.default

        for changelog in self.list_changelogs():
            try:
                changelog.validate(parameters=parameters)
            except (PumInvalidChangelog, PumSqlError) as e:
                raise PumInvalidChangelog(f"Changelog `{changelog}` is invalid.") from e
        for hook in self.pre_hooks + self.post_hooks:
            try:
                hook.validate(self.parameter_definitions)
            except PumHookError as e:
                raise PumHookError(f"Hook `{hook}` is invalid.") from e
