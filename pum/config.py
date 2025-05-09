import yaml
from .migration_parameter import MigrationParameterDefinition


class PumConfig:
    """
    A class to hold configuration settings.
    """

    def __init__(self, **kwargs):
        """
        Initialize the configuration with key-value pairs.

        Args:
            **kwargs: Key-value pairs representing configuration settings.
        """
        # self.pg_restore_exe: str | None = kwargs.get("pg_restore_exe") or os.getenv(
        #     "PG_RESTORE_EXE"
        # )
        # self.pg_dump_exe: str | None = kwargs.get("pg_dump_exe") or os.getenv("PG_DUMP_EXE")

        self.pum_migrations_table: str = (
            f"{(kwargs.get('pum_migrations_schema') or 'public')}.pum_migrations"
        )
        self.changelogs_directory: str = kwargs.get("changelogs_directory") or "changelogs"

        self.parameter_definitions = dict()
        for p in kwargs.get("parameters") or ():
            if isinstance(p, dict):
                name = p.get("name")
                type_ = p.get("type")
                default = p.get("default")
                description = p.get("description")
                self.parameter_definitions[name] = MigrationParameterDefinition(
                    name=name,
                    type_=type_,
                    default=default,
                    description=description,
                )
            elif isinstance(p, MigrationParameterDefinition):
                self.parameter_definitions[p.name] = p
            else:
                raise TypeError(
                    "parameters must be a list of dictionaries or MigrationParameterDefintion instances"
                )

    # def get(self, key, default=None) -> any:
    #     """
    #     Get a configuration value by key, with an optional default.
    #     This method allows dynamic retrieval of attributes from the PumConfig instance.
    #     Args:
    #         key (str): The name of the attribute to retrieve.
    #         default: The default value to return if the attribute does not exist.
    #     Returns:
    #         any: The value of the attribute, or the default value if the attribute does not exist.
    #     """
    #     return getattr(self, key, default)

    # def set(self, key, value):
    #     """
    #     Set a configuration value by key.
    #     This method allows dynamic setting of attributes on the PumConfig instance.

    #     Args:
    #         key (str): The name of the attribute to set.
    #         value: The value to assign to the attribute.
    #     Raises:
    #         AttributeError: If the attribute does not exist.
    #     """
    #     setattr(self, key, value)

    def parameters(self) -> dict[str, MigrationParameterDefinition]:
        """
        Get all migration parameters as a dictionary.

        Returns:
            dict[str, MigrationParameterDefintion]: A dictionary of migration parameters.
        The keys are parameter names, and the values are MigrationParameterDefintion instances.
        """
        return self.parameter_definitions

    def parameter(self, name) -> MigrationParameterDefinition:
        """
        Get a specific migration parameter by name.

        Returns:
            MigrationParameterDefintion: The migration parameter definition.
        Raises:
            KeyError: If the parameter name does not exist.
        """
        return self.parameter_definitions[name]

    @classmethod
    def from_yaml(cls, file_path):
        """
        Create a PumConfig instance from a YAML file.
        Args:
            file_path (str): The path to the YAML file.
        Returns:
            PumConfig: An instance of the PumConfig class.
        Raises:
            FileNotFoundError: If the file does not exist.
            yaml.YAMLError: If there is an error parsing the YAML file.
        """

        with open(file_path) as file:
            data = yaml.safe_load(file)
        return cls(**data)
