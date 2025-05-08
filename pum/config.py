import os

import yaml
from .migration_parameter_definition import MigrationParameterDefintion


class PumConfig:
    """
    A class to hold configuration settings.
    """

    def __init__(self, **kwargs):
        """
        Initialize the configuration with key-value pairs.
        """
        self.pg_restore_exe: str | None = kwargs.get("pg_restore_exe") or os.getenv(
            "PG_RESTORE_EXE"
        )
        self.pg_dump_exe: str | None = kwargs.get("pg_dump_exe") or os.getenv("PG_DUMP_EXE")

        self.schema_migrations_table: str = (
            kwargs.get("schema_migrations_table") or "public.pum_migrations"
        )
        self.changelogs_directory: str = kwargs.get("changelogs_directory") or "changelogs"

        self.parameter_definitions = dict()
        for p in kwargs.get("parameters") or ():
            if isinstance(p, dict):
                name = p.get("name")
                type_ = p.get("type")
                default = p.get("default")
                description = p.get("description")
                self.parameter_definitions[name] = MigrationParameterDefintion(
                    name=name,
                    type_=type_,
                    default=default,
                    description=description,
                )
            elif isinstance(p, MigrationParameterDefintion):
                self.parameter_definitions[p.name] = p
            else:
                raise TypeError(
                    "parameters must be a list of dictionaries or MigrationParameterDefintion instances"
                )

    def get(self, key, default=None):
        """
        Get a configuration value by key, with an optional default.
        """
        return getattr(self, key, default)

    def set(self, key, value):
        """
        Set a configuration value by key.
        """
        setattr(self, key, value)

    def parameters(self):
        """
        Get all changelogs parameters as a dictionary.
        """
        return self.parameter_definitions

    def parameter(self, name):
        """
        Get a specific changelog parameter by name.
        """
        return self.parameter_definitions[name]

    @classmethod
    def from_yaml(cls, file_path):
        """
        Create a PumConfig instance from a YAML file.
        """

        with open(file_path) as file:
            data = yaml.safe_load(file)
        return cls(**data)
