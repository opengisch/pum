import os

import yaml


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
        self.pg_dump_exe: str | None = kwargs.get("pg_dump_exe") or os.getenv(
            "PG_DUMP_EXE"
        )

        self.schema_migrations_table: str = (
            kwargs.get("schema_migrations_table") or "public.pum_migrations"
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

    @classmethod
    def from_yaml(cls, file_path):
        """
        Create a PumConfig instance from a YAML file.
        """

        with open(file_path) as file:
            data = yaml.safe_load(file)
        return cls(**data)
