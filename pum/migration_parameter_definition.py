from enum import Enum

class ParameterType(Enum):
    INTEGER = "integer"
    STRING = "string"
    DECIMAL = "decimal"

class MigrationParameterDefintion:
    """
    A class to define a migration parameter.
    """

    def __init__(self, name: str, type_: str | ParameterType, default: str | int | float = None, description: str = None):
        """
        Initialize a MigrationParameterDefintion instance.

        Args:
            name (str): The name of the parameter.
            type_ (str | ParameterType): The type of the parameter, as a string or ParameterType.
            default (str | int | float, optional): The default value for the parameter. Defaults to None.
            description (str, optional): A description of the parameter. Defaults to None.

        Raises:
            ValueError: If type_ is a string and not a valid ParameterType.
            TypeError: If type_ is not a string or ParameterType.
        """
        self.name = name
        if isinstance(type_, ParameterType):
            self.type = type_
        elif isinstance(type_, str):
            try:
                self.type = ParameterType(type_)
            except ValueError:
                raise ValueError(f"Invalid parameter type: {type_}")
        else:
            raise TypeError("type_ must be a str or ParameterType")
        self.default = default
        self.description = description

    def __repr__(self):
        return f"MigrationParameter(name={self.name}, type={self.type}, default={self.default}, description={self.description})"
    
    def __eq__(self, other):
        if not isinstance(other, MigrationParameterDefintion):
            return NotImplemented
        return (
            self.name == other.name and
            self.type == other.type and
            self.default == other.default and
            self.description == other.description
        )