from enum import Enum


class MigrationParameterType(Enum):
    """
    An enumeration of parameter types.
    This class defines the types of parameters that can be used in migration definitions.

    Attributes:
        BOOLEAN (str): Represents a boolean parameter type.
        INTEGER (str): Represents an integer parameter type.
        STRING (str): Represents a string parameter type.
        DECIMAL (str): Represents a decimal parameter type.
    """

    BOOLEAN = "boolean"
    INTEGER = "integer"
    STRING = "string"
    DECIMAL = "decimal"


class MigrationParameterDefinition:
    """
    A class to define a migration parameter.
    """

    def __init__(
        self,
        name: str,
        type_: str | MigrationParameterType,
        default: str | int | float = None,
        description: str = None,
    ):
        """
        Initialize a MigrationParameterDefintion instance.

        Args:
            name (str): The name of the parameter.
            type_ (str | MigrationParameterType): The type of the parameter, as a string or MigrationParameterType.
            default (str | int | float, optional): The default value for the parameter. Defaults to None.
            description (str, optional): A description of the parameter. Defaults to None.

        Raises:
            ValueError: If type_ is a string and not a valid MigrationParameterType.
            TypeError: If type_ is not a string or ParameterType.
        """
        self.name = name
        if isinstance(type_, MigrationParameterType):
            self.type = type_
        elif isinstance(type_, str):
            try:
                self.type = MigrationParameterType(type_)
            except ValueError:
                raise ValueError(f"Invalid parameter type: {type_}")
        else:
            raise TypeError("type_ must be a str or MigrationParameterType")
        self.default = default
        self.description = description

    def __repr__(self):
        return f"MigrationParameter(name={self.name}, type={self.type}, default={self.default}, description={self.description})"

    def __eq__(self, other):
        if not isinstance(other, MigrationParameterDefinition):
            return NotImplemented
        return (
            self.name == other.name
            and self.type == other.type
            and self.default == other.default
            and self.description == other.description
        )
