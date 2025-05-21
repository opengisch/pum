from enum import Enum


class MigrationParameterType(Enum):
    """An enumeration of parameter types.
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
    """A class to define a migration parameter."""

    def __init__(
        self,
        name: str,
        type_: str | MigrationParameterType,
        default: str | float | None = None,
        description: str | None = None,
    ) -> None:
        """Initialize a MigrationParameterDefintion instance.

        Args:
            name: The name of the parameter.
            type_: The type of the parameter, as a string or MigrationParameterType.
            default: The default value for the parameter. Defaults to None.
            description: A description of the parameter. Defaults to None.

        Raises:
            ValueError: If type is a string and not a valid MigrationParameterType.
            TypeError: If type is not a string or ParameterType.

        """
        self.name = name
        if isinstance(type, MigrationParameterType):
            self.type = type_
        elif isinstance(type_, str):
            try:
                self.type = MigrationParameterType(type_)
            except ValueError:
                raise ValueError(f"Invalid parameter type: {type_}") from None
        else:
            raise TypeError("type_ must be a str or MigrationParameterType")
        self.default = default
        self.description = description

    def __repr__(self) -> str:
        """Return a string representation of the MigrationParameterDefinition instance."""
        return f"MigrationParameter({self.name}, type: {self.type}, default: {self.default})"

    def __eq__(self, other: "MigrationParameterDefinition") -> bool:
        """Check if two MigrationParameterDefinition instances are equal."""
        if not isinstance(other, MigrationParameterDefinition):
            return NotImplemented
        return (
            self.name == other.name
            and self.type == other.type
            and self.default == other.default
            and self.description == other.description
        )
