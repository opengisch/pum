from enum import Enum


class ParameterType(Enum):
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


class ParameterDefinition:
    """A class to define a migration parameter."""

    def __init__(
        self,
        name: str,
        type_: str | ParameterType,
        default: str | float | None = None,
        description: str | None = None,
    ) -> None:
        """Initialize a ParameterDefintion instance.

        Args:
            name: The name of the parameter.
            type_: The type of the parameter, as a string or ParameterType.
            default: The default value for the parameter. Defaults to None.
            description: A description of the parameter. Defaults to None.

        Raises:
            ValueError: If type is a string and not a valid ParameterType.
            TypeError: If type is not a string or ParameterType.

        """
        self.name = name
        if isinstance(type, ParameterType):
            self.type = type_
        elif isinstance(type_, str):
            try:
                self.type = ParameterType(type_)
            except ValueError:
                raise ValueError(f"Invalid parameter type: {type_}") from None
        else:
            raise TypeError("type_ must be a str or ParameterType")
        self.default = default
        self.description = description

    def __repr__(self) -> str:
        """Return a string representation of the ParameterDefinition instance."""
        return f"Parameter({self.name}, type: {self.type}, default: {self.default})"

    def __eq__(self, other: "ParameterDefinition") -> bool:
        """Check if two ParameterDefinition instances are equal."""
        if not isinstance(other, ParameterDefinition):
            return NotImplemented
        return (
            self.name == other.name
            and self.type == other.type
            and self.default == other.default
            and self.description == other.description
        )
