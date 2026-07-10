import logging
import re
from typing import Any, Optional

LOGGER = logging.getLogger(__name__)


class TextValidationError(ValueError):
    """Raised when text validation fails."""


class TextProcessor:
    """
    Validates text values using an optional regex defined on a parameter definition.
      - extracting regex/name/type from a parameter definition (if provided)
      - compiling regex defensively
      - validating values (no transformation)
    """

    def __init__(
        self,
        *,
        parameter: Optional[Any] = None,
        regex: Optional[str] = None,
        parameter_name: Optional[str] = None,
        parameter_type: Optional[str] = None,
    ) -> None:
        self._parameter_name = parameter_name
        self._parameter_type = parameter_type
        self._pattern_str = regex

        if parameter is not None:
            self._parameter_name = self._safe_get_attr(parameter, "name", self._parameter_name)
            self._parameter_type = self._safe_get_attr(parameter, "type", self._parameter_type)
            self._pattern_str = self._safe_get_attr(parameter, "regex", self._pattern_str)

        self._compiled_regex = self._compile(self._pattern_str)

    @staticmethod
    def _safe_get_attr(obj: Any, attr: str, default: Any) -> Any:
        try:
            return getattr(obj, attr, default)
        except Exception:
            return default

    @staticmethod
    def _compile(pattern: Optional[str]) -> Optional[re.Pattern]:
        if not pattern:
            return None

        try:
            return re.compile(pattern)
        except re.error as exc:
            LOGGER.error("Invalid regex pattern: %s", exc)
            raise TextValidationError(f"Invalid regex pattern: {pattern}") from exc

    def validate(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value

        if self._parameter_type is not None and self._parameter_type != "text":
            return value

        if not isinstance(value, str):
            raise TextValidationError(self._format_error("Value must be a string"))

        if not self._compiled_regex:
            return value

        try:
            if self._compiled_regex.search(value) is None:
                raise TextValidationError(
                    self._format_error(
                        f"Value '{value}' does not match regex '{self._pattern_str}'"
                    )
                )
            return value
        except TextValidationError:
            raise
        except Exception as exc:
            LOGGER.exception("Unexpected validation failure: %s", exc)
            raise TextValidationError(self._format_error("Unexpected validation failure")) from exc

    def _format_error(self, message: str) -> str:
        if self._parameter_name:
            return f"Parameter '{self._parameter_name}': {message}"
