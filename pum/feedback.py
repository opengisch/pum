"""Feedback system for reporting progress and handling cancellation during operations."""

import abc
import logging

logger = logging.getLogger(__name__)


class Feedback(abc.ABC):
    """Base class for feedback during install/upgrade operations.

    This class provides methods for progress reporting and cancellation handling.
    Subclasses should implement the abstract methods to provide custom feedback mechanisms.
    """

    def __init__(self) -> None:
        """Initialize the feedback instance."""
        self._is_cancelled = False
        self._cancellation_locked = False
        self._current_step = 0
        self._total_steps = 0

    def set_total_steps(self, total: int) -> None:
        """Set the total number of steps for the operation.

        Args:
            total: The total number of steps.
        """
        self._total_steps = total
        self._current_step = 0

    def increment_step(self) -> None:
        """Increment the current step counter."""
        self._current_step += 1

    def get_progress(self) -> tuple[int, int]:
        """Get the current progress.

        Returns:
            A tuple of (current_step, total_steps).
        """
        return (self._current_step, self._total_steps)

    @abc.abstractmethod
    def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
        """Report progress during an operation.

        Args:
            message: A message describing the current operation.
            current: The current progress value (e.g., changelog number).
            total: The total number of steps.
        """
        pass

    def is_cancelled(self) -> bool:
        """Check if the operation has been cancelled.

        Returns:
            True if the operation should be cancelled, False otherwise.
            Always returns False if cancellation has been locked (after commit).
        """
        if self._cancellation_locked:
            return False
        return self._is_cancelled

    def cancel(self) -> None:
        """Cancel the operation.

        Note: This will have no effect if cancellation has been locked (after commit).
        """
        if not self._cancellation_locked:
            self._is_cancelled = True

    def reset(self) -> None:
        """Reset the cancellation status."""
        self._is_cancelled = False

    def lock_cancellation(self) -> None:
        """Lock cancellation to prevent it after a commit.

        Once locked, is_cancelled() will always return False and cancel() will have no effect.
        This should be called immediately before committing database changes.
        """
        self._cancellation_locked = True


class LogFeedback(Feedback):
    """Feedback implementation that logs progress messages.

    This is the default feedback implementation that simply logs messages
    without any UI interaction.
    """

    def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
        """Report progress by logging the message.

        Args:
            message: A message describing the current operation.
            current: The current progress value (ignored, uses internal counter).
            total: The total number of steps (ignored, uses internal counter).
        """
        logger.info(message)


class SilentFeedback(Feedback):
    """Feedback implementation that does nothing.

    This can be used when no feedback is desired.
    """

    def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
        """Do nothing - silent feedback.

        Args:
            message: A message describing the current operation (ignored).
            current: The current progress value (ignored).
            total: The total number of steps (ignored).
        """
        pass
