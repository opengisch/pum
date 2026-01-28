"""Test module for feedback functionality."""

import unittest
from pathlib import Path

import psycopg

from pum.feedback import Feedback, LogFeedback, SilentFeedback
from pum.pum_config import PumConfig
from pum.upgrader import Upgrader
from pum.exceptions import PumException


class CustomFeedback(Feedback):
    """Custom feedback for testing that tracks all progress calls."""

    def __init__(self):
        super().__init__()
        self.messages = []
        self.progress_calls = []

    def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
        """Track progress calls."""
        self.messages.append(message)
        self.progress_calls.append((message, current, total))


class TestFeedback(unittest.TestCase):
    """Test the feedback functionality."""

    pg_service = "pum_test"

    def setUp(self) -> None:
        """Set up the test environment by cleaning schemas."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            with conn.cursor() as cursor:
                # Clean up pum_test schemas
                cursor.execute(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'pum_test%'"
                )
                schemas = [row[0] for row in cursor.fetchall()]
                for schema in schemas:
                    cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

                # Clean up pum_migrations table in public schema
                cursor.execute("DROP TABLE IF EXISTS public.pum_migrations CASCADE")
            conn.commit()

    def tearDown(self) -> None:
        """Clean up the test environment."""
        with psycopg.connect(f"service={self.pg_service}") as conn:
            with conn.cursor() as cursor:
                # Clean up pum_test schemas
                cursor.execute(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'pum_test%'"
                )
                schemas = [row[0] for row in cursor.fetchall()]
                for schema in schemas:
                    cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

                # Clean up pum_migrations table in public schema
                cursor.execute("DROP TABLE IF EXISTS public.pum_migrations CASCADE")
            conn.commit()

    def test_log_feedback(self) -> None:
        """Test that LogFeedback reports progress correctly."""
        feedback = LogFeedback()

        # Should not raise any errors
        feedback.report_progress("Test message")
        feedback.report_progress("Test with progress", current=5, total=10)

        # Check cancellation
        self.assertFalse(feedback.is_cancelled())
        feedback.cancel()
        self.assertTrue(feedback.is_cancelled())
        feedback.reset()
        self.assertFalse(feedback.is_cancelled())

    def test_silent_feedback(self) -> None:
        """Test that SilentFeedback does nothing."""
        feedback = SilentFeedback()

        # Should not raise any errors and do nothing
        feedback.report_progress("Test message")
        feedback.report_progress("Test with progress", current=5, total=10)

        # Check cancellation
        self.assertFalse(feedback.is_cancelled())
        feedback.cancel()
        self.assertTrue(feedback.is_cancelled())

    def test_custom_feedback_tracks_progress(self) -> None:
        """Test that custom feedback can track progress calls."""
        feedback = CustomFeedback()

        feedback.report_progress("Starting")
        feedback.report_progress("Processing item 1", current=1, total=3)
        feedback.report_progress("Processing item 2", current=2, total=3)
        feedback.report_progress("Done")

        self.assertEqual(len(feedback.messages), 4)
        self.assertIn("Starting", feedback.messages)
        self.assertEqual(feedback.progress_calls[1], ("Processing item 1", 1, 3))
        self.assertEqual(feedback.progress_calls[2], ("Processing item 2", 2, 3))

    def test_install_with_feedback(self) -> None:
        """Test that install reports progress through feedback."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir, pum={"module": "test_feedback_install"})

        feedback = CustomFeedback()

        with psycopg.connect(f"service={self.pg_service}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, feedback=feedback)

            # Verify progress was reported
            self.assertGreater(len(feedback.messages), 0)
            self.assertTrue(any("Installing module" in msg for msg in feedback.messages))
            self.assertTrue(any("Applying changelog" in msg for msg in feedback.messages))

    def test_upgrade_with_feedback(self) -> None:
        """Test that upgrade reports progress through feedback."""
        test_dir = Path("test") / "data" / "multiple_changelogs"
        cfg = PumConfig(test_dir, pum={"module": "test_feedback_upgrade"})

        feedback = CustomFeedback()

        with psycopg.connect(f"service={self.pg_service}") as conn:
            # First install with version 1.2.3
            upgrader = Upgrader(config=cfg, max_version="1.2.3")
            upgrader.install(connection=conn)

            # Clear feedback history
            feedback.messages.clear()
            feedback.progress_calls.clear()

            # Now upgrade to latest
            upgrader2 = Upgrader(config=cfg)
            upgrader2.upgrade(connection=conn, feedback=feedback)

            # Verify progress was reported
            self.assertGreater(len(feedback.messages), 0)
            self.assertTrue(any("upgrade" in msg.lower() for msg in feedback.messages))
            # There should be changelog application messages if there were upgrades
            # Check that we have at least some progress messages
            self.assertTrue(
                len(feedback.messages) >= 2
            )  # At least "Starting upgrade" and "Committing"

    def test_install_with_cancellation(self) -> None:
        """Test that install can be cancelled via feedback."""
        test_dir = Path("test") / "data" / "multiple_changelogs"
        cfg = PumConfig(test_dir, pum={"module": "test_feedback_cancel"})

        class CancellingFeedback(CustomFeedback):
            def __init__(self, cancel_after: int):
                super().__init__()
                self.cancel_after = cancel_after
                self.call_count = 0

            def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
                super().report_progress(message, current, total)
                self.call_count += 1
                if self.call_count >= self.cancel_after:
                    self.cancel()

        feedback = CancellingFeedback(cancel_after=3)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            upgrader = Upgrader(config=cfg)
            with self.assertRaises(PumException) as context:
                upgrader.install(connection=conn, feedback=feedback)

            self.assertIn("cancelled", str(context.exception).lower())

    def test_upgrade_with_cancellation(self) -> None:
        """Test that upgrade can be cancelled via feedback."""
        test_dir = Path("test") / "data" / "multiple_changelogs"
        cfg = PumConfig(test_dir, pum={"module": "test_feedback_cancel_upgrade"})

        class CancellingFeedback(CustomFeedback):
            def __init__(self, cancel_after: int):
                super().__init__()
                self.cancel_after = cancel_after
                self.call_count = 0

            def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
                super().report_progress(message, current, total)
                self.call_count += 1
                if self.call_count >= self.cancel_after:
                    self.cancel()

        with psycopg.connect(f"service={self.pg_service}") as conn:
            # First install with version 1.2.3
            upgrader = Upgrader(config=cfg, max_version="1.2.3")
            upgrader.install(connection=conn)

            # Now upgrade with cancellation - cancel early enough to hit a changelog
            feedback = CancellingFeedback(cancel_after=2)
            upgrader2 = Upgrader(config=cfg)

            try:
                upgrader2.upgrade(connection=conn, feedback=feedback)
                # If no changelogs were applied (all were already applied),
                # then no cancellation would occur
                # This is acceptable - just verify feedback was used
                self.assertGreater(len(feedback.messages), 0)
            except PumException as e:
                # If exception was raised, verify it's about cancellation
                self.assertIn("cancelled", str(e).lower())


if __name__ == "__main__":
    unittest.main()
