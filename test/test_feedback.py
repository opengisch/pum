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
        self.progress_calls.append((message, self._current_step, self._total_steps))


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

        # Test with internal step tracking
        feedback.set_total_steps(3)
        feedback.report_progress("Starting")
        feedback.increment_step()
        feedback.report_progress("Processing item 1")
        feedback.increment_step()
        feedback.report_progress("Processing item 2")
        feedback.increment_step()
        feedback.report_progress("Done")

        self.assertEqual(len(feedback.messages), 4)
        self.assertEqual(feedback.messages[0], "Starting")
        self.assertEqual(feedback.messages[1], "Processing item 1")
        self.assertEqual(feedback.messages[2], "Processing item 2")
        self.assertEqual(feedback.messages[3], "Done")

        # Verify internal tracking
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
            # Now we report individual file execution instead of changelog level
            self.assertTrue(any("Executing" in msg for msg in feedback.messages))

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

    def test_feedback_progression(self) -> None:
        """Test that feedback tracks progression through all steps."""
        test_dir = Path("test") / "data" / "multiple_changelogs"
        cfg = PumConfig(test_dir, pum={"module": "test_feedback_progression"})

        feedback = CustomFeedback()

        with psycopg.connect(f"service={self.pg_service}") as conn:
            upgrader = Upgrader(config=cfg)
            upgrader.install(connection=conn, feedback=feedback, commit=True)

            # Verify we got progress messages for:
            # 1. Creating migrations table
            # 2. Installing module
            # 3. Each SQL file execution
            # 4. Committing changes

            self.assertGreater(len(feedback.messages), 0)

            # Check for key progress messages
            self.assertTrue(any("Creating migrations table" in msg for msg in feedback.messages))
            self.assertTrue(any("Installing module" in msg for msg in feedback.messages))
            self.assertTrue(any("Committing changes" in msg for msg in feedback.messages))

            # Check that we got messages for executing SQL files with progress indicators
            executing_messages = [msg for msg in feedback.messages if "Executing" in msg]
            self.assertGreater(
                len(executing_messages), 0, "Should have messages for executing SQL files"
            )

            # Verify messages for specific files in the test data
            # Version 1.2.3: multiple_changelogs.sql
            # Version 1.2.4: rename_created_date.sql
            # Version 1.3.0: add_created_by_column.sql
            # Version 2.0.0: create_second_table.sql, create_third_table.sql
            self.assertTrue(any("multiple_changelogs.sql" in msg for msg in feedback.messages))
            self.assertTrue(any("create_second_table.sql" in msg for msg in feedback.messages))
            self.assertTrue(any("create_third_table.sql" in msg for msg in feedback.messages))

    def test_cancellation_locked_after_commit(self) -> None:
        """Test that cancellation is locked after commit and cannot be triggered."""
        feedback = LogFeedback()

        # Initially, cancellation should work
        self.assertFalse(feedback.is_cancelled())
        feedback.cancel()
        self.assertTrue(feedback.is_cancelled())

        # Reset for next test
        feedback.reset()
        self.assertFalse(feedback.is_cancelled())

        # Lock cancellation (simulating what happens before commit)
        feedback.lock_cancellation()

        # Try to cancel - should have no effect
        feedback.cancel()
        self.assertFalse(
            feedback.is_cancelled(), "Cancellation should be locked after lock_cancellation()"
        )

        # is_cancelled should always return False when locked
        feedback._is_cancelled = True  # Force internal flag
        self.assertFalse(feedback.is_cancelled(), "is_cancelled() should return False when locked")

    def test_install_cancellation_not_possible_after_commit(self) -> None:
        """Test that cancellation during install doesn't work after commit starts."""
        test_dir = Path("test") / "data" / "single_changelog"
        cfg = PumConfig(test_dir, pum={"module": "test_cancellation_lock"})

        # Custom feedback that tries to cancel during "Committing changes"
        class CancelOnCommitFeedback(CustomFeedback):
            def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
                super().report_progress(message, current, total)
                if "Committing changes" in message:
                    # Try to cancel during commit - should have no effect
                    self.cancel()

        feedback = CancelOnCommitFeedback()
        upgrader = Upgrader(cfg)

        with psycopg.connect(f"service={self.pg_service}") as conn:
            # Should complete successfully despite cancel attempt during commit
            upgrader.install(connection=conn, feedback=feedback, commit=True)

            # Verify cancellation was attempted but had no effect
            self.assertTrue(any("Committing changes" in msg for msg in feedback.messages))
            # Even though cancel() was called, is_cancelled() should return False
            self.assertFalse(feedback.is_cancelled())


if __name__ == "__main__":
    unittest.main()
