# Feedback System

The feedback system allows you to monitor progress and cancel long-running install/upgrade operations.

## Usage

### Basic Usage with LogFeedback (default)

```python
from pum import PumConfig, Upgrader

cfg = PumConfig.from_yaml(".pum.yaml")
upgrader = Upgrader(config=cfg)

# Uses LogFeedback by default - logs progress messages
upgrader.install(connection=conn)
```

### Custom Feedback Implementation

Create a custom feedback class to integrate with your UI:

```python
from pum.feedback import Feedback
from pum import Upgrader, PumConfig

class ProgressBarFeedback(Feedback):
    """Custom feedback that updates a progress bar."""

    def __init__(self, progress_bar):
        super().__init__()
        self.progress_bar = progress_bar

    def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
        """Update the progress bar with current progress."""
        if total > 0:
            percentage = int((current / total) * 100)
            self.progress_bar.setValue(percentage)
        self.progress_bar.setFormat(message)

# Use with install/upgrade
cfg = PumConfig.from_yaml(".pum.yaml")
upgrader = Upgrader(config=cfg)

feedback = ProgressBarFeedback(my_progress_bar)
upgrader.install(connection=conn, feedback=feedback)
```

### Cancellation Support

```python
from pum.feedback import Feedback

class CancellableFeedback(Feedback):
    """Feedback that can be cancelled via a button."""

    def __init__(self, cancel_button):
        super().__init__()
        self.cancel_button = cancel_button
        # Connect button to cancel method
        self.cancel_button.clicked.connect(self.cancel)

    def report_progress(self, message: str, current: int = 0, total: int = 0) -> None:
        """Report progress."""
        print(f"[{current}/{total}] {message}")

# The install/upgrade will check is_cancelled() and raise PumException if cancelled
feedback = CancellableFeedback(my_cancel_button)
try:
    upgrader.install(connection=conn, feedback=feedback)
except PumException as e:
    if "cancelled" in str(e).lower():
        print("Installation cancelled by user")
    else:
        raise
```

### Silent Feedback

```python
from pum.feedback import SilentFeedback

# No progress reporting at all
feedback = SilentFeedback()
upgrader.install(connection=conn, feedback=feedback)
```

## Progress Reporting

The feedback system reports progress for:

1. **Drop app handlers** - Before applying changelogs
2. **Changelogs** - Each changelog being applied
3. **Create app handlers** - After applying changelogs
4. **Roles and permissions** - When creating roles or granting permissions
5. **Commit** - When committing changes

Each progress report includes:
- `message`: Description of the current operation
- `current`: Current step number (if applicable)
- `total`: Total number of steps (if applicable)
