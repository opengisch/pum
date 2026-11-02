import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pum.cli import run_cache_command


class TestCacheCommand(unittest.TestCase):
    """Test the `pum cache` command against a throwaway cache directory."""

    def setUp(self) -> None:
        self.cache_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.cache_dir.cleanup)
        patcher = patch.dict(os.environ, {"PUM_CACHE_DIR": self.cache_dir.name})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.dependencies = Path(self.cache_dir.name) / "dependencies"

    def _prefix(self, name: str) -> Path:
        """Create a prefix holding one file, as an install would leave it."""
        directory = self.dependencies / name / "lib" / "python3.12" / "site-packages"
        directory.mkdir(parents=True)
        (directory / "some_module.py").write_text("VALUE = 1\n")
        return self.dependencies / name

    @staticmethod
    def _run(action: str, **kwargs) -> tuple[int, str]:
        out = io.StringIO()
        with redirect_stdout(out):
            code = run_cache_command(action, **kwargs)
        return code, out.getvalue()

    def test_path_prints_the_directory(self) -> None:
        """`path` is meant to be piped, so it goes to stdout."""
        code, out = self._run("path")
        self.assertEqual(code, 0)
        self.assertEqual(out.strip(), str(self.dependencies))

    def test_path_works_before_anything_is_cached(self) -> None:
        """The directory is only created on the first install."""
        self.assertFalse(self.dependencies.exists())
        self.assertEqual(self._run("path")[0], 0)

    def test_list_reports_each_prefix(self) -> None:
        self._prefix("module_a-0000000000000000")
        self._prefix("module_b-1111111111111111")
        code, out = self._run("list")
        self.assertEqual(code, 0)
        self.assertIn("module_a-0000000000000000", out)
        self.assertIn("module_b-1111111111111111", out)

    def test_list_on_an_empty_cache(self) -> None:
        code, out = self._run("list")
        self.assertEqual(code, 0)
        self.assertEqual(out, "")

    def test_clear_removes_every_prefix(self) -> None:
        self._prefix("module_a-0000000000000000")
        self._prefix("module_b-1111111111111111")
        self.assertEqual(self._run("clear", force=True)[0], 0)
        self.assertEqual(list(self.dependencies.iterdir()), [])

    def test_clear_keeps_everything_when_declined(self) -> None:
        """The confirmation must be a real gate, not a formality."""
        prefix = self._prefix("module_a-0000000000000000")
        with patch("builtins.input", return_value="no"):
            self.assertEqual(self._run("clear")[0], 0)
        self.assertTrue(prefix.is_dir())

    def test_clear_asks_before_deleting(self) -> None:
        prefix = self._prefix("module_a-0000000000000000")
        with patch("builtins.input", return_value="yes") as prompt:
            self.assertEqual(self._run("clear")[0], 0)
        prompt.assert_called_once()
        self.assertFalse(prefix.exists())

    def test_clear_on_an_empty_cache_is_a_no_op(self) -> None:
        """Nothing to remove must not prompt and must not fail."""
        with patch("builtins.input", side_effect=AssertionError("should not prompt")):
            self.assertEqual(self._run("clear", force=False)[0], 0)


if __name__ == "__main__":
    unittest.main()
