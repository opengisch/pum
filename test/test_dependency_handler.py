import os
import sys
import sysconfig
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import packaging.version

from pum.dependency_handler import (
    DependencyHandler,
    _find_python_command,
    _is_python_executable,
    _python_candidate_names,
    _runs_this_python_version,
    pip_environment,
    prefix_site_packages,
    python_command,
)
from pum.exceptions import PumDependencyError


class TestDependencyHandler(unittest.TestCase):
    """Test the DependencyHandler class."""

    def test_version_too_low_no_install(self):
        """When installed version is below minimum and install is disabled, raise PumDependencyError."""
        handler = DependencyHandler(
            name="some-package",
            minimum_version=packaging.version.Version("3.0.0"),
            maximum_version=None,
        )
        with patch("pum.dependency_handler.importlib.metadata.version", return_value="1.4.1"):
            with self.assertRaises(PumDependencyError) as ctx:
                handler.resolve(install_dependencies=False)
            self.assertIn("lower than the minimum required", str(ctx.exception))

    def test_version_too_high_no_install(self):
        """When installed version is above maximum and install is disabled, raise PumDependencyError."""
        handler = DependencyHandler(
            name="some-package",
            minimum_version=None,
            maximum_version=packaging.version.Version("2.0.0"),
        )
        with patch("pum.dependency_handler.importlib.metadata.version", return_value="3.0.0"):
            with self.assertRaises(PumDependencyError) as ctx:
                handler.resolve(install_dependencies=False)
            self.assertIn("higher than the maximum allowed", str(ctx.exception))

    def test_version_too_low_with_install(self):
        """When installed version is below minimum and install is enabled, pip_install should be called."""
        handler = DependencyHandler(
            name="some-package",
            minimum_version=packaging.version.Version("3.0.0"),
            maximum_version=None,
        )
        with (
            patch("pum.dependency_handler.importlib.metadata.version", return_value="1.4.1"),
            patch.object(handler, "pip_install") as mock_pip_install,
        ):
            with tempfile.TemporaryDirectory() as tmpdir:
                handler.resolve(install_dependencies=True, install_path=tmpdir)
                mock_pip_install.assert_called_once_with(install_path=tmpdir)

    def test_version_too_high_with_install(self):
        """When installed version is above maximum and install is enabled, pip_install should be called."""
        handler = DependencyHandler(
            name="some-package",
            minimum_version=None,
            maximum_version=packaging.version.Version("2.0.0"),
        )
        with (
            patch("pum.dependency_handler.importlib.metadata.version", return_value="3.0.0"),
            patch.object(handler, "pip_install") as mock_pip_install,
        ):
            with tempfile.TemporaryDirectory() as tmpdir:
                handler.resolve(install_dependencies=True, install_path=tmpdir)
                mock_pip_install.assert_called_once_with(install_path=tmpdir)

    def test_version_satisfied(self):
        """When installed version satisfies constraints, no error is raised."""
        handler = DependencyHandler(
            name="some-package",
            minimum_version=packaging.version.Version("1.0.0"),
            maximum_version=packaging.version.Version("4.0.0"),
        )
        with patch("pum.dependency_handler.importlib.metadata.version", return_value="2.0.0"):
            handler.resolve(install_dependencies=False)

    def test_not_installed_no_install(self):
        """When package is not installed and install is disabled, raise PumDependencyError."""
        handler = DependencyHandler(
            name="nonexistent-package",
            minimum_version=packaging.version.Version("1.0.0"),
            maximum_version=None,
        )
        with patch(
            "pum.dependency_handler.importlib.metadata.version",
            side_effect=__import__("importlib").metadata.PackageNotFoundError(
                "nonexistent-package"
            ),
        ):
            with self.assertRaises(PumDependencyError):
                handler.resolve(install_dependencies=False)

    def test_not_installed_with_install(self):
        """When package is not installed and install is enabled, pip_install should be called."""
        handler = DependencyHandler(
            name="nonexistent-package",
            minimum_version=packaging.version.Version("1.0.0"),
            maximum_version=None,
        )
        with (
            patch(
                "pum.dependency_handler.importlib.metadata.version",
                side_effect=__import__("importlib").metadata.PackageNotFoundError(
                    "nonexistent-package"
                ),
            ),
            patch.object(handler, "pip_install") as mock_pip_install,
        ):
            with tempfile.TemporaryDirectory() as tmpdir:
                handler.resolve(install_dependencies=True, install_path=tmpdir)
                mock_pip_install.assert_called_once_with(install_path=tmpdir)


class TestPrefixSitePackages(unittest.TestCase):
    """Test the discovery of the site-packages of a pip `--prefix` install."""

    def test_scheme_paths_are_returned_for_an_empty_prefix(self):
        """Before pip has run, the scheme paths are all there is to go on."""
        with tempfile.TemporaryDirectory() as prefix:
            directories = prefix_site_packages(prefix)
            self.assertTrue(directories)
            for directory in directories:
                self.assertTrue(directory.startswith(str(Path(prefix))))
            scheme = "nt" if os.name == "nt" else "posix_prefix"
            expected = sysconfig.get_paths(scheme, vars={"base": prefix, "platbase": prefix})
            self.assertIn(expected["purelib"], directories)
            self.assertIn(expected["platlib"], directories)

    def test_relocated_layouts_are_discovered(self):
        """Debian's dist-packages and local/ prefix are found once they exist."""
        with tempfile.TemporaryDirectory() as prefix:
            debian = Path(prefix) / "lib" / "python3" / "dist-packages"
            debian_local = Path(prefix) / "local" / "lib" / "python3" / "dist-packages"
            fedora = Path(prefix) / "lib64" / "python3.12" / "site-packages"
            for directory in (debian, debian_local, fedora):
                directory.mkdir(parents=True, exist_ok=True)

            directories = prefix_site_packages(prefix)
            for directory in (debian, debian_local, fedora):
                self.assertIn(str(directory), directories)

    def test_windows_layout_is_discovered(self):
        """`Lib/site-packages` sits one level shallower than the posix layout.

        In its own prefix: on a case-insensitive filesystem `Lib` and `lib`
        are the same directory.
        """
        with tempfile.TemporaryDirectory() as prefix:
            windows = Path(prefix) / "Lib" / "site-packages"
            windows.mkdir(parents=True)
            self.assertIn(str(windows), prefix_site_packages(prefix))

    def test_unrelated_directories_are_ignored(self):
        """Only site-packages and dist-packages are picked up."""
        with tempfile.TemporaryDirectory() as prefix:
            noise = Path(prefix) / "share" / "my-packages"
            noise.mkdir(parents=True)
            self.assertNotIn(str(noise), prefix_site_packages(prefix))

    def test_no_duplicates(self):
        """A directory matching both the scheme and a glob is returned once."""
        with tempfile.TemporaryDirectory() as prefix:
            scheme = "nt" if os.name == "nt" else "posix_prefix"
            purelib = sysconfig.get_paths(scheme, vars={"base": prefix, "platbase": prefix})[
                "purelib"
            ]
            Path(purelib).mkdir(parents=True, exist_ok=True)
            directories = prefix_site_packages(prefix)
            self.assertEqual(len(directories), len(set(directories)))

    def test_pip_environment_exposes_the_prefix(self):
        """pip must see what the prefix already holds, ahead of the outer PYTHONPATH."""
        with tempfile.TemporaryDirectory() as prefix:
            with patch.dict(os.environ, {"PYTHONPATH": "/somewhere/else"}):
                entries = pip_environment(prefix)["PYTHONPATH"].split(os.pathsep)
            self.assertEqual(entries[-1], "/somewhere/else")
            self.assertEqual(entries[:-1], prefix_site_packages(prefix))


class TestPythonCommand(unittest.TestCase):
    """Test the resolution of the interpreter used to run pip."""

    def setUp(self):
        _find_python_command.cache_clear()
        _runs_this_python_version.cache_clear()

    def tearDown(self):
        _find_python_command.cache_clear()
        _runs_this_python_version.cache_clear()

    def test_resolves_to_a_matching_interpreter(self):
        """Whatever the environment, the result runs and matches this version."""
        resolved = python_command()
        self.assertTrue(_runs_this_python_version(resolved))

    def test_resolves_when_the_host_is_not_an_interpreter(self):
        """An embedded host (QGIS) must not be used to run pip."""
        with tempfile.TemporaryDirectory() as tmpdir:
            host = Path(tmpdir) / ("qgis-bin.exe" if os.name == "nt" else "qgis")
            host.write_text("not an interpreter")
            host.chmod(0o755)
            with patch.object(sys, "executable", str(host)):
                resolved = python_command()
            self.assertNotEqual(Path(resolved), host)
            self.assertTrue(_runs_this_python_version(resolved))

    def test_prefers_the_interpreter_shipped_next_to_the_host(self):
        """The macOS QGIS bundle keeps its interpreter in `Contents/MacOS/bin`."""
        with tempfile.TemporaryDirectory() as tmpdir:
            macos = Path(tmpdir) / "QGIS.app" / "Contents" / "MacOS"
            (macos / "bin").mkdir(parents=True)
            host = macos / "QGIS"
            host.write_text("not an interpreter")
            host.chmod(0o755)
            # The exact name the resolver looks for first, `.exe` included.
            bundled = macos / "bin" / _python_candidate_names()[0]
            try:
                os.symlink(sys.executable, bundled)
            except (OSError, NotImplementedError) as e:
                self.skipTest(f"cannot symlink an interpreter here: {e}")
            if not _runs_this_python_version(str(bundled)):
                self.skipTest("a symlinked interpreter does not run on this platform")

            with patch.object(sys, "executable", str(host)):
                resolved = python_command()
            self.assertEqual(Path(resolved), bundled)

    def test_raises_when_nothing_is_found(self):
        """The error names the host and stays actionable."""
        with patch.object(sys, "executable", "/nowhere/qgis"):
            with patch("pum.dependency_handler._find_python_command", return_value=None):
                with self.assertRaises(PumDependencyError) as ctx:
                    python_command()
        self.assertIn("/nowhere/qgis", str(ctx.exception))

    def test_is_python_executable(self):
        """A name check alone is not enough."""
        self.assertTrue(_is_python_executable(sys.executable))
        self.assertFalse(_is_python_executable(None))
        self.assertFalse(_is_python_executable(""))
        self.assertFalse(_is_python_executable("/nonexistent/python"))
        with tempfile.TemporaryDirectory() as tmpdir:
            # A Microsoft Store reparse point: named python, zero bytes.
            stub = Path(tmpdir) / f"python{'.exe' if os.name == 'nt' else ''}"
            stub.touch()
            stub.chmod(0o755)
            self.assertFalse(_is_python_executable(stub))
            # A directory named python is not an interpreter either.
            directory = Path(tmpdir) / "python3-dir"
            directory.mkdir()
            self.assertFalse(_is_python_executable(directory))

    def test_a_mismatched_interpreter_is_rejected(self):
        """Installing with another minor version would target an unused prefix."""
        with patch.object(sys, "version_info", SimpleNamespace(major=3, minor=0)):
            _runs_this_python_version.cache_clear()
            self.assertFalse(_runs_this_python_version(sys.executable))


if __name__ == "__main__":
    unittest.main()
