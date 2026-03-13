import tempfile
import unittest
from unittest.mock import patch

import packaging.version

from pum.dependency_handler import DependencyHandler
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


if __name__ == "__main__":
    unittest.main()
