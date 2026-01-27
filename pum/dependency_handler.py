import logging
import packaging
import packaging.version
import os
import sys
import importlib.metadata
import subprocess
from pathlib import Path

from .exceptions import PumDependencyError

logger = logging.getLogger(__name__)


class DependencyHandler:
    def __init__(
        self,
        name: str,
        *,
        minimum_version: packaging.version.Version | None,
        maximum_version: packaging.version.Version | None,
    ):
        """
        Initialize the DependencyHandler with a dependency name and version.
        Args:
            name (str): The name of the dependency.
            version (packaging.version.Version | None): The version of the dependency, or None if not specified.
        """
        self.name = name
        self.minimum_version = minimum_version
        self.maximum_version = maximum_version

    def resolve(self, install_dependencies: bool = False, install_path: str | None = None):
        """
        Resolve the dependency by checking if it is installed and compatible with the current PUM version.

        Args:
            install_dependencies: If True, the dependency will be locally installed.
        Raises:
            PumConfigError: If the dependency is not installed or is incompatible.
        """
        try:
            importlib.metadata.version(self.name)

            installed_version = packaging.version.Version(importlib.metadata.version(self.name))
            if self.minimum_version and installed_version < self.minimum_version:
                raise PumDependencyError(
                    f"Installed version of `{self.name}` ({installed_version}) is lower than the minimum required ({self.minimum_version})."
                )
            if self.maximum_version and installed_version > self.maximum_version:
                raise PumDependencyError(
                    f"Installed version of `{self.name}` ({installed_version}) is higher than the maximum allowed ({self.maximum_version})."
                )

            logger.debug(f"Dependency {self.name} is satisfied.")

        except importlib.metadata.PackageNotFoundError as e:
            if not install_dependencies:
                raise PumDependencyError(
                    f"Dependency `{self.name}` is not installed. You can activate the installation."
                ) from e
            else:
                if install_path is None:
                    raise PumDependencyError(
                        f"Dependency `{self.name}` is not installed and no install path was provided."
                    )
                logger.debug(f"Dependency {self.name} is not installed, proceeding to install.")
                logger.warning(f"Dependency {self.name} is not installed, trying to install {e}")
                self.pip_install(install_path=install_path)
                logger.warning(f"Dependency {self.name} is now installed in {install_path}")

    def pip_install(self, install_path: str):
        """
        Installs given reqs with pip
        Code copied from qpip plugin
        """

        req = self.name
        if self.minimum_version and self.maximum_version:
            req += f">={self.minimum_version},<={self.maximum_version}"
        elif self.minimum_version:
            req += f">={self.minimum_version}"
        elif self.maximum_version:
            req += f"<={self.maximum_version}"

        python_cmd = self.python_command()

        # First, ensure pip is installed in the target directory and upgrade it if needed
        try:
            pip_version_output = subprocess.run(
                [python_cmd, "-m", "pip", "--version"], capture_output=True, text=True, check=False
            )
            if pip_version_output.returncode == 0:
                # Extract pip version (format: "pip X.Y.Z from ...")
                pip_version_str = pip_version_output.stdout.split()[1]
                pip_version = packaging.version.Version(pip_version_str)
                if pip_version < packaging.version.Version("22.0"):
                    logger.warning(
                        f"pip version {pip_version} is outdated, installing newer pip to target directory..."
                    )
                    # Install a newer pip to the target directory first
                    # This will be used by subsequent installations
                    upgrade_cmd = [
                        python_cmd,
                        "-m",
                        "pip",
                        "install",
                        "--upgrade",
                        "pip>=22.0",
                        "--target",
                        install_path,
                    ]
                    upgrade_result = subprocess.run(
                        upgrade_cmd, capture_output=True, text=True, check=False
                    )
                    if upgrade_result.returncode == 0:
                        logger.info(f"Successfully upgraded pip in {install_path}")
        except Exception as e:
            logger.debug(f"Could not check/upgrade pip version: {e}")

        # Set PYTHONPATH to include install_path so pip can find itself and other packages
        env = os.environ.copy()
        # Ensure install_path is a string for environment variables (Windows compatibility)
        install_path_str = str(install_path)
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{install_path_str}{os.pathsep}{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = install_path_str

        command = [python_cmd, "-m", "pip", "install", req, "--target", install_path_str]

        try:
            output = subprocess.run(command, capture_output=True, text=True, check=False, env=env)
            if output.returncode != 0:
                logger.error("pip installed failed: %s", output.stderr)
                raise PumDependencyError(output.stderr)
        except TypeError:
            logger.error("Invalid command: %s", " ".join(command))
            raise PumDependencyError("invalid command: {}".format(" ".join(filter(None, command))))

    def python_command(self):
        # python is normally found at sys.executable, but there is an issue on windows qgis so use 'python' instead
        # https://github.com/qgis/QGIS/issues/45646
        if os.name == "nt":
            return "python"

        # On macOS and Linux, if we're running inside QGIS, sys.executable points to the QGIS app
        # Look for the python executable in the same directory as sys.executable
        if sys.executable and "QGIS" in sys.executable:
            python_dir = Path(sys.executable).parent
            python_executable = python_dir / "python"
            if python_executable.exists():
                return str(python_executable)
            # Try python3 as fallback
            python3_executable = python_dir / "python3"
            if python3_executable.exists():
                return str(python3_executable)

        return sys.executable
