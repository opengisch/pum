import logging
import packaging
import packaging.version
import os
import sys
import importlib.metadata
import subprocess

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

            logger.info(f"Dependency {self.name} is satisfied.")

        except importlib.metadata.PackageNotFoundError as e:
            if not install_dependencies:
                raise PumDependencyError(
                    f"Dependency `{self.name}` is not installed. You can activate the installation."
                ) from e
            else:
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

        command = [self.python_command(), "-m", "pip", "install", req, "--target", install_path]

        try:
            output = subprocess.run(command, capture_output=True, text=True, check=False)
            if output.returncode != 0:
                logger.error("pip installed failed: %s", output.stderr)
                raise PumDependencyError(output.stderr)
        except TypeError:
            logger.error("Invalid command: %s", " ".join(command))
            raise PumDependencyError("invalid command: {}".format(" ".join(filter(None, command))))

    def python_command(self):
        # python is normally found at sys.executable, but there is an issue on windows qgis so use 'python' instead
        # https://github.com/qgis/QGIS/issues/45646
        return "python" if os.name == "nt" else sys.executable
