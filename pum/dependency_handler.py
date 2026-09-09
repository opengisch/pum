import logging
import packaging
import packaging.version
import functools
import os
import sys
import importlib.metadata
import subprocess
import sysconfig
from pathlib import Path

from .exceptions import PumDependencyError

logger = logging.getLogger(__name__)

# On Windows, prevent console windows from flashing when running subprocesses
_subprocess_kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}


def _is_python_executable(path: str | Path | None) -> bool:
    """Return whether `path` is a runnable Python interpreter."""
    if not path:
        return False
    path = Path(path)
    if not path.name.lower().startswith("python"):
        return False
    return path.is_file() and os.access(path, os.X_OK)


def _runs_this_python_version(path: Path) -> bool:
    """Return whether `path` runs and reports the version of the current interpreter."""
    expected = f"{sys.version_info.major}.{sys.version_info.minor}"
    try:
        output = subprocess.run(
            [str(path), "-c", "import sys; print('%s.%s' % sys.version_info[:2])"],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
            **_subprocess_kwargs,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return output.returncode == 0 and output.stdout.strip() == expected


@functools.cache
def _resolve_python_command(host_executable: str) -> str:
    """Find a Python interpreter when `host_executable` is not one.

    Cached, and keyed on the host executable, because it may spawn a probe process
    per candidate and is called once per dependency.
    """
    versioned = f"python{sys.version_info.major}.{sys.version_info.minor}"
    search_dirs: list[Path] = []
    if host_executable:
        # macOS application bundles ship the interpreter next to the host binary.
        search_dirs.append(Path(host_executable).parent)
    bindir = sysconfig.get_config_var("BINDIR")
    if bindir:
        # BINDIR is stale for relocated builds (vcpkg), where the interpreter sits
        # one level up, so probe both.
        search_dirs += [Path(bindir), Path(bindir).parent]
    search_dirs += [Path(sys.base_prefix) / "bin", Path(sys.base_prefix)]

    for directory in search_dirs:
        for name in (versioned, "python3", "python"):
            candidate = directory / name
            # The candidate has to be run, not merely found: a bundled interpreter
            # may need a wrapper to set PYTHONHOME, and a version mismatch would
            # install the dependencies into a site-packages nothing imports.
            if _is_python_executable(candidate) and _runs_this_python_version(candidate):
                return str(candidate)

    raise PumDependencyError(
        f"No Python interpreter found to run pip with: `{host_executable}` is not one. "
        "Install the module dependencies manually."
    )


def prefix_site_packages(prefix: str | Path) -> list[str]:
    """Return the site-packages directories of a pip `--prefix` installation.

    The first entries are what the standard scheme prescribes; the globs pick up
    distributions that relocate it (Debian's `local/` scheme, `lib64`).
    """
    prefix = Path(prefix)
    scheme = "nt" if os.name == "nt" else "posix_prefix"
    paths = sysconfig.get_paths(scheme, vars={"base": str(prefix), "platbase": str(prefix)})
    candidates = [paths["purelib"], paths["platlib"]]
    candidates += [str(p) for p in sorted(prefix.glob("lib*/python*/*-packages"))]
    candidates += [str(p) for p in sorted(prefix.glob("local/lib*/python*/*-packages"))]

    directories = []
    for candidate in candidates:
        if candidate not in directories:
            directories.append(candidate)
    return directories


class _VersionMismatchError(Exception):
    """Internal exception used to signal version mismatch within resolve()."""

    pass


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
            installed_version = packaging.version.Version(importlib.metadata.version(self.name))
            if self.minimum_version and installed_version < self.minimum_version:
                if not install_dependencies:
                    raise PumDependencyError(
                        f"Installed version of `{self.name}` ({installed_version}) is lower than the minimum required ({self.minimum_version})."
                    )
                raise _VersionMismatchError(
                    f"Installed version of `{self.name}` ({installed_version}) is lower than the minimum required ({self.minimum_version})."
                )
            if self.maximum_version and installed_version > self.maximum_version:
                if not install_dependencies:
                    raise PumDependencyError(
                        f"Installed version of `{self.name}` ({installed_version}) is higher than the maximum allowed ({self.maximum_version})."
                    )
                raise _VersionMismatchError(
                    f"Installed version of `{self.name}` ({installed_version}) is higher than the maximum allowed ({self.maximum_version})."
                )

            logger.debug(f"Dependency {self.name} is satisfied.")

        except (importlib.metadata.PackageNotFoundError, _VersionMismatchError) as e:
            if not install_dependencies:
                raise PumDependencyError(
                    f"Dependency `{self.name}` is not installed. You can activate the installation."
                ) from e
            else:
                if install_path is None:
                    raise PumDependencyError(
                        f"Dependency `{self.name}` is not installed and no install path was provided."
                    )
                logger.debug(f"Dependency {self.name} is not satisfied, proceeding to install.")
                logger.warning(f"Dependency {self.name} is not satisfied, trying to install: {e}")
                self.pip_install(install_path=install_path)
                logger.warning(f"Dependency {self.name} is now installed in {install_path}")

    def pip_install(self, install_path: str):
        """Install the dependency with pip under the `install_path` prefix.

        `--prefix` is used rather than `--target`: pip forces `--ignore-installed`
        for `--target`, which reinstalls the whole dependency closure and shadows
        the packages the host application already provides.

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
        install_path_str = str(install_path)

        # Let pip see what is already installed under the prefix, so that a cached
        # dependency is not installed again and a locally upgraded pip is picked up.
        env = os.environ.copy()
        pythonpath = [*prefix_site_packages(install_path_str), env.get("PYTHONPATH", "")]
        env["PYTHONPATH"] = os.pathsep.join(p for p in pythonpath if p)

        # First, ensure pip is installed in the prefix and upgrade it if needed
        try:
            pip_version_output = subprocess.run(
                [python_cmd, "-m", "pip", "--version"],
                capture_output=True,
                text=True,
                check=False,
                env=env,
                **_subprocess_kwargs,
            )
            if pip_version_output.returncode == 0:
                # Extract pip version (format: "pip X.Y.Z from ...")
                pip_version_str = pip_version_output.stdout.split()[1]
                pip_version = packaging.version.Version(pip_version_str)
                if pip_version < packaging.version.Version("22.0"):
                    logger.warning(
                        f"pip version {pip_version} is outdated, installing newer pip to the prefix..."
                    )
                    # Install a newer pip to the prefix first
                    # This will be used by subsequent installations
                    upgrade_cmd = [
                        python_cmd,
                        "-m",
                        "pip",
                        "install",
                        "--upgrade",
                        "pip>=22.0",
                        "--prefix",
                        install_path_str,
                    ]
                    upgrade_result = subprocess.run(
                        upgrade_cmd,
                        capture_output=True,
                        text=True,
                        check=False,
                        env=env,
                        **_subprocess_kwargs,
                    )
                    if upgrade_result.returncode == 0:
                        logger.info(f"Successfully upgraded pip in {install_path}")
        except Exception as e:
            logger.debug(f"Could not check/upgrade pip version: {e}")

        command = [python_cmd, "-m", "pip", "install", req, "--prefix", install_path_str]

        try:
            output = subprocess.run(
                command, capture_output=True, text=True, check=False, env=env, **_subprocess_kwargs
            )
            if output.returncode != 0:
                logger.error("pip installed failed: %s", output.stderr)
                raise PumDependencyError(output.stderr)
        except TypeError:
            logger.error("Invalid command: %s", " ".join(command))
            raise PumDependencyError("invalid command: {}".format(" ".join(filter(None, command))))

    def python_command(self):
        """Return the Python interpreter to invoke pip with.

        `sys.executable` cannot be trusted: when Python is embedded in a host
        application it points at the host binary, and executing that would start a
        second instance of the application instead of running pip. QGIS never sets
        `PyConfig.program_name`, so this is the case on every platform.
        """
        # python is normally found at sys.executable, but there is an issue on windows qgis so use 'python' instead
        # https://github.com/qgis/QGIS/issues/45646
        if os.name == "nt":
            return "python"

        if _is_python_executable(sys.executable):
            return sys.executable

        return _resolve_python_command(sys.executable or "")
