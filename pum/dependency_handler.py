import functools
import importlib.metadata
import logging
import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

import packaging
import packaging.version

from .exceptions import PumDependencyError

logger = logging.getLogger(__name__)

# On Windows, prevent console windows from flashing when running subprocesses
_subprocess_kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}

# Probing a candidate interpreter spawns a process. What keeps the cost down is
# `_is_python_executable`, which rejects almost every candidate without running
# it; the timeout only bounds the handful that survive that gate and then hang.
_PROBE_TIMEOUT = 20

_EXE_SUFFIX = ".exe" if os.name == "nt" else ""

# Directory names pip may install into, across distributions and platforms.
_SITE_PACKAGES_NAMES = ("site-packages", "dist-packages")


def _is_python_executable(path: str | Path | None) -> bool:
    """Return whether `path` looks like a runnable Python interpreter.

    The size check is for Windows, where the Microsoft Store installs zero-byte
    `python.exe` reparse points that open the store instead of running anything.
    """
    if not path:
        return False
    path = Path(path)
    if not path.name.lower().startswith("python"):
        return False
    try:
        if not path.is_file() or path.stat().st_size == 0:
            return False
    except OSError:
        return False
    return os.access(path, os.X_OK)


@functools.cache
def _runs_this_python_version(path: str) -> bool:
    """Return whether `path` runs and reports the version of the current interpreter.

    Cached: the same candidate is reached from several search directories, and
    each probe costs a process.
    """
    expected = f"{sys.version_info.major}.{sys.version_info.minor}"
    try:
        # B603: fixed argv, no shell; ``path`` is an interpreter candidate found
        # next to sys.executable, not user input.
        output = subprocess.run(  # nosec B603
            [path, "-c", "import sys; print('%s.%s' % sys.version_info[:2])"],
            capture_output=True,
            text=True,
            check=False,
            timeout=_PROBE_TIMEOUT,
            **_subprocess_kwargs,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return output.returncode == 0 and output.stdout.strip() == expected


def _python_candidate_dirs() -> list[Path]:
    """Return the directories that may hold this interpreter, most likely first."""
    dirs: list[Path] = []

    def add(directory: str | Path | None) -> None:
        if not directory:
            return
        directory = Path(directory)
        if directory not in dirs:
            dirs.append(directory)

    # 1. Next to the running binary. When Python is embedded this is the host
    #    application's directory, which is also where the macOS QGIS bundle keeps
    #    the interpreter it ships (`QGIS.app/Contents/MacOS/bin`).
    if sys.executable:
        executable_dir = Path(sys.executable).parent
        add(executable_dir)
        add(executable_dir / "bin")

    # 2. The scripts directory of the running installation: this is the scheme pip
    #    itself uses, so it stays correct for relocated installations.
    try:
        add(sysconfig.get_paths()["scripts"])
    except (KeyError, OSError):  # pragma: no cover - depends on a broken sysconfig
        pass

    # 3. The installation prefixes. `base_prefix` is the interpreter itself even
    #    inside a virtual environment, and it is what an embedded host resolves
    #    through PYTHONHOME (OSGeo4W on Windows, the bundle on macOS).
    for prefix in (sys.prefix, sys.base_prefix, sys.exec_prefix, sys.base_exec_prefix):
        if not prefix:
            continue
        add(prefix)
        add(Path(prefix) / "bin")
        add(Path(prefix) / "Scripts")

    # 4. BINDIR and its parent. On Windows BINDIR is derived from `sys.executable`
    #    and therefore points at the host application; for relocated builds
    #    (vcpkg) it is stale and the interpreter sits one level up.
    bindir = sysconfig.get_config_var("BINDIR")
    if bindir:
        add(bindir)
        add(Path(bindir).parent)

    return dirs


def _python_candidate_names() -> list[str]:
    """Return the interpreter file names to look for, most specific first."""
    names = [
        f"python{sys.version_info.major}.{sys.version_info.minor}",
        f"python{sys.version_info.major}",
        "python",
    ]
    return [f"{name}{_EXE_SUFFIX}" for name in names]


@functools.cache
def _find_python_command() -> str | None:
    """Return a Python interpreter matching the running one, or None.

    Returns None rather than raising so that the failure is cached too: without
    that, every dependency would re-probe every candidate with a subprocess each.
    """
    for directory in _python_candidate_dirs():
        for name in _python_candidate_names():
            candidate = directory / name
            # The candidate has to be run, not merely found: a bundled interpreter
            # may need a wrapper to set PYTHONHOME, and a version mismatch would
            # install the dependencies into a site-packages nothing imports.
            if _is_python_executable(candidate) and _runs_this_python_version(str(candidate)):
                return str(candidate)

    # Last resort: PATH. Probed like the rest, so a mismatched interpreter (a
    # different minor version, a Store stub) is rejected rather than used.
    for name in _python_candidate_names():
        found = shutil.which(name)
        if found and _is_python_executable(found) and _runs_this_python_version(found):
            return found

    return None


def python_command() -> str:
    """Return the Python interpreter to invoke pip with.

    `sys.executable` cannot be trusted: when Python is embedded in a host
    application it points at the host binary, and executing that would start a
    second instance of the application instead of running pip. QGIS never sets
    `PyConfig.program_name`, so this is the case on every platform, Windows
    included (https://github.com/qgis/QGIS/issues/45646).

    Raises:
        PumDependencyError: If no interpreter matching the running one was found.

    """
    if _is_python_executable(sys.executable):
        return sys.executable

    found = _find_python_command()
    if found:
        return found

    searched = ", ".join(f"`{d}`" for d in _python_candidate_dirs())
    raise PumDependencyError(
        f"No Python {sys.version_info.major}.{sys.version_info.minor} interpreter found to run "
        f"pip with: `{sys.executable}` is not one, and none was found in {searched} or on PATH. "
        "Install the module dependencies manually."
    )


def prefix_site_packages(prefix: str | Path) -> list[str]:
    """Return the site-packages directories of a pip `--prefix` installation.

    The scheme paths come first: they are what pip prescribes, and unlike the
    globs they are known before anything has been installed. The globs then pick
    up the layouts distributions relocate -- Debian's `dist-packages` and its
    `local/` prefix, `lib64` on Fedora -- which can only be discovered once the
    directories exist. Callers must therefore call this again after pip has run.
    """
    prefix = Path(prefix)
    scheme = "nt" if os.name == "nt" else "posix_prefix"
    paths = sysconfig.get_paths(scheme, vars={"base": str(prefix), "platbase": str(prefix)})
    candidates = [paths["purelib"], paths["platlib"]]

    # Scheme-agnostic discovery, bounded in depth: `Lib/site-packages` (2),
    # `lib/python3.12/site-packages` (3), `local/lib/python3/dist-packages` (4).
    for depth in range(4):
        for name in _SITE_PACKAGES_NAMES:
            for found in sorted(prefix.glob("*/" * depth + name)):
                if found.is_dir():
                    candidates.append(str(found))

    directories: list[str] = []
    for candidate in candidates:
        if candidate not in directories:
            directories.append(candidate)
    return directories


def pip_environment(install_path: str | Path) -> dict[str, str]:
    """Return the environment for a pip subprocess targeting `install_path`.

    Exposing the prefix on PYTHONPATH lets pip see what is already installed
    there, so that a cached dependency is not installed again and a pip upgraded
    into the prefix is picked up.
    """
    env = os.environ.copy()
    entries = [*prefix_site_packages(install_path), env.get("PYTHONPATH", "")]
    env["PYTHONPATH"] = os.pathsep.join(entry for entry in entries if entry)
    return env


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

    def resolve(
        self, install_dependencies: bool = False, install_path: str | Path | None = None
    ) -> None:
        """
        Resolve the dependency by checking if it is installed and compatible with the current PUM version.

        Args:
            install_dependencies: If True, the dependency will be locally installed.
            install_path: The pip prefix to install into when the dependency is missing.
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

    def requirement(self) -> str:
        """Return the pip requirement specifier for this dependency."""
        req = self.name
        if self.minimum_version and self.maximum_version:
            req += f">={self.minimum_version},<={self.maximum_version}"
        elif self.minimum_version:
            req += f">={self.minimum_version}"
        elif self.maximum_version:
            req += f"<={self.maximum_version}"
        return req

    def pip_install(self, install_path: str | Path):
        """Install the dependency with pip under the `install_path` prefix.

        `--prefix` is used rather than `--target`: pip forces `--ignore-installed`
        for `--target`, which reinstalls the whole dependency closure and shadows
        the packages the host application already provides.
        """

        req = self.requirement()
        python_cmd = python_command()
        install_path_str = str(install_path)

        # pip has to be importable by that interpreter; without this check its
        # absence would surface as an opaque pip stderr from the install below.
        # B603: fixed argv, no shell; ``python_cmd`` is the interpreter resolved
        # by python_command(), not user input.
        probe = subprocess.run(  # nosec B603
            [python_cmd, "-m", "pip", "--version"],
            capture_output=True,
            text=True,
            check=False,
            env=pip_environment(install_path_str),
            **_subprocess_kwargs,
        )
        if probe.returncode != 0:
            raise PumDependencyError(
                f"`{python_cmd} -m pip` is not available: {probe.stderr.strip()}. "
                "Install the module dependencies manually."
            )

        command = [python_cmd, "-m", "pip", "install", req, "--prefix", install_path_str]

        # B603: fixed argv, no shell; the only variable part is the requirement
        # string built from the module dependency definition.
        output = subprocess.run(  # nosec B603
            command,
            capture_output=True,
            text=True,
            check=False,
            env=pip_environment(install_path_str),
            **_subprocess_kwargs,
        )
        if output.returncode != 0:
            logger.error("pip install failed: %s", output.stderr)
            raise PumDependencyError(output.stderr)
