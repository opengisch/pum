"""PUM version resolution.

The installed PUM version is exposed as two module-level attributes:

- ``__version__``: the canonical version string (PEP 396).
- ``VERSION``: the same value parsed into a :class:`packaging.version.Version`,
  which is convenient for comparisons.

Resolution priority (first match wins):

1. Bundled ``pum-*.dist-info/METADATA`` sibling to the package directory.
   Checked first so that the reported version always matches the code being
   executed, even when a different ``pum`` is installed system-wide (e.g. when
   PUM is vendored inside another package such as ``oqtopus.libs.pum``).
2. ``git describe`` when running from a source checkout.
3. :func:`importlib.metadata.version` for a regular installed package.
4. ``"0.0.0"`` as an ultimate fallback.
"""

from __future__ import annotations

import glob
import importlib.metadata
import os
import subprocess
from pathlib import Path

import packaging.version


def _resolve_version() -> packaging.version.Version:
    # 1. Bundled pum-*.dist-info/METADATA (next to this package)
    # The parent is taken lexically: when the package directory is itself a symlink
    # (a development checkout linked into a vendoring directory), a trailing ".." is
    # resolved by the kernel against the link target, which would look for the
    # dist-info next to the checkout instead of next to the symlink.
    package_dir = os.path.dirname(os.path.abspath(__file__))
    dist_info_dirs = glob.glob(os.path.join(os.path.dirname(package_dir), "pum-*.dist-info"))
    bundled_versions: list[str] = []
    for dist_info in dist_info_dirs:
        metadata_path = os.path.join(dist_info, "METADATA")
        if os.path.isfile(metadata_path):
            with open(metadata_path) as f:
                for line in f:
                    if line.startswith("Version:"):
                        bundled_versions.append(line.split(":", 1)[1].strip())
                        break
    if bundled_versions:
        return max(packaging.version.Version(v) for v in bundled_versions)

    # 2. git describe (development from source)
    # Symlinks are resolved here: the checkout holding the .git directory is the one
    # the code actually lives in, which may differ from the lexical parent above.
    try:
        source_dir = Path(__file__).resolve().parent.parent
        git_dir = source_dir / ".git"
        if git_dir.exists():
            result = subprocess.run(
                ["git", "describe", "--tags", "--always", "--dirty"],
                cwd=source_dir,
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                git_version = result.stdout.strip()
                # Clean up git version to be PEP 440 compatible.
                # e.g. "0.9.2-10-g1234567" -> "0.9.2.post10"
                # e.g. "0.9.2"             -> "0.9.2"
                # e.g. "1234567" (no tags) -> "0.0.0+1234567"
                if "-" in git_version:
                    parts = git_version.split("-")
                    if len(parts) >= 3 and parts[0][0].isdigit():
                        return packaging.version.Version(f"{parts[0]}.post{parts[1]}")
                    return packaging.version.Version(f"0.0.0+{parts[0]}")
                if git_version[0].isdigit():
                    return packaging.version.Version(git_version)
                return packaging.version.Version(f"0.0.0+{git_version}")
    except Exception:
        pass

    # 3. Installed package metadata
    try:
        return packaging.version.Version(importlib.metadata.version("pum"))
    except importlib.metadata.PackageNotFoundError:
        pass

    # 4. Ultimate fallback
    return packaging.version.Version("0.0.0")


VERSION: packaging.version.Version = _resolve_version()
__version__: str = str(VERSION)

__all__ = ["VERSION", "__version__"]
