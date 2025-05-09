from pathlib import Path
from os.path import isdir, join
from os import listdir
from .exceptions import PumException
from packaging.version import parse as parse_version
from .config import PumConfig
from .changelog import Changelog


def last_version(
    config: PumConfig,
    dir: str | Path = ".",
    min_version: str | None = None,
    max_version: str | None = None,
) -> str | None:
    """
    Return the last version of the changelogs.
    The changelogs are sorted by version.
    If after_current_version is True, only the changelogs that are after the current version will be returned.
    If after_current_version is False, all changelogs will be returned.

    Args:
        config (PumConfig): The configuration object.
        dir (str | Path): The directory where the changelogs are located.
        min_version (str | None): The version to start from (inclusive).
        max_version (str | None): The version to end at (inclusive).

    Returns:
        str | None: The last version of the changelogs. If no changelogs are found, None is returned.
    """
    changelogs = list_changelogs(config, dir, min_version, max_version)
    if not changelogs:
        return None
    if min_version:
        changelogs = [c for c in changelogs if c.version >= parse_version(min_version)]
    if max_version:
        changelogs = [c for c in changelogs if c.version <= parse_version(max_version)]
    if not changelogs:
        return None
    return changelogs[-1].version


def list_changelogs(
    config: PumConfig,
    dir: str | Path = ".",
    min_version: str | None = None,
    max_version: str | None = None,
) -> list:
    """
    Return a list of changelogs.
    The changelogs are sorted by version.
    If after_current_version is True, only the changelogs that are after the current version will be returned.
    If after_current_version is False, all changelogs will be returned.

    Args:
        config (PumConfig): The configuration object.
        dir (str | Path): The directory where the changelogs are located.
        min_version (str | None): The version to start from (inclusive).
        max_version (str | None): The version to end at (inclusive).

    Returns:
        list: A list of changelogs. Each changelog is represented by a Changelog object.
    """
    path = Path(dir)
    if not path.is_dir():
        raise PumException(f"Module directory `{path}` does not exist.")
    path = path / config.changelogs_directory
    if not path.is_dir():
        raise PumException(f"Changelogs directory `{path}` does not exist.")

    changelogs = [Changelog(path / d) for d in listdir(path) if isdir(join(path, d))]

    if min_version:
        changelogs = [c for c in changelogs if c.version >= parse_version(min_version)]
    if max_version:
        changelogs = [c for c in changelogs if c.version <= parse_version(max_version)]

    changelogs.sort(key=lambda c: c.version)
    return changelogs


def changelog_files(changelog: str) -> list[Path]:
    """
    Get the ordered list of SQL files in the changelog directory.
    This is not recursive, it only returns the files in the given changelog directory.
    Args:
        changelog (str): The changelog directory.
    Returns:
        list[Path]: A list of paths to the changelog files.
    """
    files = [
        changelog.dir / f
        for f in listdir(changelog.dir)
        if (changelog.dir / f).is_file() and f.endswith(".sql")
    ]
    files.sort()
    return files
