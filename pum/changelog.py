from pathlib import Path
from os.path import basename, isdir, join
from os import listdir
from .exceptions import PumException
from packaging.version import parse as parse_version
from .config import PumConfig


class Changelog:
    """
    This class represent a changelog directory.
    The directory name is the version of the changelog.
    """

    def __init__(self, dir):
        """
        Args:
            dir (str): The directory where the changelog is located.
        """
        self.dir = dir
        self.version = parse_version(basename(dir))

    def __repr__(self):
        return f"<dir: {self.dir} (v: {self.version})>"


def last_version(
    config: PumConfig,
    dir: str | Path = ".",
    after_version: str | None = None,
    before_version: str | None = None,
) -> str | None:
    """
    Return the last version of the changelogs.
    The changelogs are sorted by version.
    If after_current_version is True, only the changelogs that are after the current version will be returned.
    If after_current_version is False, all changelogs will be returned.

    Args:
        config (PumConfig): The configuration object.
        dir (str | Path): The directory where the changelogs are located.
        after_version (str | None): The version to start from.
        before_version (str | None): The version to end at.
    """
    changelogs = list_changelogs(config, dir, after_version, before_version)
    if not changelogs:
        return None
    if after_version:
        changelogs = [c for c in changelogs if c.version > parse_version(after_version)]
    if before_version:
        changelogs = [c for c in changelogs if c.version < parse_version(before_version)]
    if not changelogs:
        return None
    return changelogs[-1].version


def list_changelogs(
    config: PumConfig,
    dir: str | Path = ".",
    after_version: str | None = None,
    before_version: str | None = None,
) -> list:
    """
    Return a list of changelogs.
    The changelogs are sorted by version.
    If after_current_version is True, only the changelogs that are after the current version will be returned.
    If after_current_version is False, all changelogs will be returned.

    Args:
        config (PumConfig): The configuration object.
        dir (str | Path): The directory where the changelogs are located.
        after_version (str | None): The version to start from.
        before_version (str | None): The version to end at.
    """
    path = Path(dir)
    if not path.is_dir():
        raise PumException(f"Module directory `{path}` does not exist.")
    path = path / config.changelogs_directory
    if not path.is_dir():
        raise PumException(f"Changelogs directory `{path}` does not exist.")

    changelogs = [Changelog(path / d) for d in listdir(path) if isdir(join(path, d))]

    if after_version:
        changelogs = [c for c in changelogs if c.version > parse_version(after_version)]
    if before_version:
        changelogs = [c for c in changelogs if c.version < parse_version(before_version)]

    changelogs.sort(key=lambda c: c.version)
    return changelogs
