from os.path import basename
from packaging.version import parse as parse_version


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
