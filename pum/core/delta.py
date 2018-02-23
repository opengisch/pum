# -*- coding: utf-8 -*-

import re

from hashlib import md5
from os.path import basename


class Delta:
    """This class represent a delta file."""

    DELTA_PRE_PY = 0
    DELTA_PRE_SQL = 1
    DELTA_PY = 2
    DELTA_SQL = 3
    DELTA_POST_PY = 4
    DELTA_POST_SQL = 5

    FILENAME_PATTERN = (
        r"^(delta_)(\d+\.\d+\.\d+)(_*)(\w*)\."
        r"(pre\.sql|sql|post\.sql|pre\.py|py|post\.py)$")

    @staticmethod
    def is_valid_delta_name(file):
        """Return if a file has a valid name

        A delta file name can be:
        - pre-all.py
        - pre-all.sql
        - delta_x.x.x_ddmmyyyy.pre.py
        - delta_x.x.x_ddmmyyyy.pre.sql
        - delta_x.x.x_ddmmyyyy.py
        - delta_x.x.x_ddmmyyyy.sql
        - delta_x.x.x_ddmmyyyy.post.py
        - delta_x.x.x_ddmmyyyy.post.sql
        - post-all.py
        - post-all.sql

        where x.x.x is the version number and _ddmmyyyy is an optional
        description, usually representing the date of the delta file
        """
        filename = basename(file)
        pattern = re.compile(Delta.FILENAME_PATTERN)
        if re.match(pattern, filename):
            return True
        return False

    def __init__(self, file):
        self.file = file
        filename = basename(self.file)
        pattern = re.compile(self.FILENAME_PATTERN)
        self.match = re.match(pattern, filename)

    def get_version(self):
        """Return the version of the delta file."""
        return self.match.group(2)

    def get_name(self):
        """Return the name (description) of the delta file."""
        return self.match.group(4)

    def get_checksum(self):
        """Return the md5 checksum of the delta file."""
        with open(self.file, 'rb') as f:
            cs = md5(f.read()).hexdigest()
        return cs

    def get_type(self):
        """Return the type of the delta file.

        Returns
        -------
        type: int
        """

        ext = self.match.group(5)

        if ext == 'pre.py':
            return Delta.DELTA_PRE_PY
        elif ext == 'pre.sql':
            return Delta.DELTA_PRE_SQL
        elif ext == 'py':
            return Delta.DELTA_PY
        elif ext == 'sql':
            return Delta.DELTA_SQL
        elif ext == 'post.py':
            return Delta.DELTA_POST_PY
        elif ext == 'post.sql':
            return Delta.DELTA_POST_SQL

    def get_file(self):
        return self.file
