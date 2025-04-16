#!/usr/bin/env python

import importlib.util
import inspect
import logging
import os
import re
from hashlib import md5
from os import listdir
from os.path import basename, dirname, isdir, isfile, join
from pathlib import Path

import psycopg
from packaging.version import parse as parse_version
from psycopg import Connection

from pum.config import PumConfig
from pum.exceptions import PumException
from pum.schema_migrations import SchemaMigrations
from pum.utils.execute_sql import execute_sql

logger = logging.getLogger(__name__)


class Changelog:
    """This class represent a changelog directory."""

    def __init__(self, dir):
        self.dir = dir
        self.version = parse_version(basename(dir))

    def __repr__(self):
        return f"<dir: {self.dir} (v: {self.version})>"


class Upgrader:
    """This class is used to upgrade an existing database using sql delta files.

    Stores the info about the upgrade in a table on the database."""

    def __init__(
        self,
        pg_service: str,
        config: PumConfig,
        variables=None,
        dir: str | Path = ".",
        max_version=None,
    ):
        """
        Initialize the Upgrader class.
        This class is used to install a new instance or to upgrade an existing instance of a module.
        Stores the info about the upgrade in a table on the database.
        The table is created in the schema defined in the config file if it does not exist.

        Args:
        pg_service: str
            The name of the postgres service (defined in pg_service.conf)
            related to the db
        config: PumConfig
            The configuration object
        variables: dict
            dictionary for variables to be used in SQL deltas ( name => value )
        dir: str | Path
            The directory where the module is located.
        max_version: str
            Maximum (including) version to run the deltas up to.
        """

        self.pg_service = pg_service
        self.config = config
        self.variables = variables
        self.max_version = parse_version(max_version) if max_version else None
        self.schema_migrations = SchemaMigrations(self.config)
        self.dir = dir

    def install(self):
        """Installs the given module"""

        with psycopg.connect(f"service={self.pg_service}") as conn:
            if self.schema_migrations.exists(conn):
                raise PumException(
                    "Schema migrations table already exists. Use upgrade() to upgrade the db or start with a clean db."
                )
            self.schema_migrations.create(conn, commit=False)
            for changelog in self.changelogs(after_current_version=False):
                self.__apply_changelog(conn, changelog, commit=False)
                self.schema_migrations.set_baseline(
                    conn=conn,
                    version=changelog.version,
                    beta_testing=False,
                    commit=False,
                )
            logger

    def run(self, verbose=False):
        if not self.exists_table_upgrades():
            raise UpgradesTableNotFoundError(self.upgrades_table)

        self.__run_pre_all()

        deltas = self.__get_delta_files()
        deltas = [delta for dirname in deltas for delta in deltas[dirname]]
        if not deltas:
            print("No delta files found")

        db_version = self.current_db_version()

        for d in deltas:
            if verbose:
                print(
                    "Found delta {}, version {}, type {}".format(
                        d.get_name(), d.get_version(), d.get_type()
                    )
                )
                print("     Already applied: ", self.__is_applied(d))
                print(
                    "     Version greather or equal than current: ",
                    d.get_version() >= db_version,
                )
            if (
                not self.__is_applied(d)
                and d.get_version() >= db_version
                and (self.max_version is None or d.get_version() <= self.max_version)
            ):
                print(
                    "     Applying delta {} {}...".format(
                        d.get_version(), d.get_name()
                    ),
                    end=" ",
                )

                if d.get_type() & DeltaType.SQL:
                    self.__run_delta_sql(d)
                    print("OK")
                elif d.get_type() & DeltaType.PYTHON:
                    self.__run_delta_py(d)
                    print("OK")
                else:
                    print("Delta not applied")
            else:
                if verbose:
                    print("Delta not applied")

        self.__run_post_all()

    def __get_dbname(self):
        """Return the db name."""
        return self.connection.get_dsn_parameters()["dbname"]

    def __get_dbuser(self):
        """Return the db user"""
        return self.connection.get_dsn_parameters()["user"]

    def changelogs(self, after_current_version: bool = True) -> list[Changelog]:
        """
        Return a list of changelogs.
        The changelogs are sorted by version.
        If after_current_version is True, only the changelogs that are after the current version will be returned.
        If after_current_version is False, all changelogs will be returned.
        """
        path = Path(self.dir)
        if not path.is_dir():
            raise PumException(f"Module directory `{path}` does not exist.")
        path = path / self.config.changelogs_directory
        if not path.is_dir():
            raise PumException(f"Changelogs directory `{path}` does not exist.")

        changelogs = [
            Changelog(path / d) for d in os.listdir(path) if isdir(join(path, d))
        ]

        if after_current_version:
            changelogs = [
                c
                for c in changelogs
                if c.version > self.schema_migrations.current_version()
            ]

        changelogs.sort(key=lambda c: c.version)
        return changelogs

    def changelog_files(self, changelog: str) -> list[Path]:
        """
        Get the list of changelogs and return a list of pathes.
        This is not recursive, it only returns the files in the given changelog directory.
        """
        files = [
            changelog.dir / f
            for f in os.listdir(changelog.dir)
            if (changelog.dir / f).is_file()
        ]
        files.sort()
        return files

    def __apply_changelog(
        self, conn: Connection, changelog: Changelog, commit: bool = True
    ):
        """
        Apply a changelog
        This will execute all the files in the changelog directory.
        The changelog directory is the one that contains the delta files.

        Args:
            conn: Connection
                The connection to the database
            changelog: Changelog
                The changelog to apply
            commit: bool
                If true, the transaction is committed. The default is true.
        """
        files = self.changelog_files(changelog)
        for file in files:
            execute_sql(conn=conn, sql=file, commit=commit)

    def __run_delta_sql(self, delta):
        """Execute the delta sql file on the database"""

        self.__run_sql_file(delta.get_file())
        self.__update_upgrades_table(delta)

    def __run_pre_all(self):
        """Execute the pre-all.py and pre-all.sql files if they exist"""

        # if the list of delta dirs is [delta1, delta2] the pre scripts of delta2 are
        # executed before the pre scripts of delta1

        for d in reversed(self.dirs):
            pre_all_sql_path = os.path.join(d, "pre-all.sql")
            if os.path.isfile(pre_all_sql_path):
                print("     Applying pre-all.sql...", end=" ")
                self.__run_sql_file(pre_all_sql_path)
                print("OK")

    def __run_post_all(self):
        """Execute the post-all.py and post-all.sql files if they exist"""

        # if the list of delta dirs is [delta1, delta2] the post scripts of delta1 are
        # executed before the post scripts of delta2

        for d in self.dirs:
            post_all_sql_path = os.path.join(d, "post-all.sql")
            if os.path.isfile(post_all_sql_path):
                print("     Applying post-all.sql...", end=" ")
                self.__run_sql_file(post_all_sql_path)
                print("OK")

    def __run_sql_file(self, filepath):
        """Execute the sql file at the passed path

        Parameters
        ----------
        filepath: str
            the path of the file to execute"""

        with open(filepath) as delta_file:
            sql = delta_file.read()
            if self.variables:
                self.cursor.execute(sql, self.variables)
            else:
                self.cursor.execute(sql)
            self.connection.commit()

    def show_info(self):
        """Print info about found delta files and about already made upgrades"""

        deltas = self.__get_delta_files()

        table = [["Version", "Name", "Type", "Status"]]

        for dir_ in deltas:
            print("delta files in dir: ", dir_)

            for delta in deltas[dir_]:
                line = [str(delta.get_version()), delta.get_name()]
                if delta.get_type() == DeltaType.PRE_PYTHON:
                    line.append("pre py")
                elif delta.get_type() == DeltaType.PRE_SQL:
                    line.append("pre sql")
                elif delta.get_type() == DeltaType.PYTHON:
                    line.append("delta py")
                elif delta.get_type() == DeltaType.SQL:
                    line.append("delta sql")
                elif delta.get_type() == DeltaType.POST_PYTHON:
                    line.append("post py")
                elif delta.get_type() == DeltaType.POST_SQL:
                    line.append("post sql")

                if self.__is_applied(delta):
                    line.append("Applied")
                else:
                    line.append("Pending")

                table.append(line)

        self.__print_table(table)

        print("")
        print("Applied upgrades in database")

        query = """SELECT
                version,
                description,
                type,
                installed_by,
                installed_on,
                success
                FROM {}
                """.format(
            self.upgrades_table
        )

        self.cursor.execute(query)
        records = self.cursor.fetchall()

        table = [["Version", "Name", "Type", "Installed by", "Installed on", "Status"]]

        for i in records:
            line = [str(i[0]), str(i[1])]
            delta_type = i[2]
            if delta_type == 0:
                line.append("baseline")
            elif delta_type == DeltaType.PRE_PYTHON:
                line.append("pre py")
            elif delta_type == DeltaType.PRE_SQL:
                line.append("pre sql")
            elif delta_type == DeltaType.PYTHON:
                line.append("delta py")
            elif delta_type == DeltaType.SQL:
                line.append("delta sql")
            elif delta_type == DeltaType.POST_PYTHON:
                line.append("post py")
            elif delta_type == DeltaType.POST_SQL:
                line.append("post sql")

            line.append(str(i[3]))
            line.append(str(i[4]))

            success = str(i[5])
            if success == "True":
                line.append("Success")
            else:
                line.append("Failed")

            table.append(line)

        self.__print_table(table)

    @staticmethod
    def __print_table(table):
        """Print a list in tabular format
        Based on https://stackoverflow.com/a/8356620"""

        col_width = [max(len(x) for x in col) for col in zip(*table)]
        print(
            "| "
            + " | ".join(
                "{:{}}".format(x, col_width[i]) for i, x in enumerate(table[0])
            )
            + " |"
        )
        print(
            "| "
            + " | ".join(
                "{:{}}".format("-" * col_width[i], col_width[i])
                for i, x in enumerate(table[0])
            )
            + " |"
        )
        for line in table[1:]:
            print(
                "| "
                + " | ".join(
                    "{:{}}".format(x, col_width[i]) for i, x in enumerate(line)
                )
                + " |"
            )

    def __is_applied(self, delta):
        """Verifies if delta file is already applied on database

        Parameters
        ----------
        delta: Delta object
            The delta object representing the delta file

        Returns
        -------
        bool
            True if the delta is already applied on the db
            False otherwise
        """
        query = """
        SELECT id FROM {}
        WHERE version = '{}'
            AND description = '{}'
            AND type = '{}'
            AND success = 'TRUE'
        """.format(
            self.upgrades_table, delta.get_version(), delta.get_name(), delta.get_type()
        )

        self.cursor.execute(query)
        if not self.cursor.fetchone():
            return False
        else:
            return True

    def __update_upgrades_table(self, delta):
        """Add a new record into the upgrades information table about the
        applied delta

        Parameters
        ----------
        delta: Delta
            the applied delta file"""

        query = """
        INSERT INTO {} (
            --id,
            version,
            description,
            type,
            script,
            checksum,
            installed_by,
            --installed_on,
            execution_time,
            success
        ) VALUES(
            '{}',
            '{}',
            {},
            '{}',
            '{}',
            '{}',
            1,
            TRUE
        ) """.format(
            self.upgrades_table,
            delta.get_version(),
            delta.get_name(),
            delta.get_type(),
            delta.get_file(),
            delta.get_checksum(),
            self.__get_dbuser(),
        )

        self.cursor.execute(query)
        self.connection.commit()


class DeltaType:
    PRE = 1
    POST = 2

    PYTHON = 4
    SQL = 8

    PRE_PYTHON = PRE | PYTHON
    PRE_SQL = PRE | SQL

    POST_PYTHON = POST | PYTHON
    POST_SQL = POST | SQL


class Delta:
    """This class represent a delta file."""

    FILENAME_PATTERN = (
        r"^(delta_)(\d+\.\d+\.\d+)(_*)(\w*)\."
        r"(pre\.sql|sql|post\.sql|pre\.py|py|post\.py)$"
    )

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

    def __repr__(self):
        return f"<file: {self.file} (v: {self.get_version()}, n: {self.get_name()}>"

    def get_version(self):
        """Return the version of the delta file."""
        return pkg_resources.parse_version(self.match.group(2))

    def get_name(self):
        """Return the name (description) of the delta file."""
        return self.match.group(4)

    def get_checksum(self):
        """Return the md5 checksum of the delta file."""
        with open(self.file, "rb") as f:
            cs = md5(f.read()).hexdigest()
        return cs

    def get_type(self):
        """Return the type of the delta file.

        Returns
        -------
        type: int
        """

        ext = self.match.group(5)

        if ext == "pre.py":
            return DeltaType.PRE_PYTHON
        elif ext == "pre.sql":
            return DeltaType.PRE_SQL
        elif ext == "py":
            return DeltaType.PYTHON
        elif ext == "sql":
            return DeltaType.SQL
        elif ext == "post.py":
            return DeltaType.POST_PYTHON
        elif ext == "post.sql":
            return DeltaType.POST_SQL

    def get_priority(self) -> int:
        """Return the priority of the file from 1 (pre) to 3 (post)

        Returns
        -------
        type: int
        """
        dtype = self.get_type()
        if dtype & DeltaType.PRE:
            return 1
        elif dtype & DeltaType.POST:
            return 3
        else:
            return 2

    def get_file(self):
        return self.file


class UpgradesTableNotFoundError(Exception):
    """raise this when Upgrades table is not present"""
