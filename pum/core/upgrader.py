#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import re
import os
from os import listdir
from os.path import isfile, join, basename, dirname
from collections import OrderedDict
import psycopg2
import psycopg2.extras
from hashlib import md5
import importlib.util
import inspect
from enum import IntFlag

from .deltapy import DeltaPy


class Upgrader:
    """This class is used to upgrade an existing database using sql delta files.

    Stores the info about the upgrade in a table on the database."""

    def __init__(self, pg_service, upgrades_table, dirs, variables=None,
                 max_version=None):
        """Constructor

            Parameters
            ----------
            pg_service: str
                The name of the postgres service (defined in pg_service.conf)
                related to the db
            upgrades_table: str
                The name of the table (int the format schema.name) where the
                informations about the upgrades are stored
            dirs: list(str)
                The paths to directories where delta files are stored
            variables: dictionary
                dictionary for variables to be used in SQL deltas ( name => value )
            max_version: str
                Maximum (including) version to run the deltas up to.
        """
        self.pg_service = pg_service
        self.connection = psycopg2.connect("service={}".format(pg_service))
        self.cursor = self.connection.cursor()
        self.upgrades_table = upgrades_table
        self.dirs = dirs
        self.variables = variables
        self.max_version = max_version

    def run(self, verbose=False):
        if not self.exists_table_upgrades():
            raise UpgradesTableNotFoundError(self.upgrades_table)

        self.__run_pre_all()

        deltas = self.__get_delta_files()
        deltas = [delta for dirname in deltas for delta in deltas[dirname]]
        if not deltas:
            print('No delta files found')

        db_version = self.current_db_version()

        for d in deltas:
            if verbose:
                print('Found delta {}, version {}, type {}'.format(
                    d.get_name(), d.get_version(), d.get_type()))
                print('     Already applied: ', self.__is_applied(d))
                print('     Version greather or equal than current: ',
                      d.get_version() >= db_version)
            if not self.__is_applied(d) and d.get_version() >= db_version and \
                    (self.max_version is None or d.get_version() <= self.max_version):
                print('     Applying delta {} {}...'.format(
                    d.get_version(), d.get_name()), end=' ')

                if d.get_type() & DeltaType.SQL:
                    self.__run_delta_sql(d)
                    print('OK')
                elif d.get_type() & DeltaType.PYTHON:
                    self.__run_delta_py(d)
                    print('OK')
                else:
                    print('Delta not applied')
            else:
                if verbose:
                    print('Delta not applied')

        self.__run_post_all()

    def exists_table_upgrades(self):
        """Return if the upgrades table exists

        Returns
        -------
        bool
            True if the table exists
            False if the table don't exists"""

        query = """
            SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables
            WHERE  table_schema = '{}'
            AND    table_name = '{}'
            );
        """.format(self.upgrades_table[:self.upgrades_table.index('.')],
                   self.upgrades_table[self.upgrades_table.index('.')+1:])

        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def __get_dbname(self):
        """Return the db name."""
        return self.connection.get_dsn_parameters()['dbname']

    def __get_dbuser(self):
        """Return the db user"""
        return self.connection.get_dsn_parameters()['user']

    def __get_delta_files(self):
        """Search for delta files and return a dict of Delta objects, keyed by directory names."""
        files = [(d, f) for d in self.dirs for f in listdir(d) if isfile(join(d, f))]

        deltas = OrderedDict()
        for d, f in files:
            file_ = join(d, f)

            if not Delta.is_valid_delta_name(file_):
                continue

            delta = Delta(file_)

            if d not in deltas:
                deltas[d] = []
            deltas[d].append(delta)

        # sort delta objects in each bucket
        for d in deltas:
            deltas[d].sort(key=lambda x: (x.get_version(), x.get_priority(), x.get_name()))

        return deltas

    def __run_delta_sql(self, delta):
        """Execute the delta sql file on the database"""

        self.__run_sql_file(delta.get_file())
        self.__update_upgrades_table(delta)

    def __run_delta_py(self, delta):
        """Execute the delta py file"""

        self.__run_py_file(delta.get_file(), delta.get_name())
        self.__update_upgrades_table(delta)

    def __run_pre_all(self):
        """Execute the pre-all.py and pre-all.sql files if they exist"""

        # if the list of delta dirs is [delta1, delta2] the pre scripts of delta2 are
        # executed before the pre scripts of delta1

        for d in reversed(self.dirs):
            pre_all_py_path = os.path.join(d, 'pre-all.py')
            if os.path.isfile(pre_all_py_path):
                print('     Applying pre-all.py...', end=' ')
                self.__run_py_file(pre_all_py_path, 'pre-all')
                print('OK')

            pre_all_sql_path = os.path.join(d, 'pre-all.sql')
            if os.path.isfile(pre_all_sql_path):
                print('     Applying pre-all.sql...', end=' ')
                self.__run_sql_file(pre_all_sql_path)
                print('OK')

    def __run_post_all(self):
        """Execute the post-all.py and post-all.sql files if they exist"""

        # if the list of delta dirs is [delta1, delta2] the post scripts of delta1 are
        # executed before the post scripts of delta2

        for d in self.dirs:
            post_all_py_path = os.path.join(d, 'post-all.py')
            if os.path.isfile(post_all_py_path):
                print('     Applying post-all.py...', end=' ')
                self.__run_py_file(post_all_py_path, 'post-all')
                print('OK')

            post_all_sql_path = os.path.join(d, 'post-all.sql')
            if os.path.isfile(post_all_sql_path):
                print('     Applying post-all.sql...', end=' ')
                self.__run_sql_file(post_all_sql_path)
                print('OK')

    def __run_sql_file(self, filepath):
        """Execute the sql file at the passed path

        Parameters
        ----------
        filepath: str
            the path of the file to execute"""

        with open(filepath, 'r') as delta_file:
            sql = delta_file.read()
            if self.variables:
                self.cursor.execute(sql, self.variables)
            else:
                self.cursor.execute(sql)
            self.connection.commit()

    def __run_py_file(self, filepath, module_name):
        """Execute the python file at the passed path

        Parameters
        ----------
        filepath: str
            the path of the file to execute
        module_name: str
            the name of the python module
            """

        # Import the module
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        delta_py = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(delta_py)

        # Get the python file's directory path
        # Note: we add a separator for backward compatibility, as existing DeltaPy subclasses
        # may assume that delta_dir ends with a separator
        dir_ = dirname(filepath) + os.sep

        # Search for subclasses of DeltaPy
        for name in dir(delta_py):
            obj = getattr(delta_py, name)
            if inspect.isclass(obj) and not obj == DeltaPy and issubclass(
                    obj, DeltaPy):

                delta_py_inst = obj(
                    self.current_db_version(), dir_, self.dirs, self.pg_service,
                    self.upgrades_table, variables=self.variables)
                delta_py_inst.run()

    def show_info(self):
        """Print info about found delta files and about already made upgrades"""

        deltas = self.__get_delta_files()

        table = [['Version', 'Name', 'Type', 'Status']]

        for dir_ in deltas:
            print('delta files in dir: ', dir_)

            for delta in deltas[dir_]:
                line = [delta.get_version(), delta.get_name()]
                if delta.get_type() == DeltaType.PRE_PYTHON:
                    line.append('pre py')
                elif delta.get_type() == DeltaType.PRE_SQL:
                    line.append('pre sql')
                elif delta.get_type() == DeltaType.PYTHON:
                    line.append('delta py')
                elif delta.get_type() == DeltaType.SQL:
                    line.append('delta sql')
                elif delta.get_type() == DeltaType.POST_PYTHON:
                    line.append('post py')
                elif delta.get_type() == DeltaType.POST_SQL:
                    line.append('post sql')

                if self.__is_applied(delta):
                    line.append('Applied')
                else:
                    line.append('Pending')

                table.append(line)

        self.__print_table(table)

        print('')
        print('Applied upgrades in database')

        query = """SELECT
                version,
                description,
                type,
                installed_by,
                installed_on,
                success
                FROM {}
                """.format(self.upgrades_table)

        self.cursor.execute(query)
        records = self.cursor.fetchall()

        table = [['Version', 'Name', 'Type', 'Installed by', 'Installed on',
                  'Status']]

        for i in records:
            line = [str(i[0]), str(i[1])]
            delta_type = i[2]
            if delta_type == 0:
                line.append('baseline')
            elif delta_type == DeltaType.PRE_PYTHON:
                line.append('pre py')
            elif delta_type == DeltaType.PRE_SQL:
                line.append('pre sql')
            elif delta_type == DeltaType.PYTHON:
                line.append('delta py')
            elif delta_type == DeltaType.SQL:
                line.append('delta sql')
            elif delta_type == DeltaType.POST_PYTHON:
                line.append('post py')
            elif delta_type == DeltaType.POST_SQL:
                line.append('post sql')

            line.append(str(i[3]))
            line.append(str(i[4]))

            success = str(i[5])
            if success == 'True':
                line.append('Success')
            else:
                line.append('Failed')

            table.append(line)

        self.__print_table(table)

    @staticmethod
    def __print_table(table):
        """Print a list in tabular format
        Based on https://stackoverflow.com/a/8356620"""

        col_width = [max(len(x) for x in col) for col in zip(*table)]
        print("| " + " | ".join("{:{}}".format(x, col_width[i])
                                for i, x in enumerate(table[0])) + " |")
        print("| " + " | ".join("{:{}}".format('-' * col_width[i], col_width[i])
                                for i, x in enumerate(table[0])) + " |")
        for line in table[1:]:
            print("| " + " | ".join("{:{}}".format(x, col_width[i])
                                    for i, x in enumerate(line)) + " |")

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
            AND checksum = '{}'
            AND success = 'TRUE'
        """.format(
            self.upgrades_table, delta.get_version(), delta.get_checksum())

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
            self.upgrades_table, delta.get_version(), delta.get_name(),
            delta.get_type(), delta.get_file(), delta.get_checksum(),
            self.__get_dbuser())

        self.cursor.execute(query)
        self.connection.commit()

    def create_upgrades_table(self):
        """Create the upgrades information table"""

        query = """CREATE TABLE IF NOT EXISTS {}
                (
                id serial NOT NULL,
                version character varying(50),
                description character varying(200) NOT NULL,
                type integer NOT NULL,
                script character varying(1000) NOT NULL,
                checksum character varying(32) NOT NULL,
                installed_by character varying(100) NOT NULL,
                installed_on timestamp without time zone NOT NULL DEFAULT now(),
                execution_time integer NOT NULL,
                success boolean NOT NULL,
                PRIMARY KEY (id)
                )
        """.format(self.upgrades_table)

        self.cursor.execute(query)
        self.connection.commit()

    def set_baseline(self, version):
        """Set the baseline into the creation information table

        version: str
            The version of the current database to set in the information
            table. The baseline must be in the format x.x.x where x are numbers.
        """
        pattern = re.compile(r"^\d+\.\d+\.\d+$")
        if not re.match(pattern, version):
            raise ValueError('Wrong version format')

        query = """
                INSERT INTO {} (
                    version,
                    description,
                    type,
                    script,
                    checksum,
                    installed_by,
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
                ) """.format(self.upgrades_table, version, 'baseline', 0,
                             '', '', self.__get_dbuser())
        self.cursor.execute(query)
        self.connection.commit()

    def current_db_version(self):
        """Read the upgrades information table and return the current db
        version

        Returns
        -------
        str
            the current db version
        """

        query = """
        SELECT version from {} WHERE success = TRUE ORDER BY version DESC
        """.format(self.upgrades_table)

        self.cursor.execute(query)

        return self.cursor.fetchone()[0]


class DeltaType(IntFlag):
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

    def __repr__(self):
        return '<file: {f} (v: {v}, n: {n}>'.format(f=self.file, v=self.get_version(), n=self.get_name())

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
            return DeltaType.PRE_PYTHON
        elif ext == 'pre.sql':
            return DeltaType.PRE_SQL
        elif ext == 'py':
            return DeltaType.PYTHON
        elif ext == 'sql':
            return DeltaType.SQL
        elif ext == 'post.py':
            return DeltaType.POST_PYTHON
        elif ext == 'post.sql':
            return DeltaType.POST_SQL

    def get_priority(self) -> int:
        """
        Rerturns the priority of the file from 1 (pre) to 3 (post)
        :return: the priority
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
