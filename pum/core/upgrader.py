#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import re
from os import listdir
from os.path import isfile, join, basename
import psycopg2
import psycopg2.extras
from hashlib import md5

class Upgrader():
    """This class is used to upgrade an existing database using sql delta files.
    
    Stores the info about the upgrade in a table on the database."""

    def __init__(self, pg_service, upgrades_table, dir):
        """Constructor

            Parameters
            ----------
            pg_service: string
                The name of the postgres service (defined in pg_service.conf) related to the db
            upgrades_table: sting
                The name of the table (int the format schema.name) where the information about the upgrades are stored
            dir: string
                The path of the directory where the delta files are stored
        """
        self.connection = psycopg2.connect("service={}".format(pg_service))
        self.cursor = self.connection.cursor()
        self.upgrades_table = upgrades_table
        self.dir = dir

    def run(self, verbose=False):
        if not self.exists_table_upgrades():
            raise UpgradesTableNotFoundError(self.upgrades_table)
            
        deltas = self.__get_delta_files()
        for d in deltas:
            if verbose:
                print('Found delta {}, version {}, type {}'.format(d.get_name(), d.get_version(), d.get_type()))
                print('     Already applied: ',self.__is_applied(d))
                print('     Version greather or equal than current: ', self.__is_version_greater_or_equal_than_current(d.get_version()))
            if (not self.__is_applied(d)) and (self.__is_version_greater_or_equal_than_current(d.get_version())):
                print('     Applying delta {} {}...'.format(d.get_version(), d.get_type()), end=' ')
                self.__run_delta(d)
                print('OK')
            else:
                if verbose:
                    print('Delta not applied')

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
        """Search for delta files and return a list of Delta objects."""
        files_in_dir = [f for f in listdir(self.dir) if isfile(join(self.dir, f))]

        deltas = []
        for i in files_in_dir:
            file = join(self.dir, i)

            if not Delta.is_valid_delta_name(file):
                continue

            delta = Delta(file)
            deltas.append(delta)

        return sorted(deltas, key = lambda x: (x.get_version(), x.get_type()))

    def __run_delta(self, delta):
        """Execute the delta file on the database"""
        delta_file = open(delta.get_file(), 'r')
        self.cursor.execute(delta_file.read())
        self.connection.commit()
        self.__update_upgrades_table(delta)

    def show_info(self):
        """Print info about delta file and about already made upgrade"""
        deltas = self.__get_delta_files()
        print('delta files in dir: ', self.dir)
        table = []
        table.append(['Version', 'Name', 'Type', 'Status'])

        for delta in deltas:
            line = []
            line.append(delta.get_version())
            line.append(delta.get_name())
            if delta.get_type() == Delta.PRE:
                line.append('pre')
            elif delta.get_type() == Delta.DELTA:
                line.append('delta')
            elif delta.get_type() == Delta.POST:
                line.append('post')

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

        table = []
        table.append(['Version', 'Name', 'Type', 'Installed by', 'Installed on', 'Status'])

        for i in records:
            line = []
            line.append(str(i[0]))
            line.append(str(i[1]))
            type = i[2]
            if type == 0:
                 line.append('baseline')
            elif type == Delta.PRE:
                line.append('pre')
            elif type == Delta.DELTA:
                line.append('delta')
            elif type == Delta.POST:
                line.append('post')

            line.append(str(i[3]))
            line.append(str(i[4]))

            success = str(i[5])
            if success == 'True':
                line.append('Success')
            else:
                line.append('Failed')

            table.append(line)

        self.__print_table(table)

    """Based on https://stackoverflow.com/a/8356620"""
    def __print_table(self, table):
        """Print a list in tabular format"""
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
        """.format(self.upgrades_table, delta.get_version(), delta.get_checksum())

        self.cursor.execute(query)
        if self.cursor.fetchone() == None:
            return False
        else:
            return True

    def __update_upgrades_table(self, delta):
        #TODO docstring
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
        ) """.format(self.upgrades_table, delta.get_version(), delta.get_name(), delta.get_type(),
                     delta.get_file(), delta.get_checksum(), self.__get_dbuser())

        self.cursor.execute(query)
        self.connection.commit()

    def create_upgrades_table(self):
        #TODO docstring
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
                CONSTRAINT upgrades_pk PRIMARY KEY (id)
                )
        """.format(self.upgrades_table)

        self.cursor.execute(query)
        self.connection.commit()
        
    def set_baseline(self, version):
        #TODO docstring
        #TODO test if version in < of existing version
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

    def __is_version_greater_or_equal_than_current(self, version):
        #TODO docstring
        query = """
        SELECT version from {} WHERE success = TRUE ORDER BY version DESC        
        """.format(self.upgrades_table)

        self.cursor.execute(query)

        if version >= self.cursor.fetchone()[0]:
            return True
        return False


class Delta():
    """This class represent a delta file."""

    # BASELINE = 0
    PRE = 1
    DELTA = 2
    POST = 3


    @staticmethod
    def is_valid_delta_name(file):
        """Return if a file has a valid name
        
        A delta file name must be:
        delta_x.x.x_ddmmyyyy.sql or 
        delta_x.x.x_ddmmyyyy.sql.post or 
        delta_x.x.x_ddmmyyyy.sql.pre
        
        where x.x.x is the version number and _ddmmyyyy is an optional description, usually representing the date 
        of the delta file
        """
        filename = basename(file)
        pattern = re.compile(r"^(delta_\d+\.\d+\.\d+).*(\.sql\.pre|\.sql|\.sql\.post)$")
        if re.match(pattern, filename):
            return True
        return False

    def __init__(self, file):
        self.file = file
        filename = basename(self.file)
        pattern = re.compile(r"^(delta_)(\d+\.\d+\.\d+)(_*)(.*)(\.sql\.pre|\.sql|\.sql\.post)$")
        self.match = re.match(pattern, filename)

    def get_version(self):
        """Return the version of the delta file."""
        return self.match.group(2)

    def get_name(self):
        """Return the name (description) of the delta file."""
        return self.match.group(4)

    def get_checksum(self):
        """Return the md5 checksum of the delta file."""
        return md5(open(self.file, 'rb').read()).hexdigest()

    def get_type(self):
        """Return the type of the delta file.
        
        Returns
        -------
        type: int
            1 for pre file
            2 for normal delta file
            3 for post file
        """
        ext =  self.match.group(5)

        if ext == '.sql.pre':
            return Delta.PRE
        elif ext == '.sql':
            return Delta.DELTA
        elif ext == '.sql.post':
            return Delta.POST

    def get_file(self):
        return self.file

class UpgradesTableNotFoundError(LookupError):
    '''raise this when Upgrades table is not present'''

if __name__ == "__main__":
    """
    Main process
    """
    #TODO add option to create upgrades table

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pg_service', help='Name of the postgres service', required=True)
    parser.add_argument('-t', '--table', help='Version table', required=True)
    parser.add_argument('-d', '--dir', help='Delta directory', required=True)
    parser.add_argument('-i', '--info', help='Show only info', action='store_true')
    parser.add_argument('-b', '--baseline', help='Create baseline')
    args = parser.parse_args()

    db_upgrader = Upgrader(args.pg_service, args.table, args.dir)


    if args.info:
        db_upgrader.show_info()
    elif args.baseline:
        db_upgrader.create_upgrades_table()
        db_upgrader.set_baseline(args.baseline)
        print('Created table upgrades with baseline')
    else:
        if not db_upgrader.exists_table_upgrades():
            print('Table upgrades don\'t exists. Run upgrader.py -b BASELINE to create')
        else:
            print('Running upgrader')
            db_upgrader.run(verbose=True)
            print('Upgrader done')
