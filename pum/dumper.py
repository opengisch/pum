#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import os
import subprocess
from datetime import date
import psycopg2
import psycopg2.extras

class Dumper():
    """This class is used to dump and restore a Postgres database."""

    def __init__(self, pg_service, file):
        self.connection = psycopg2.connect("service={}".format(pg_service))
        self.file = file

    def __get_dbname(self):
        return self.connection.get_dsn_parameters()['dbname']

    def __get_dbuser(self):
        return self.connection.get_dsn_parameters()['user']

    def __print_connection_info(self):

        cursor = self.connection.cursor()
        cursor.execute('select version() as version '
                       ', current_database() as db '
                       ', current_user as user '
                       ', now()::text as query_time '
                       )
        result = cursor.fetchone()
        print(result)
        print(self.connection.get_dsn_parameters())
        cursor.close()

    def pg_backup(self):
        #TODO test on Windows and OSX

        #pg_dump_exe = 'C:\\Program Files\\PostgreSQL\\9.3\\bin\\pg_dump.exe'
        pg_dump_exe = 'pg_dump'

        command = []
        command.append(pg_dump_exe)
        command.append('-Fc')
        command.append('-U')
        command.append(self.__get_dbuser())
        command.append('-f')
        command.append(self.file)
        command.append(self.__get_dbname())

        try:
            subprocess.check_output(command, stderr=subprocess.STDOUT)

        except subprocess.CalledProcessError as e:
            print('pg_dump failed', e.output)
            raise SystemExit(1)


    def pg_restore(self):
        # TODO test on Windows and OSX

        # pg_restore_exe = 'C:\\Program Files\\PostgreSQL\\9.3\\bin\\pg_restore.exe'
        pg_restore_exe = 'pg_restore'

        command = []
        command.append(pg_restore_exe)
        command.append('-U')
        command.append(self.__get_dbuser())
        command.append('-d')
        command.append(self.__get_dbname())
        command.append(self.file)

        try:
            subprocess.check_output(command, stderr=subprocess.STDOUT)

        except subprocess.CalledProcessError as e:
            print('pg_restore failed', e.output)
            raise SystemExit(1)


if __name__ == "__main__":
    """
    Main process
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pg_service', help='Name of the postgres service', required=True)
    parser.add_argument('-d', '--dump', help='Make a backup file of the database', action='store_true')
    parser.add_argument('-r', '--restore', help='Restore the db from the backup file', action='store_true')
    parser.add_argument('file', help='The backup file')
    args = parser.parse_args()
    #TODO options dump and restore cannot be true together

    db_dumper = Dumper(args.pg_service, args.file)

    if args.dump:
        print('Creating db backup in {}... '.format(args.file), end='')
        db_dumper.pg_backup()
        print('OK')
    elif args.restore:
        print('Restoring db backup... ', end='')
        db_dumper.pg_restore()
        print('OK')

