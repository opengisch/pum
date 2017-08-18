# -*- coding: utf-8 -*-

from __future__ import print_function

import subprocess

import psycopg2
import psycopg2.extras

from utils.exceptions import PgDumpError, PgRestoreError, DbConnectionError


class Dumper:
    """This class is used to dump and restore a Postgres database."""
    # TODO docstring

    def __init__(self, pg_service, file):
        self.file = file

        try:
            self.connection = psycopg2.connect("service={}".format(pg_service))
        except:
            raise DbConnectionError('Unable to connect to service {}'.format(
                pg_service))

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
        # print(result)
        # print(self.connection.get_dsn_parameters())
        cursor.close()

    def pg_backup(self, pg_dump_exe='pg_dump'):

        command = [
            pg_dump_exe, '-Fc', '-U', self.__get_dbuser(), '-f',
            self.file, self.__get_dbname()]

        try:
            subprocess.check_output(command, stderr=subprocess.STDOUT)

        except subprocess.CalledProcessError as e:
            raise PgDumpError(e.output)

    def pg_restore(self, pg_restore_exe='pg_restore'):

        command = [
            pg_restore_exe, '-U', self.__get_dbuser(), '-d',
            self.__get_dbname(), self.file]

        try:
            subprocess.check_output(command, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            raise PgRestoreError(e.output)
