# -*- coding: utf-8 -*-

from __future__ import print_function

import subprocess

import psycopg2
import psycopg2.extras


class Dumper:
    """This class is used to dump and restore a Postgres database."""
    # TODO docstring

    def __init__(self, pg_service, file):
        self.file = file

        self.connection = psycopg2.connect("service={}".format(pg_service))

    def __get_dbname(self):
        return self.connection.get_dsn_parameters()['dbname']

    def __get_dbuser(self):
        return self.connection.get_dsn_parameters()['user']

    def pg_backup(self, pg_dump_exe='pg_dump'):

        command = [
            pg_dump_exe, '-Fc', '-U', self.__get_dbuser(), '-f',
            self.file, self.__get_dbname()]

        subprocess.check_output(command, stderr=subprocess.STDOUT)

    def pg_restore(self, pg_restore_exe='pg_restore'):

        command = [
            pg_restore_exe, '-U', self.__get_dbuser(), '-d',
            self.__get_dbname(), self.file]

        subprocess.check_output(command, stderr=subprocess.STDOUT)
