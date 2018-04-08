# -*- coding: utf-8 -*-

from __future__ import print_function

import subprocess


class Dumper:
    """This class is used to dump and restore a Postgres database."""

    def __init__(self, pg_service, file):
        self.file = file

        self.pg_service = pg_service

    def pg_backup(self, pg_dump_exe='pg_dump', skip_schemas=None):
        """Call the pg_dump command to create a db backup

        Parameters
        ----------
        pg_dump_exe: str
            the pg_dump command path
        skip_schemas: str[]
            list of schemas to be skipped
        """

        command = [
            pg_dump_exe, '-Fc', '-f', self.file,
            'service={}'.format(self.pg_service)
        ]
        for schema in skip_schemas or []:
            command.append("--exclude-schema={}".format(schema))

        subprocess.check_output(command, stderr=subprocess.STDOUT)

    def pg_restore(self, pg_restore_exe='pg_restore'):
        """Call the pg_restore command to restore a db backup

        Parameters
        ----------
        pg_restore_exe: str
            the pg_restore command path
        """

        command = [
            pg_restore_exe, '-d',
            'service={}'.format(self.pg_service),
            '--exit-on-error',
            '--no-owner',
            self.file]

        try:
            subprocess.check_output(command)
        except subprocess.CalledProcessError as e:
            print("*** pg_restore failed:\n", e.stderr)
