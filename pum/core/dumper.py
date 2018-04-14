# -*- coding: utf-8 -*-

from __future__ import print_function

import subprocess
from distutils.version import LooseVersion


class Dumper:
    """This class is used to dump and restore a Postgres database."""

    def __init__(self, pg_service, file):
        self.file = file

        self.pg_service = pg_service

    def pg_backup(self, pg_dump_exe='pg_dump', exclude_schema=None):
        """Call the pg_dump command to create a db backup

        Parameters
        ----------
        pg_dump_exe: str
            the pg_dump command path
        exclude_schema: str[]
            list of schemas to be skipped
        """

        command = [
            pg_dump_exe, '-Fc', '-f', self.file,
            'service={}'.format(self.pg_service)
            ]
        if exclude_schema:
            command.append(' '.join("--exclude-schema={}".format(schema) for schema in exclude_schema))

        subprocess.check_output(command, stderr=subprocess.STDOUT)

    def pg_restore(self, pg_restore_exe='pg_restore', exclude_schema=None):
        """Call the pg_restore command to restore a db backup

        Parameters
        ----------
        pg_restore_exe: str
            the pg_restore command path
        """

        command = [
            pg_restore_exe, '-d',
            'service={}'.format(self.pg_service),
            '--no-owner'
            ]

        if exclude_schema:
            exclude_schema_available = False
            try:
                pg_version = subprocess.check_output(['pg_restore','--version'])
                pg_version = str(pg_version).replace('\\n', '').replace("'", '').split(' ')[-1]
                exclude_schema_available = LooseVersion(pg_version) >= LooseVersion("10.0")
            except subprocess.CalledProcessError as e:
                print("*** Could not get pg_restore version:\n", e.stderr)
            if exclude_schema_available:
                command.append(' '.join("--exclude-schema={}".format(schema) for schema in exclude_schema))
        command.append(self.file)

        try:
            subprocess.check_output(command)
        except subprocess.CalledProcessError as e:
            print("*** pg_restore failed:\n", command, '\n', e.stderr)
