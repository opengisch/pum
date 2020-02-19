# -*- coding: utf-8 -*-

import sys
import subprocess
from distutils.version import LooseVersion

from pum.core.exceptions import PgDumpFailed, PgDumpCommandError, PgRestoreFailed, PgRestoreCommandError


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
            command.insert(-1, ' '.join("--exclude-schema={}".format(schema) for schema in exclude_schema))

        try:
            if sys.version_info[1] < 7:
                output = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else:
                output = subprocess.run(command, capture_output=True, text=True)
            if output.returncode != 0:
                raise PgDumpFailed(output.stderr)
        except TypeError as e:
            raise PgDumpCommandError('invalid command: {}'.format(' '.join(filter(None, command))))

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
            if sys.version_info[1] < 7:
                output = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else:
                output = subprocess.run(command, capture_output=True, text=True)
            if output.returncode != 0:
                raise PgRestoreFailed(output.stderr)
        except TypeError as e:
            raise PgRestoreCommandError('invalid command: {}'.format(' '.join(filter(None, command))))
