#!/usr/bin/env python

from __future__ import print_function

import argparse
import os

import psycopg2
import psycopg2.extras
import yaml
from pum.checker import Checker
from pum.upgrader import Upgrader, UpgradesTableNotFoundError

from pum.core.dumper import Dumper


class Manager():
    """This class is used to managing qwat upgrade procedure."""

    def __init__(self, pg_service_prod, pg_service_test, pg_service_comp,
                 config_file='conf/pum_config.yaml'):
        """Create the manager class instance.

            Parameters
            ----------
            pg_service_prod: basestring
                The name of the postgres service (defined in pg_service.conf) related to the production db
            pg_service_test: string
                The name of the postgres service (defined in pg_service.conf) related to the test db (copy of the 
                production db where the delta are applied for testing the procedure)
            pg_service_comp: basestring
                The name of the postgres service (defined in pg_service.conf) related to the comparation db
                (db generated with init_qwat.sh script, used to verifiy if db test is correct)
        """
        self.pg_service_prod = pg_service_prod
        self.pg_service_test = pg_service_test
        self.pg_service_comp = pg_service_comp
        self.config_file = config_file
        self.load_config()

    def load_config(self):
        """Load the configurations from yaml configuration file and store it to instance variables."""
        config = yaml.safe_load(open(self.config_file))
        self.upgrades_table = config['upgrades_table']
        self.delta_dir = config['delta_dir']
        self.backup_file = config['backup_file']
        self.ignore_list = config['ignore_elements']

    def run(self):
        """Run the qwat upgrade procedure:
            - check if the upgrades table exists in PG_SERVICE_PROD, if not, ask the user if want to create it
              and set the baseline of the table with the current version founded in *qwat_sys.versions*
            - createe a dump of the PG_SERVICE_PROD db
            - restore the db dump into PG_SERVICE_TEST
            - apply the delta files found in the delta directory to the PG_SERVICE_TEST db. Only the delta 
              files with version greater or equal than the current version are to be applied
            - create PG_SERVICE_COMP whit the last qwat db version, using init_qwat.sh script
            - check if there are differences between PG_SERVICE_TEST and PG_SERVICE_COMP
            - if no significant differences are found, apply the delta files to PG_SERVICE_PROD. Only the delta 
              files with version greater or equal than the current version are to be applied
        """
        upgrader_prod = Upgrader(self.pg_service_prod, self.upgrades_table, self.delta_dir)
        if not upgrader_prod.exists_table_upgrades():
            self.__ask_create_upgrades_table(upgrader_prod)
            self.__set_baseline(self.pg_service_prod, upgrader_prod)

        print('Creating db backup in {}... '.format(self.backup_file), end='')
        dumper = Dumper(self.pg_service_prod, self.backup_file)
        dumper.pg_backup()
        print(Bcolors.OKGREEN + 'OK' + Bcolors.ENDC)

        print('Restoring backup on db_test... ', end='')
        dumper = Dumper(self.pg_service_test, self.backup_file)
        dumper.pg_restore()
        print(Bcolors.OKGREEN + 'OK' + Bcolors.ENDC)

        print('Applying deltas to db_test... ', end='')
        upgrader_test = Upgrader(self.pg_service_test, self.upgrades_table, self.delta_dir)
        upgrader_test.run()
        print(Bcolors.OKGREEN + 'OK' + Bcolors.ENDC)

        print('Creating db_comp with init_qwat.sh... ', end='')
        os.chdir('..')
        os.system('./init_qwat.sh -p {}'.format(self.pg_service_comp))
        os.chdir('db_manager')
        print(Bcolors.OKGREEN + 'OK' + Bcolors.ENDC)

        print('Checking db_test with db_comp... ', end='')
        checker = Checker(self.pg_service_test, self.pg_service_comp, silent=False)
        if checker.check_all(self.ignore_list):
            print(Bcolors.OKGREEN + 'OK' + Bcolors.ENDC)
            if self.__confirm(prompt='Apply deltas to {}?'.format(upgrader_prod.connection.dsn)):
                print('Applying deltas to db... ', end='')
                upgrader_prod.run()
                print(Bcolors.OKGREEN + 'OK' + Bcolors.ENDC)
        else:
            print(Bcolors.FAIL + 'FAILED' + Bcolors.ENDC)
            raise SystemExit(1)

    def __ask_create_upgrades_table(self, upgrader):
        """Ask the user if he want to create the upgrades table.

            Parameters
            ----------
            upgrader: Upgrader
                The Upgrader instance designate to create the table.
        """
        print(Bcolors.FAIL + 'Table {} not found in {}'.format(upgrader.upgrades_table, upgrader.connection.dsn) + Bcolors.ENDC)
        print('Do you want to create it now?')

        if self.__confirm(prompt='Create table {} in {}?'.format(upgrader.upgrades_table, upgrader.connection.dsn)):
            upgrader.create_upgrades_table()
            print(Bcolors.OKGREEN + 'Created table  {} in {}'.format(upgrader.upgrades_table, upgrader.connection.dsn) + Bcolors.ENDC)
        else:
            raise UpgradesTableNotFoundError

        # From http://code.activestate.com/recipes/541096-prompt-the-user-for-confirmation/
    def __confirm(self, prompt=None, resp=False):
        """Prompt for yes or no response from the user.

            Parameters
            ----------
            prompt: basestring
                The question to be prompted to the user.
            resp: bool
                The default value assumed by the caller when user simply types ENTER.
                
            Returns
            -------
            bool
                True if the user response is 'y' or 'Y'
                False if the user response is 'n' or 'N'
        """

        if prompt is None:
            prompt = 'Confirm'

        if resp:
            prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
        else:
            prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

        while True:
            # TODO raw_input is only python2, in python3 is input()
            ans = raw_input(prompt)
            if not ans:
                return resp
            if ans not in ['y', 'Y', 'n', 'N']:
                print('please enter y or n.')
                continue
            if ans == 'y' or ans == 'Y':
                return True
            if ans == 'n' or ans == 'N':
                return False

    def __set_baseline(self, pg_service, upgrader):
        """Set the version of the current db version into the upgrades table.

            Parameters
            ----------
            pg_service: basestring
                The name of the postgres service (defined in pg_service.conf) related to the db where find the qwat db
                version.
            upgrader: Upgrader
                The Upgrader instance designate to set the baseline.
                
            This method is to be called right after the creation of the upgrades table.
        """

        query = """
                SELECT version FROM qwat_sys.versions
                """

        connection = psycopg2.connect("service={}".format(pg_service))
        cursor = connection.cursor()
        cursor.execute(query)

        version = cursor.fetchone()[0]
        upgrader.set_baseline(version)


class Bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


if __name__ == "__main__":
    """
    Main process
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-pp', '--pg_service_prod', help='Name of the pg_service related to production db', required=True)
    parser.add_argument('-pt', '--pg_service_test', help='Name of the pg_service related to a test db used to test the '
                        'migration', required=True)
    parser.add_argument('-pc', '--pg_service_comp', help='Name of the pg_service related to a db used to compare the '
                                                  'updated db test with the last version of the db', required=True)

    args = parser.parse_args()

    #TODO add an option to set config file
    manager = Manager(args.pg_service_prod, args.pg_service_test, args.pg_service_comp)
    manager.run()
        