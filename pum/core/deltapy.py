# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod


class DeltaPy(metaclass=ABCMeta):
    """This abstract class must be instantiated by the delta.py classes"""

    def __init__(
            self, current_db_version, delta_dir, delta_dirs, pg_service, upgrades_table, variables: dict={}):
        """Constructor, receive some useful parameters accessible from the
        subclasses als properties.

        Parameters
        ----------
        current_db_version: str
            The current db version
        pg_service: str
            The name of the postgres service (defined in pg_service.conf)
            related to the db
        upgrades_table: str
            The name of the table (int the format schema.name) where the
            information about the upgrades are stored
        delta_dir: str
            The path to the directory where this delta file is stored
        delta_dirs: list(str)
            The paths to directories where delta files are stored
        """

        self.__current_db_version = current_db_version
        self.__delta_dir = delta_dir
        self.__delta_dirs = delta_dirs
        self.__pg_service = pg_service
        self.__upgrades_table = upgrades_table
        self.__variables = variables

    @abstractmethod
    def run(self):
        """This method must be implemented in the subclasses. It is called
        when the delta.py file is run by Upgrader class"""
        pass

    @property
    def variables(self):
        """Return the dictionary of variables"""
        return self.__variables

    def variable(self, name: str, default_value = None):
        """
        Returns the value of the variable given in PUM
        :param name: the name of the variable
        :param default_value: If not given or None, an exception will be raised if the variable is not found.
        :return: the variable value
        """
        if default_value:
            return self.__variables.get(name, default_value)
        else:
            return self.__variables[name]


    @property
    def current_db_version(self):
        """Return the current db version"""
        return self.__current_db_version

    @property
    def delta_dir(self):
        """Return the path of the delta directory including this delta"""
        return self.__delta_dir

    @property
    def delta_dirs(self):
        """Return the paths of the delta directories"""
        return self.__delta_dirs

    @property
    def pg_service(self):
        """Return the name of the postgres service"""
        return self.__pg_service

    @property
    def upgrades_table(self):
        """Return the name of the upgrades information table"""
        return self.__upgrades_table

    def write_message(self, text):
        """Print a message from the subclass.

        Parameters
        ----------
        text: str
            The message to print
        """
        print('Message from {}: {}'.format(self.__class__.__name__, text))
