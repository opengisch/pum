# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod


class DeltaPy(metaclass=ABCMeta):
    """This abstract class must be instantiated by the delta.py classes"""

    def __init__(
            self, current_db_version, delta_dir, pg_service, upgrades_table):
        """Constructor, receive some useful parameters accesible from the
        subclasses als propperties.

        Parameters
        ----------
        current_db_version: str
            The current db version
        pg_service: str
            The name of the postgres service (defined in pg_service.conf)
            related to the db
        upgrades_table: str
            The name of the table (int the format schema.name) where the
            informations about the upgrades are stored
        delta_dir: str
            The path of the directory where the delta files are stored
        """

        self.__current_db_version = current_db_version
        self.__delta_dir = delta_dir
        self.__pg_service = pg_service
        self.__upgrades_table = upgrades_table
        
    @abstractmethod
    def run(self):
        """This method must be implemented in the subclasses. It is called
        when the delta.py file is runned by Upgrader class"""
        pass

    @property
    def current_db_version(self):
        """Return the current db version"""
        return self.__current_db_version

    @property
    def delta_dir(self):
        """Return the path of the delta directory"""
        return self.__delta_dir

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
