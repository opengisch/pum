# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod


class PUMCommand(ABC):
    """Abstract Base Class representing a pum command"""

    @abstractmethod
    def add_parser(subparsers):
        pass

    @abstractmethod
    def run(args):
        pass

    def write_output(message):
        # TODO print through pum class?
        print(message)
