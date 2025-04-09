from enum import Enum


class MessageType(Enum):
    DEFAULT = "DEFAULT"
    WAITING = "WAITING"
    OKGREEN = "OKGREEN"
    WARNING = "WARNING"
    FAIL = "FAIL"
    BOLD = "BOLD"
    UNDERLINE = "UNDERLINE"
