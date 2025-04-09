import os
import sys

from pum.utils.message_type import MessageType


# From http://code.activestate.com/recipes/541096-prompt-the-user-for
# -confirmation/
def ask_for_confirmation(prompt=None, resp=False):
    """Prompt for a yes or no response from the user.

    Parameters
    ----------
    prompt: basestring
        The question to be prompted to the user.
    resp: bool
        The default value assumed by the caller when user simply
        types ENTER.

    Returns
    -------
    bool
        True if the user response is 'y' or 'Y'
        False if the user response is 'n' or 'N'
    """
    global input
    if prompt is None:
        prompt = "Confirm"

    if resp:
        prompt = "{} [{}]|{}: ".format(prompt, "y", "n")
    else:
        prompt = "{} [{}]|{}: ".format(prompt, "n", "y")

    while True:
        ans = input(prompt)
        if not ans:
            return resp
        if ans not in ["y", "Y", "n", "N"]:
            print("please enter y or n.")
            continue
        if ans == "y" or ans == "Y":
            return True
        if ans == "n" or ans == "N":
            return False


class Bcolors:
    WAITING = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def __out(message: str, type: MessageType = MessageType.DEFAULT) -> None:
    """
    Print output messages with optional formatting.

    Parameters
    ----------
    message : str
        The message to display.
    type : MessageType, optional (default: MessageType.DEFAULT)
        The type of message which determines the formatting.
    """
    supported_platform = sys.platform != "win32" or "ANSICON" in os.environ
    if supported_platform:
        if type == MessageType.WAITING:
            print(Bcolors.WAITING + message + Bcolors.ENDC, end="")
        elif type == MessageType.OKGREEN:
            print(Bcolors.OKGREEN + message + Bcolors.ENDC)
        elif type == MessageType.WARNING:
            print(Bcolors.WARNING + message + Bcolors.ENDC)
        elif type == MessageType.FAIL:
            print(Bcolors.FAIL + message + Bcolors.ENDC)
        elif type == MessageType.BOLD:
            print(Bcolors.BOLD + message + Bcolors.ENDC)
        elif type == MessageType.UNDERLINE:
            print(Bcolors.UNDERLINE + message + Bcolors.ENDC)
        else:
            print(message)
    else:
        print(message)
