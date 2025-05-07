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
