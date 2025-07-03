class ValueWarning(Exception):
    """
    Used to catch ValueWarnings in the Validator classes so that process won't error out, but we can warn the user.
    """
    def __init__(self, message):
        super().__init__(message)