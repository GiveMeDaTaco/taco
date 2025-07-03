import os

def check_directory(path, mode=0o0770, permissions=0o0770) -> None:
    """
    Ensures that the path (if a file path, then parent directory) exists.
    If it doesn't exist, it creates the directory.

    :param path: file path or directory
    :param mode: the mode that will be used to create the directory; 0o0770 creates the directory with 770 permissions
    :param permissions: ensures the proper permissions are set after directory creation; 0o0770 = 770
    :returns None:
    """
    if not os.path.isdir(path):
        directory = os.path.dirname(path)
    else:
        directory = path

    if not os.path.exists(directory):
        os.makedirs(directory, mode=mode)
        # ensure permissions are 770
        os.chmod(directory, permissions)