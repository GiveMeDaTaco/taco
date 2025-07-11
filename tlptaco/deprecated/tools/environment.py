import os
from types import ModuleType
import importlib.util
import re
from tlptaco.validations.tools import ToolsValidator

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

def import_local_python_functions(script_path: str, path_type: str = 'relative') -> ModuleType:
    """
    Used to import a local python script as a library

    :param script_path: path to the script
    :param path_type: whether the path provided is the absolute or relative path (relative must be relative to the script where you call this function)
    :return: the file python as a library
    :rtype: ModuleType
    """
    # verify values provided are correct
    ToolsValidator.import_local_python_functions(script_path, path_type)

    other_script = ''
    if path_type == 'relative':
        # get the directory of the script you are running
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # path to the script you want to import
        other_script = os.path.normpath(os.path.join(script_dir, script_path))
    elif path_type == 'absolute':
        other_script = script_path

    # verify this file exists
    if not os.path.isfile(other_script):
        raise FileNotFoundError(f"Cannot find {other_script}")

    # create the library name (the name that python will use internally to refer to this script)
    module_name = os.path.splitext(os.path.basename(other_script))[0]
    # remove any illegal characters from the module_name
    module_name = re.sub(r'\W|^(?=\d)', '_', module_name)

    # load other script
    spec = importlib.util.spec_from_file_location(module_name, other_script)
    module = importlib.util.module_from_spec(spec)
    if spec.loader:
        spec.loader.exec_module(module)
        return module
    else:
        raise ImportError(f"Could not load module from {other_script}")