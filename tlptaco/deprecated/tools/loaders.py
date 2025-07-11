import json
import yaml
from collections import OrderedDict
from tlptaco.tools.environment import import_local_python_functions
from tlptaco.validations.tools import ToolsValidator


def load_ordered_json(file_path: str) -> OrderedDict:
    """
    Loads a JSON file as an ordered dictionary

    :param file_path: path to the JSON file
    :return: returns the JSON file in the form of an OrderedDict
    :rtype: OrderedDict
    """
    with open(file_path, 'r') as f:
        ordered_json = json.load(f, object_pairs_hook=OrderedDict)
    return ordered_json


def load_py_dict(file_path: str, function_name: str, path_type='relative', **kwargs) -> OrderedDict:
    """
    Loads a dictionary for tables or conditions that is found in a .py file

    :param path_type: absolute or relative, tells the script how to use the py_file_name to locate the file
    :param file_path: the relative file path to the file that contains the function
    :param function_name: the function within the file that you want to use
    :param kwargs: the arguments to pass to your function
    :return: values returned from your function, should be an OrderedDict
    """
    # validate input variables
    ToolsValidator.load_py_dict_parameters(file_path, function_name, path_type)

    module = import_local_python_functions(file_path, path_type)
    # validate module
    ToolsValidator.load_py_dict_module(module, function_name, file_path)

    # get function from module
    func = getattr(module, function_name)
    result = func(**kwargs)

    # make sure the result is an OrderedDict
    if not isinstance(result, OrderedDict):
        raise TypeError(f"{function_name} should return a type OrderedDict, not type {type(result)}")

    return func(**kwargs)


def load_yaml(file_path: str, Loader=yaml.SafeLoader, object_pairs_hook=OrderedDict) -> OrderedDict:
    """
    Loads a YAML file as an OrderedDict

    :param file_path: path to your yaml file
    :param Loader: what yaml loader to use (default should be fine)
    :param object_pairs_hook: what collection to save this to, default is OrderedDict
    :return:
    """

    def ordered_load(stream, Loader, object_pairs_hook):
        class OrderedLoader(Loader):
            pass

        def construct_mapping(loader, node):
            loader.flatten_mapping(node)
            return object_pairs_hook(loader.construct_pairs(node))

        OrderedLoader.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            construct_mapping)

        return yaml.load(stream, OrderedLoader)

    with open(file_path, 'r') as file:
        return ordered_load(file, Loader, object_pairs_hook)