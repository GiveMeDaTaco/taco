class ToolsValidator:
    """
    Contains validation for various functions in `tlptaco.tools`
    """

    @staticmethod
    def import_local_python_functions(script_path: str, path_type: str):
        if not isinstance(script_path, str):
            raise TypeError(
                f"Expected type str for script_path in import_local_python_functions, not {type(script_path)}")

        if not isinstance(path_type, str):
            raise TypeError(f"Expected type str for path_type in import_local_python_functions, not {type(path_type)}")

        path_type = path_type.lower()
        path_type_values = ['relative', 'absolute']
        if path_type.lower() not in path_type_values:
            raise ValueError(f"path_type must be either 'relative' or 'absolute'")

    @staticmethod
    def load_py_dict_parameters(file_path: str, function_name: str, path_type='relative'):
        # reuse validations from import_local_python_functions since these variables will eventually get passed here anyways
        ToolsValidator.import_local_python_functions(file_path, path_type)
        # ensure function_name is a string
        if not isinstance(function_name, str):
            raise TypeError(f"Expected type str for function_name, not type {type(function_name)}")

    @staticmethod
    def load_py_dict_module(module, function_name, py_file_name):
        if hasattr(module, function_name):
            func = getattr(module, function_name)
            if not callable(func):
                raise AttributeError(
                    f"User provided function name {function_name}, but this is not a Callable in {py_file_name}")
        else:
            raise AttributeError(f"{function_name} does not exist in {py_file_name}")