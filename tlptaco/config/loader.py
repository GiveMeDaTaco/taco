"""
Load and parse tlptaco configuration files into Pydantic models.
"""
try:
    import yaml
except ImportError:
    yaml = None
import json

from tlptaco.config.schema import AppConfig

# TODO: the yaml needs to be read in order (i.e., OrderedDict since segment order matters)
def load_config(path: str) -> AppConfig:
    """
    Load a YAML or JSON config file and parse into AppConfig.
    """
    if path.lower().endswith(('.yml', '.yaml')):
        if yaml is None:
            raise ImportError("PyYAML is required to load YAML configs; please install pyyaml")
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
    elif path.lower().endswith('.json'):
        with open(path, 'r') as f:
            data = json.load(f)
    else:
        raise ValueError('Unsupported config format, must be .yaml/.yml or .json')

    return AppConfig.parse_obj(data)