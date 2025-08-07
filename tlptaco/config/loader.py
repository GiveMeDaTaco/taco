"""
Load and parse tlptaco configuration files into Pydantic models.
"""
try:
    import yaml
except ImportError:
    yaml = None
import json

# ---------------------------------------------------------------------------
# Configuration loader with *tab-sanitisation* helper
# ---------------------------------------------------------------------------

from tlptaco.config.schema import AppConfig
from pydantic import ValidationError

# TODO: the yaml needs to be read in order (i.e., OrderedDict since segment order matters)
def load_config(path: str) -> AppConfig:
    """
    Load a YAML or JSON config file and parse into AppConfig.
    """
    path_lower = path.lower()

    had_tabs = False  # track whether we detected tab characters (YAML only)

    # ------------------------------------------------------------------
    # YAML handling with tab sanitisation
    # ------------------------------------------------------------------
    if path_lower.endswith(('.yml', '.yaml')):
        if yaml is None:
            raise ImportError("PyYAML is required to load YAML configs; please install pyyaml")

        # Read raw text first so we can inspect/replace tabs if present.
        with open(path, 'r', encoding='utf-8') as f:
            raw_text = f.read()

        had_tabs = '\t' in raw_text
        text_for_parser = raw_text.replace('\t', '  ') if had_tabs else raw_text

        try:
            data = yaml.safe_load(text_for_parser)
        except yaml.YAMLError as e:  # type: ignore[attr-defined]
            msg = f"Failed to parse YAML config '{path}': {e}"
            if had_tabs:
                msg += (
                    "\nNote: tab characters were detected and replaced with spaces "
                    "during parsing.  YAML relies on *space* indentation – "
                    "please convert tabs to spaces and try again."
                )
            raise ValueError(msg) from e

    # ------------------------------------------------------------------
    # JSON – unchanged (tabs are allowed in JSON strings)
    # ------------------------------------------------------------------
    elif path_lower.endswith('.json'):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        raise ValueError('Unsupported config format, must be .yaml/.yml or .json')

    # ------------------------------------------------------------------
    # Pydantic validation
    # ------------------------------------------------------------------
    try:
        return AppConfig.parse_obj(data)
    except ValidationError as ve:
        # Append helpful note if tabs were present in the original YAML
        if had_tabs:
            note = (
                "\nThe configuration file contained tab characters which often break "
                "YAML indentation. Replace all tab characters with spaces (e.g. 2 "
                "or 4 spaces) and rerun."
            )
            raise ValueError(str(ve) + note) from ve
        raise