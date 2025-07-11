"""
Configure project-wide logging.
"""
import logging

def configure_logging(cfg, verbose=False):
    """
    Configure root logger:
      - console handler at DEBUG if verbose, else cfg.level
      - file handler at cfg.level if cfg.file
      - debug file handler at DEBUG if cfg.debug_file
    Returns the root logger.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
    # Console handler
    ch = logging.StreamHandler()
    console_level = logging.DEBUG if verbose else getattr(logging, cfg.level.upper(), logging.INFO)
    ch.setLevel(console_level)
    ch.setFormatter(fmt)
    root.addHandler(ch)
    # File handler
    if getattr(cfg, 'file', None):
        fh = logging.FileHandler(cfg.file)
        file_level = getattr(logging, cfg.level.upper(), logging.INFO)
        fh.setLevel(file_level)
        fh.setFormatter(fmt)
        root.addHandler(fh)
    # Debug file handler
    if getattr(cfg, 'debug_file', None):
        dfh = logging.FileHandler(cfg.debug_file)
        dfh.setLevel(logging.DEBUG)
        dfh.setFormatter(fmt)
        root.addHandler(dfh)
    return root

def get_logger(name: str):
    return logging.getLogger(f"tlptaco.{name}")