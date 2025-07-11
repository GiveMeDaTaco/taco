"""
Configure project-wide logging.
"""
import logging
try:
    from rich.logging import RichHandler
    from rich.text import Text
except ImportError:
    RichHandler = None
    Text = None

# Emoji icons for log levels to enhance readability
LEVEL_EMOJI = {
    "DEBUG":    "🐛",
    "INFO":     "ℹ️",
    "WARNING":  "⚠️",
    "ERROR":    "❌",
    "CRITICAL": "🔥",
}

class EmojiFormatter(logging.Formatter):
    """
    Logging Formatter that injects an emoji based on the log level.
    """
    def format(self, record):
        # Attach emoji for the level
        record.emoji = LEVEL_EMOJI.get(record.levelname, "")
        return super().format(record)

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
    # Prepare EmojiFormatter for file handlers or fallback console
    fmt_str = "%(emoji)s %(asctime)s %(name)s %(levelname)s: %(message)s"
    fmt = EmojiFormatter(fmt_str, datefmt="[%X]")
    # Determine console log level
    console_level = logging.DEBUG if verbose else getattr(logging, cfg.level.upper(), logging.INFO)
    # Console handler: use EmojiRichHandler (with emojis) if Rich is available, else fallback
    if RichHandler is not None and Text is not None:
        # Subclass RichHandler to prefix level names with emoji
        class EmojiRichHandler(RichHandler):  # type: ignore
            def get_level_text(self, record):  # noqa: A003
                level = record.levelname
                style = f"logging.level.{level.lower()}"
                emoji = LEVEL_EMOJI.get(level, "")
                # pad level name to width 8
                padded = level.ljust(8)
                return Text.assemble((emoji + ' ' + padded, style))
        rich_handler = EmojiRichHandler(
            level=console_level,
            markup=True,
            show_time=True,
            show_level=True,
            show_path=False,
        )
        root.addHandler(rich_handler)
    else:
        ch = logging.StreamHandler()
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