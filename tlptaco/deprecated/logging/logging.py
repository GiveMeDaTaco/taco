import functools
import sys
import logging
import sqlparse


class DuplicateFilter(logging.Filter):
    """
    Identifies when the same log entry is sent multiple times in a row and only keeps 1.
    It can be tricky to deal with the complex nesting process that tlptaco uses to generate eligibility
    and manage logging, so this class helps with a part of that.
    """

    def __init__(self):
        super().__init__()
        self.last_log = None

    def filter(self, record):
        current_log = (record.levelno, record.msg)
        if current_log == self.last_log:
            return False
        self.last_log = current_log
        return True


# Global variable to track indentation level
INDENT_LEVEL = 0


class DebugFilter(logging.Filter):
    """
    Filter for identifying DEBUG log entries
    """

    def filter(self, record):
        return record.levelno == logging.DEBUG


class CustomLogger:
    """
    Custom logger to manage logging for tlptaco; acts as a wrapper around `logging.Logger` (Pythons built-in logging library class)

    Attributes:
        logger (logging.Logger): an instance of logging.Logger that will be used to handle the logs
        log_file_location (str): file location for log details (everything except DEBUG messages)
        debug_file_location (str): file location for log details (only DEBUG messages); NOTE: tlptaco uses DEBUG to store all the sql that ran during the process
    """

    def __init__(self, name, log_level=logging.INFO, log_file=None, log_format=None, date_format=None, debug_file=None,
                 debug_format=None):
        """
        Initializes class

        :param name: logger name
        :type name: str
        :param log_level: the minimum level to filter on in the log_file (NEVER includes DEBUG)
        :type log_level: str
        :param log_file: the file location for the log
        :type log_file: str
        :param log_format: the format for the messages in the log_file
        :type log_format: str
        :param date_format: the date format for the log_file
        :type date_format: str
        :param debug_file: the file location for all DEBUG messages
        :type debug_file: str
        :param debug_format: message format for the debug_file
        :type debug_format: str
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.log_file_location = log_file
        self.debug_file_location = debug_file

        # Set default log format and date format if not provided
        if log_format is None:
            log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        if date_format is None:
            date_format = '%Y-%m-%d %H:%M:%S'
        if debug_format is None:
            debug_format = '%(message)s'

        formatter = logging.Formatter(log_format, datefmt=date_format)
        debug_formatter = logging.Formatter(debug_format, datefmt=date_format)

        # Create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(formatter)
        ch.addFilter(DuplicateFilter())
        self.logger.addHandler(ch)

        # Create file handler if log_file is specified
        if self.log_file_location is not None:
            fh = logging.FileHandler(self.log_file_location)
            fh.setLevel(log_level)
            fh.setFormatter(formatter)
            fh.addFilter(DuplicateFilter())
            self.logger.addHandler(fh)

        if self.debug_file_location is not None:
            d_fh = logging.FileHandler(self.debug_file_location)
            d_fh.setLevel(logging.DEBUG)
            d_fh.setFormatter(debug_formatter)
            d_fh.addFilter(DuplicateFilter())
            d_fh.addFilter(DebugFilter())
            self.logger.addHandler(d_fh)

    def __getattr__(self, attr):
        """
        When attempting to `getattr`, pull it from the `self.logger` instance instead.
        NOTE: that `__getattr__` is only called in Python when an attribute doesn't exist in the class.
        This means if we call CustomLogger.abc, we didn't define CustomLogger.abc so it pulls it from self.logger.abc

        :param attr: name of attribute to retrieve
        :return: attribute from `self.logger`
        """
        return getattr(self.logger, attr)

    @staticmethod
    def indent_log(message):
        """
        Indent the log based on the global INDENT_LEVEL

        :param message: logging message
        :return: logging message with added indent on the front
        :rtype: str
        """
        global INDENT_LEVEL
        indent = "    " * (INDENT_LEVEL * 4)
        return f"{indent}{message}"

    def info(self, message, *args, **kwargs):
        """Creates INFO logging message"""
        message = self.indent_log(message)
        self.logger.info(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        """
        Creates ERROR logging message

        :param message: ERROR message
        :param args: additional arguments to pass to `self.logger`
        :param kwargs: additional named arguments to pass to `self.logger`
        """
        message = self.indent_log(message)
        self.logger.error(message, *args, **kwargs)

    # NOTE: debug is not indented
    def debug(self, message, *args, **kwargs):
        """
        Creates DEBUG logging message

        :param message: DEBUG message
        :param args: additional arguments to pass to `self.logger`
        :param kwargs: additional named arguments to pass to `self.logger`
        """
        self.logger.debug(message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        """
        Creates WARNING logging message

        :param message: WARNING message
        :param args: additional arguments to pass to `self.logger`
        :param kwargs: additional named arguments to pass to `self.logger`
        """
        message = self.indent_log(message)
        self.logger.warning(message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        """
        Creates CRITICAL logging message

        :param message: CRITICAL message
        :param args: additional arguments to pass to `self.logger`
        :param kwargs: additional named arguments to pass to `self.logger`
        """
        message = self.indent_log(message)
        self.logger.critical(message, *args, **kwargs)

    def format_debug_file(self):
        """
        Reformats the DEBUG log file for more readable SQL, since the DEBUG file's purpose is to hold all
        SQL code ran during the process
        """
        if self.debug_file_location is not None:
            with open(self.debug_file_location, 'r') as f:
                log_contents = f.read()

            log_contents = sqlparse.format(log_contents, reindent=True, keyword_case='upper')

            with open(self.debug_file_location, 'w') as f:
                f.write(log_contents)


def call_logger(*var_names):
    """
    Decorator that can be used to automatically call the logger when a function starts and finishes

    :param var_names: DEPRECATED
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            global INDENT_LEVEL
            class_name = self.__class__.__name__
            function_name = func.__name__

            self.logger.info(f"Initiating {class_name}.{function_name}")
            INDENT_LEVEL += 1

            def trace_function(frame, event, arg):
                if event == "line":
                    if var_names:
                        for var_name in var_names:
                            if var_name in frame.f_locals:
                                self.logger.info(
                                    f"{class_name}.{function_name}: {var_name}=\n\t{frame.f_locals[var_name]}".replace(
                                        '\n', '\n\t'))
                return trace_function

            try:
                if var_names:
                    sys.settrace(trace_function)
                result = func(self, *args, **kwargs)
                sys.settrace(None)

                INDENT_LEVEL -= 1
                self.logger.info(f"Finished {class_name}.{function_name}")
                return result
            except Exception as e:
                INDENT_LEVEL -= 1
                self.logger.error(f"Error in {class_name}.{function_name}: {e}")
                sys.settrace(None)
                raise

        return wrapper

    return decorator