"""  
This module contains functions to set up loggers for the package. 

The base logger is set up first, writes to a file, and is used to create other loggers.
The general logger is used for most of the package and prints to the console.
The pride logger is used for the pride module and prints to the console and a file.
The rinex logger is used for the rinex module and prints to the console and a file.
The notebook logger is used for the notebook module and prints to the console with a minimal format.
"""

import logging
import os
from functools import wraps
from pathlib import Path

BASIC_FORMAT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')
MINIMAL_NOTEBOOK_FORMAT = logging.Formatter('%(message)s')

BASE_LOG_FILE_NAME = 'es_sfg_tools.log'
PRIDE_LOG_FILE_NAME = 'pride.log'
RINEX_LOG_FILE_NAME = 'rinex.log'


class BaseLogger:
    dir = Path.home() / ".es_sfg_tools"
    dir.mkdir(exist_ok=True)
    name = "es_sfg_tools.log"
    path = str(dir / BASE_LOG_FILE_NAME)
    format = BASIC_FORMAT
    level = logging.INFO

    logger:logging.Logger = logging.getLogger(cls.path)
    base_handler = logging.FileHandler(path)
    base_handler.setFormatter(BASIC_FORMAT)
    logger.setLevel(level)
    logger.addHandler(base_handler)
    handlers = {'base': base_handler}

    @staticmethod
    def reset_base_handler(cls):
        cls.logger.removeHandler(cls.handlers['base'])
        cls.base_handler = logging.FileHandler(cls.path)
        cls.base_handler.setFormatter(cls.format)
        cls.logger.addHandler(cls.base_handler)
        cls.handlers['base'] = cls.base_handler

    @staticmethod
    def set_dir(cls, dir: Path):
        cls.dir = dir
        cls.path = str(dir /cls.name)
        cls.reset_base_handler()

    @staticmethod
    def set_format_minimal(cls):
        cls.format = MINIMAL_NOTEBOOK_FORMAT
        cls.reset_base_handler()

    @staticmethod
    def set_format_basic(cls):
        cls.format = BASIC_FORMAT
        cls.handlers['base'].setFormatter(cls.format)

    @staticmethod
    def set_base_level(cls, level):
        cls.level = level
        cls.logger.setLevel(level)

    @staticmethod
    def route_to_console(cls):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(cls.format)
        cls.logger.addHandler(console_handler)
        cls.handlers['console'] = console_handler

    @staticmethod
    def remove_console(cls):
        cls.logger.removeHandler(cls.handlers['console'])
        del cls.handlers['console']


def ensure_directory_exists(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        directory_path = kwargs.get('directory_path', './logs')
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        return func(*args, **kwargs)
    return wrapper

@ensure_directory_exists
def setup_base_logger(directory_path='./logs'):
    """ 
    This function sets up the base logger for the package. Multiple loggers can be created from this base logger. 
    The base logger only logs to a file and is used to set up other loggers.
    """

    # Base logger
    base_logger = logging.getLogger('base_logger')
    base_logger.setLevel(logging.INFO)

    # Create a file handler for the base logger
    base_log_file = os.path.join(directory_path, BASE_LOG_FILE_NAME)
    base_file_handler = logging.FileHandler(base_log_file)
    base_file_handler.setLevel(logging.INFO)

    # Create a formatter and set it for both handlers
    base_file_handler.setFormatter(BASIC_FORMAT)

    # Add the handlers to the base logger
    base_logger.addHandler(base_file_handler)

@ensure_directory_exists
def setup_general_logger(directory_path='./logs'):
    """ 
    This function sets up a general logger for the package to import and use where another logger is not specified. 
    It will log to the base log file and print to the console.
    """

    # Set up the base logger
    setup_base_logger(directory_path=directory_path)

    # Set up a general logger (child of base logger) for most of the package to use and print to console
    logger = logging.getLogger('base_logger.logger')
    logger.setLevel(logging.INFO)

    # Create a console handler for the logger
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(BASIC_FORMAT)
    logger.addHandler(console_handler)

    # Set the logger to propagate to the base logger
    logger.propagate = True

    return logger

@ensure_directory_exists
def setup_pride_logger(directory_path='./logs'):

    """ This function sets up a logger for the pride module. It logs to the base log file and prints to the console. """

    # Pride logger (child of base_logger)
    pride_logger = logging.getLogger('base_logger.pride_logger')
    pride_logger.setLevel(logging.INFO)
    pride_log_file = os.path.join(directory_path, PRIDE_LOG_FILE_NAME) # TODO - change this to a subdirectory (should it be logs/pride.log?)

    # Create a console handler for the pride logger
    pride_console_handler = logging.StreamHandler()
    pride_console_handler.setLevel(logging.INFO)
    pride_console_handler.setFormatter(BASIC_FORMAT)
    pride_logger.addHandler(pride_console_handler)

    # Create a file handler for the pride logger
    pride_file_handler = logging.FileHandler(pride_log_file)
    pride_file_handler.setLevel(logging.INFO)
    pride_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    pride_logger.addHandler(pride_file_handler)

    # Set the logger to propagate to the base logger
    pride_logger.propagate = True

    return pride_logger

@ensure_directory_exists
def setup_rinex_logger(directory_path='./logs'):
    """ 
    This function sets up a logger for the rinex module. 
    It logs to the base log file and prints to the console with only the message.
    """
    # Rinex logger (child of base_logger)
    rinex_logger = logging.getLogger('base_logger.rinex_logger')
    rinex_logger.setLevel(logging.INFO)
    rinex_log_file = os.path.join(directory_path, RINEX_LOG_FILE_NAME)

    # Set up the formatter for the rinex logger and the console handler
    rinex_console_handler = logging.StreamHandler()
    rinex_console_handler.setLevel(logging.INFO)
    rinex_console_handler.setFormatter(BASIC_FORMAT)
    rinex_logger.addHandler(rinex_console_handler)

    # Create a file handler for the pride logger
    rinex_file_handler = logging.FileHandler(rinex_log_file)
    rinex_file_handler.setLevel(logging.INFO)
    rinex_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    rinex_logger.addHandler(rinex_file_handler)

    # Set the logger to propagate to the base logger
    rinex_logger.propagate = True

    return rinex_logger


def setup_notebook_logger():
    """ 
    This function sets up a logger for the notebook module. 
    It logs to the base log file and prints to the console with only the message.
    """
    # Notebook logger (child of base_logger)
    notebook_logger = logging.getLogger('base_logger.notebook_logger')
    notebook_logger.setLevel(logging.INFO)

    # Set up the formatter for the notebook logger and the console handler
    notebook_console_handler = logging.StreamHandler()
    notebook_console_handler.setLevel(logging.INFO)
    notebook_console_handler.setFormatter(MINIMAL_NOTEBOOK_FORMAT)

    # Add the console handler to the notebook logger
    notebook_logger.addHandler(notebook_console_handler)

    # Set the logger to propagate to the base logger
    notebook_logger.propagate = True

    return notebook_logger


# Set up the loggers
# logger = setup_general_logger()
# logger.info('Starting the general logger')
# pride_logger = setup_pride_logger()
# pride_logger.info('Starting the pride logger')
# notebook_logger = setup_notebook_logger()
# notebook_logger.info('Starting the notebook logger')
