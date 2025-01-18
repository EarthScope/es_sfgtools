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
from typing import Literal

BASIC_FORMAT = logging.Formatter("%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s")
MINIMAL_NOTEBOOK_FORMAT = logging.Formatter('%(message)s')

BASE_LOG_FILE_NAME = 'es_sfg_tools.log'
DEFAULT_PATH = os.path.join(Path.home(), ".sfgtools")
LOG_FILE_PATH = os.getenv('LOG_FILE_PATH', DEFAULT_PATH)

class _BaseLogger:
    '''
    _BaseLogger is a base class for creating and managing loggers with file and console handlers.

    Attributes:
        name (str): The name of the logger.
        dir (Path): The directory where the log file will be stored.
        file_name (str): The name of the log file.
        path (str): The full path to the log file.
        format (logging.Formatter): The logging format to be used.
        level (int): The logging level.
        logger (logging.Logger): The logger instance.
        file_handler (logging.FileHandler): The file handler for the logger.
        console_handler (logging.StreamHandler): The console handler for the logger (optional).

    Methods:
        __init__(name, dir, file_name, format, level):
            Initializes the _BaseLogger instance with the specified parameters.

        _reset_file_handler():
            Resets the file handler for the logger.

        set_dir(dir):
            Sets the directory for the logger and updates the file path.

        set_format_minimal():
            Sets the logging format to a minimal notebook format.

        set_format_basic():
            Sets the logging format to a basic format.

        set_level(level):
            Sets the logging level for the logger.

        route_to_console():
            Configures the logger to route log messages to the console.

        remove_console():
            Removes the console handler from the logger.
    '''
    def __init__(self, 
                 name:str= "base_logger",
                 dir: Path= LOG_FILE_PATH, #Path=Path.home()/".sfgtools",
                 file_name:str=BASE_LOG_FILE_NAME, 
                 format:logging.Formatter = BASIC_FORMAT,
                 console_format:logging.Formatter = MINIMAL_NOTEBOOK_FORMAT, 
                 level=logging.INFO):
        
        self.name = name
        self.dir = dir
        # Create the full path if it does not exist
        os.makedirs(self.dir, exist_ok=True) 
        self.file_name = file_name
        self.path = os.path.join(dir, self.file_name)
        self.format = format
        self.console_format = console_format
        self.level = level
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(level)
        self.file_handler = logging.FileHandler(self.path)
        self.file_handler.setFormatter(format)
        self.logger.addHandler(self.file_handler)
  
    def _reset_file_handler(self) -> None:
        """
        Resets the file handler for the logger.
        This method removes the existing file handler from the logger, creates a new
        file handler with the specified path, sets the formatter for the new file handler,
        and adds the new file handler to the logger.

        Attributes:
            self.logger (logging.Logger): The logger instance to which the file handler is attached.
            self.file_handler (logging.FileHandler): The current file handler for the logger.
            self.path (str): The file path for the new file handler.
            self.format (logging.Formatter): The formatter to be set for the new file handler.
        """

        if self.file_handler:
            self.logger.removeHandler(self.file_handler)
        try:
            self.file_handler = logging.FileHandler(self.path)
            self.file_handler.setFormatter(self.format)
            self.logger.addHandler(self.file_handler)
        except Exception as e:
            self.logger.error(f"Failed to set file handler: {e}")

    def set_dir(self, dir: Path) -> None:
        """
        Set the directory for the logger and update the file path.
        Args:
            dir (Path): The directory path to set for the logger.
        Updates:
            self.dir: Sets the logger's directory to the provided path.
            self.path: Updates the logger's file path based on the new directory.
        Calls:
            self._reset_file_handler(): Resets the file handler to use the new file path.
        """

        self.dir = dir
        self.path = str(dir / self.file_name)
        self._reset_file_handler()
    
    def set_format_minimal(self) -> None:
        """
        Set the logging format to a minimal notebook format.
        This method updates the logging format to `MINIMAL_NOTEBOOK_FORMAT`
        and resets the file handler to apply the new format.
        """

        self.format = MINIMAL_NOTEBOOK_FORMAT
        self._reset_file_handler()  
    
    def set_format_basic(self):
        """
        Set the logging format to a basic format.
        This method sets the logging format to a predefined basic format
        and applies it to the file handler associated with the logger.

        Attributes:
            self.format (logging.Formatter): The formatter object set to the basic format.
            self.file_handler (logging.FileHandler): The file handler to which the formatter is applied.
        """

        self.format = BASIC_FORMAT
        self.file_handler.setFormatter(self.format)

    def set_level(self, level:Literal[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL]) -> None:
        """
        Set the logging level for the logger.

        Args:
        level (int): The logging level to set. This can be one of the standard
                     logging levels (e.g., logging.DEBUG, logging.INFO, logging.WARNING,
                     logging.ERROR, logging.CRITICAL).
        """

        self.level = level
        self.logger.setLevel(level)
    
    def route_to_console(self):
        """
        Configures the logger to route log messages to the console.
        This method sets up a StreamHandler for the logger, which outputs log messages to the console (standard output).
        It also applies the specified formatter to the console handler.
        Attributes:
            console_handler (logging.StreamHandler): The handler for routing log messages to the console.
        """
        if not hasattr(self,'console_handler'):
            self.console_handler = logging.StreamHandler()
            self.console_handler.setFormatter(self.console_format)
            self.logger.addHandler(self.console_handler)
    
    def remove_console(self):
        """
        Removes the console handler from the logger.
        This method detaches the console handler from the logger instance,
        effectively stopping the logger from outputting logs to the console.
        """
        if hasattr(self,'console_handler'):
            self.logger.removeHandler(self.console_handler)

    def logdebug(self, message) -> None:
        """ Log a debug message with stacklevel=2 (logging module goes up the stack to get the calling function) """
        self.logger.debug(message, stacklevel=2)

    def loginfo(self ,message) -> None:
        """ Log an info message with stacklevel=2 (logging module goes up the stack to get the calling function) """
        self.logger.info(message, stacklevel=2)

    def logerr(self, message) -> None:
        """ Log an error message with stacklevel=2 (logging module goes up the stack to get the calling function)"""
        self.logger.error(message, stacklevel=2)

    def logwarn(self, message) -> None:
        """ Log a warning message with stacklevel=2 (logging module goes up the stack to get the calling function) """
        self.logger.warning(message, stacklevel=2)
        
BaseLogger = _BaseLogger()

# Create loggers for specific modules & set them to propagate to the base logger
GNSSLogger = _BaseLogger(
    name="base_logger.gnss_logger",
    file_name="gnss.log",
)
# Propagate the log messages to the base logger as well
GNSSLogger.propagate= True

ProcessLogger = _BaseLogger(
    name="base_logger.processing_logger",
    file_name="processing.log",
)
ProcessLogger.propagate= True

GarposLogger = _BaseLogger(
    name="base_logger.garpos_logger",
    file_name="garpos.log",
)
GarposLogger.propagate= True


def route_all_loggers_to_console():
    GNSSLogger.route_to_console()
    ProcessLogger.route_to_console()
    GarposLogger.route_to_console()

def remove_all_loggers_from_console():
    GNSSLogger.remove_console()
    ProcessLogger.remove_console()
    GarposLogger.remove_console()

def set_all_logger_levels(level: Literal[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]):
    GNSSLogger.set_level(level)
    ProcessLogger.set_level(level)
    GarposLogger.set_level(level)

def change_all_logger_dirs(dir: Path):
    GNSSLogger.set_dir(dir)
    ProcessLogger.set_dir(dir)
    GarposLogger.set_dir(dir)

# class BaseLogger:
#     dir = Path.home() / ".es_sfg_tools"
#     dir.mkdir(exist_ok=True)
#     name = BASE_LOG_FILE_NAME
#     path = str(dir / name)
#     format = BASIC_FORMAT
#     level = logging.INFO

#     logger:logging.Logger = logging.getLogger(path)
#     base_handler = logging.FileHandler(path)
#     base_handler.setFormatter(BASIC_FORMAT)
#     logger.setLevel(level)
#     logger.addHandler(base_handler)
#     handlers = {'base': base_handler}

#     @staticmethod
#     def info(info:str) -> None:
#         BaseLogger.logger.info(info)

#     @staticmethod
#     def reset_base_handler():
#         BaseLogger.logger.removeHandler(BaseLogger.handlers["base"])
#         BaseLogger.base_handler = logging.FileHandler(BaseLogger.path)
#         BaseLogger.base_handler.setFormatter(BaseLogger.format)
#         BaseLogger.logger.addHandler(BaseLogger.base_handler)
#         BaseLogger.handlers["base"] = BaseLogger.base_handler

#     @staticmethod
#     def set_dir(dir: Path):
#         BaseLogger.dir = dir
#         BaseLogger.path = str(dir / BaseLogger.name)
#         BaseLogger.reset_base_handler()

#     @staticmethod
#     def set_format_minimal(cls):
#         BaseLogger.format = MINIMAL_NOTEBOOK_FORMAT
#         BaseLogger.reset_base_handler()

#     @staticmethod
#     def set_format_basic():
#         BaseLogger.format = BASIC_FORMAT
#         BaseLogger.handlers["base"].setFormatter(BaseLogger.format)

#     @staticmethod
#     def set_base_level(level):
#         BaseLogger.level = level
#         BaseLogger.logger.setLevel(level)

#     @staticmethod
#     def route_to_console():
#         console_handler = logging.StreamHandler()
#         console_handler.setFormatter(BaseLogger.format)
#         BaseLogger.logger.addHandler(console_handler)
#         BaseLogger.handlers["console"] = console_handler

#     @staticmethod
#     def remove_console(cls):
#         BaseLogger.logger.removeHandler(BaseLogger.handlers["console"])
#         del BaseLogger.handlers["console"]


# def ensure_directory_exists(func):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         directory_path = kwargs.get('directory_path', './logs')
#         if not os.path.exists(directory_path):
#             os.makedirs(directory_path)
#         return func(*args, **kwargs)
#     return wrapper


# PRIDE_LOG_FILE_NAME = 'pride.log'
# RINEX_LOG_FILE_NAME = 'rinex.log'
# @ensure_directory_exists
# def setup_base_logger(directory_path='./logs'):
#     """ 
#     This function sets up the base logger for the package. Multiple loggers can be created from this base logger. 
#     The base logger only logs to a file and is used to set up other loggers.
#     """

#     # Base logger
#     base_logger = logging.getLogger('base_logger')
#     base_logger.setLevel(logging.INFO)

#     # Create a file handler for the base logger
#     base_log_file = os.path.join(directory_path, BASE_LOG_FILE_NAME)
#     base_file_handler = logging.FileHandler(base_log_file)
#     base_file_handler.setLevel(logging.INFO)

#     # Create a formatter and set it for both handlers
#     base_file_handler.setFormatter(BASIC_FORMAT)

#     # Add the handlers to the base logger
#     base_logger.addHandler(base_file_handler)

# @ensure_directory_exists
# def setup_general_logger(directory_path='./logs'):
#     """ 
#     This function sets up a general logger for the package to import and use where another logger is not specified. 
#     It will log to the base log file and print to the console.
#     """

#     # Set up the base logger
#     setup_base_logger(directory_path=directory_path)

#     # Set up a general logger (child of base logger) for most of the package to use and print to console
#     logger = logging.getLogger('base_logger.logger')
#     logger.setLevel(logging.INFO)

#     # Create a console handler for the logger
#     console_handler = logging.StreamHandler()
#     console_handler.setLevel(logging.INFO)
#     console_handler.setFormatter(BASIC_FORMAT)
#     logger.addHandler(console_handler)

#     # Set the logger to propagate to the base logger
#     logger.propagate = True

#     return logger

# @ensure_directory_exists
# def setup_pride_logger(directory_path='./logs'):

#     """ This function sets up a logger for the pride module. It logs to the base log file and prints to the console. """

#     # Pride logger (child of base_logger)
#     pride_logger = logging.getLogger('base_logger.pride_logger')
#     pride_logger.setLevel(logging.INFO)
#     pride_log_file = os.path.join(directory_path, PRIDE_LOG_FILE_NAME) # TODO - change this to a subdirectory (should it be logs/pride.log?)

#     # Create a console handler for the pride logger
#     pride_console_handler = logging.StreamHandler()
#     pride_console_handler.setLevel(logging.INFO)
#     pride_console_handler.setFormatter(BASIC_FORMAT)
#     pride_logger.addHandler(pride_console_handler)

#     # Create a file handler for the pride logger
#     pride_file_handler = logging.FileHandler(pride_log_file)
#     pride_file_handler.setLevel(logging.INFO)
#     pride_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
#     pride_logger.addHandler(pride_file_handler)

#     # Set the logger to propagate to the base logger
#     pride_logger.propagate = True

#     return pride_logger

# @ensure_directory_exists
# def setup_rinex_logger(directory_path='./logs'):
#     """ 
#     This function sets up a logger for the rinex module. 
#     It logs to the base log file and prints to the console with only the message.
#     """
#     # Rinex logger (child of base_logger)
#     rinex_logger = logging.getLogger('base_logger.rinex_logger')
#     rinex_logger.setLevel(logging.INFO)
#     rinex_log_file = os.path.join(directory_path, RINEX_LOG_FILE_NAME)

#     # Set up the formatter for the rinex logger and the console handler
#     rinex_console_handler = logging.StreamHandler()
#     rinex_console_handler.setLevel(logging.INFO)
#     rinex_console_handler.setFormatter(BASIC_FORMAT)
#     rinex_logger.addHandler(rinex_console_handler)

#     # Create a file handler for the pride logger
#     rinex_file_handler = logging.FileHandler(rinex_log_file)
#     rinex_file_handler.setLevel(logging.INFO)
#     rinex_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
#     rinex_logger.addHandler(rinex_file_handler)

#     # Set the logger to propagate to the base logger
#     rinex_logger.propagate = True

#     return rinex_logger


# def setup_notebook_logger():
#     """ 
#     This function sets up a logger for the notebook module. 
#     It logs to the base log file and prints to the console with only the message.
#     """
#     # Notebook logger (child of base_logger)
#     notebook_logger = logging.getLogger('base_logger.notebook_logger')
#     notebook_logger.setLevel(logging.INFO)

#     # Set up the formatter for the notebook logger and the console handler
#     notebook_console_handler = logging.StreamHandler()
#     notebook_console_handler.setLevel(logging.INFO)
#     notebook_console_handler.setFormatter(MINIMAL_NOTEBOOK_FORMAT)

#     # Add the console handler to the notebook logger
#     notebook_logger.addHandler(notebook_console_handler)

#     # Set the logger to propagate to the base logger
#     notebook_logger.propagate = True

#     return notebook_logger


# Set up the loggers
# logger = setup_general_logger()
# logger.info('Starting the general logger')
# pride_logger = setup_pride_logger()
# pride_logger.info('Starting the pride logger')
# notebook_logger = setup_notebook_logger()
# notebook_logger.info('Starting the notebook logger')
