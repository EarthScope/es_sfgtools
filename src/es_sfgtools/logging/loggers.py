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
import sys
from pathlib import Path
from typing import Literal

BASIC_FORMAT = logging.Formatter(
    "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
)
MINIMAL_NOTEBOOK_FORMAT = logging.Formatter("%(message)s")

BASE_LOG_FILE_NAME = "es_sfg_tools.log"
DEFAULT_PATH = os.path.join(Path.home(), ".sfgtools")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", DEFAULT_PATH)


class _BaseLogger:
    """
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
    """

    def __init__(
        self,
        name: str = "base_logger",
        dir: Path = LOG_FILE_PATH,  # Path=Path.home()/".sfgtools",
        file_name: str = BASE_LOG_FILE_NAME,
        format: logging.Formatter = BASIC_FORMAT,
        console_format: logging.Formatter = MINIMAL_NOTEBOOK_FORMAT,
        level=logging.DEBUG,
    ):

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

        # Create a file handler for the logger and always set to DEBUG
        self.file_handler = logging.FileHandler(self.path)
        self.file_handler.setFormatter(format)
        self.file_handler.setLevel(logging.DEBUG)
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

        # Remove all handlers to avoid duplicates
        for handler in list(self.logger.handlers):
            if type(handler) == logging.FileHandler:
                self.logger.removeHandler(handler)
        try:
            self.file_handler = logging.FileHandler(self.path)
            self.file_handler.setFormatter(self.format)
            self.file_handler.setLevel(logging.DEBUG)
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

    def set_level(self, level: Literal[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]) -> None:  # type: ignore
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
        if not any(type(h) == logging.StreamHandler for h in self.logger.handlers):
            self.console_handler = logging.StreamHandler()
            self.console_handler.setFormatter(self.console_format)
            self.console_handler.setLevel(logging.INFO)
            self.logger.addHandler(self.console_handler)
            self.logdebug(f"Routing {self.name} logger to console")

    def non_negotiable_console_log(self,message: str) -> str| None:
        if not hasattr(self, "console_handler"):
            return message

    def remove_console(self):
        """
        Removes the console handler from the logger.
        This method detaches the console handler from the logger instance,
        effectively stopping the logger from outputting logs to the console.
        """
        if hasattr(self, "console_handler"):
            print(f"\nRemoving {self.name} logger from console \n")
            self.logger.removeHandler(self.console_handler)

    def logdebug(self, message) -> None:
        """Log a debug message with stacklevel=2 (logging module goes up the stack to get the calling function)"""
        self.logger.debug(message, stacklevel=2)

    def loginfo(self, message) -> None:
        """Log an info message with stacklevel=2 (logging module goes up the stack to get the calling function)"""
        self.logger.info(message, stacklevel=2)

    def logerr(self, message) -> None|str:
        """Log an error message with stacklevel=2 (logging module goes up the stack to get the calling function)"""
        self.logger.error(message, stacklevel=2)
        return self.non_negotiable_console_log(message)

    def logwarn(self, message) -> None:
        """Log a warning message with stacklevel=2 (logging module goes up the stack to get the calling function)"""
        self.logger.warning(message, stacklevel=2)


def route_all_loggers_to_console():
    PRIDELogger.route_to_console()
    ProcessLogger.route_to_console()
    GarposLogger.route_to_console()



def remove_all_loggers_from_console():
    PRIDELogger.remove_console()
    ProcessLogger.remove_console()
    GarposLogger.remove_console()



def set_all_logger_levels(level: Literal[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]):  # type: ignore
    PRIDELogger.set_level(level)
    ProcessLogger.set_level(level)
    GarposLogger.set_level(level)
 


def change_all_logger_dirs(dir: Path):
    BaseLogger.set_dir(dir)
    PRIDELogger.set_dir(dir)
    ProcessLogger.set_dir(dir)
    GarposLogger.set_dir(dir)


# Create the base logger
BaseLogger = _BaseLogger()

# Create loggers for specific modules & set them to propagate to the base logger
PRIDELogger = _BaseLogger(
    name="base_logger.pride_logger",
    file_name="pride.log",
)
PRIDELogger.propagate = True

ProcessLogger = _BaseLogger(
    name="base_logger.processing_logger",
    file_name="processing.log",
)
ProcessLogger.propagate = True

GarposLogger = _BaseLogger(
    name="base_logger.garpos_logger",
    file_name="garpos.log",
)
GarposLogger.propagate = True



# Route all loggers to the console
route_all_loggers_to_console()
