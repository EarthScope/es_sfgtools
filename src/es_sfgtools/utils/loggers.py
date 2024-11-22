import logging

base_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')


def setup_base_logger():
    """ 
    This function sets up the base logger for the package. Multiple loggers can be created from this base logger. 
    The base logger only logs to a file and is used to set up other loggers.
    """
    # Base logger
    base_logger = logging.getLogger('base_logger')
    base_logger.setLevel(logging.INFO)

    # Create a file handler for the base logger
    base_log_file = 'es_sfg_tools.log'
    base_file_handler = logging.FileHandler(base_log_file)
    base_file_handler.setLevel(logging.INFO)

    # Create a formatter and set it for both handlers
    base_file_handler.setFormatter(base_formatter)

    # Add the handlers to the base logger
    base_logger.addHandler(base_file_handler)

    return base_logger

def setup_general_logger():
    """ 
    This function sets up a general logger for the package to import and use where another logger is not specified. 
    It will log to the base log file and print to the console.
    """
    # Set up a general logger for most of the package to use and print to console
    logger = logging.getLogger('base_logger.logger')
    logger.setLevel(logging.INFO)

    # Create a console handler for the logger
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(base_formatter)
    logger.addHandler(console_handler)

    return logger

def setup_pride_logger():
    """ This function sets up a logger for the pride module. It logs to the base log file and prints to the console. """
    # Pride logger (child of base_logger)
    pride_logger = logging.getLogger('base_logger.pride_logger')
    pride_logger.setLevel(logging.INFO)
    pride_log_file = 'pride.log' # TODO - change this to a subdirectory (should it be logs/pride.log?)

    # Create a console handler for the pride logger
    pride_console_handler = logging.StreamHandler()
    pride_console_handler.setLevel(logging.INFO)
    pride_console_handler.setFormatter(base_formatter)
    pride_logger.addHandler(pride_console_handler)

    # Create a file handler for the pride logger
    pride_file_handler = logging.FileHandler(pride_log_file)
    pride_file_handler.setLevel(logging.INFO)
    pride_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    pride_logger.addHandler(pride_file_handler)

    return pride_logger

def setup_notebook_logger():
    """ 
    This function sets up a logger for the notebook module. It logs to the base log file and prints to the console with
    only the message.
    """
    # Notebook logger (child of base_logger)
    notebook_logger = logging.getLogger('base_logger.notebook_logger')
    notebook_logger.setLevel(logging.INFO)

    # Set up the formatter for the notebook logger and the console handler
    notebook_formatter = logging.Formatter('%(message)s')
    notebook_console_handler = logging.StreamHandler()
    notebook_console_handler.setLevel(logging.INFO)
    notebook_console_handler.setFormatter(notebook_formatter)

    # Add the console handler to the notebook logger
    notebook_logger.addHandler(notebook_console_handler)

    return notebook_logger

# Set up the loggers
base_logger = setup_base_logger()
logger = setup_general_logger()
pride_logger = setup_pride_logger()
notebook_logger = setup_notebook_logger() # TODO - may not set this up automatically, but rather have the user set it up

# Set both loggers to propagate to the base logger
pride_logger.propagate = True
notebook_logger.propagate = True

# Example usage
pride_logger.info('This is an info message from pride_logger')
notebook_logger.info('This is an info message from notebook_logger')
logger.info('This is an info message from logger')