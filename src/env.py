"""
This module contains functions for getting environment variables.
"""
import os


def get_env(key: str, default: str = None) -> str:
    """Gets an environment variable.

    Parameters
    ----------
    key : str
        The name of the environment variable.
    default : str, optional
        The default value to return if the environment variable is not set.

    Returns
    -------
    str
        The value of the environment variable.
    """
    return os.environ.get(key, default=default)


def get_env_required(key: str) -> str:
    """Gets a required environment variable.

    Parameters
    ----------
    key : str
        The name of the environment variable.

    Returns
    -------
    str
        The value of the environment variable.

    Raises
    ------
    KeyError
        If the environment variable is not set.
    """
    try:
        return os.environ[key]
    except KeyError:
        raise KeyError(f"The required environment variable `{key}` is missing")