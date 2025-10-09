"""
This module contains functions for getting environment variables.
"""
import os

def get_env(key: str, default: str = None) -> str:
    """
    Gets an environment variable.

    :param key: The name of the environment variable.
    :type key: str
    :param default: The default value to return if the environment variable is not set.
    :type default: str, optional
    :return: The value of the environment variable.
    :rtype: str
    """
    return os.environ.get(key, default=default)


def get_env_required(key: str) -> str:
    """
    Gets a required environment variable.

    :param key: The name of the environment variable.
    :type key: str
    :return: The value of the environment variable.
    :rtype: str
    :raises KeyError: If the environment variable is not set.
    """
    try:
        return os.environ[key]
    except KeyError:
        raise KeyError(f"The required environment variable `{key}` is missing")
