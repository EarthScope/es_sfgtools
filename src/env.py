import os

def get_env(key: str, default: str = None) -> str:
    return os.environ.get(key, default=default)


def get_env_required(key: str) -> str:
    try:
        return os.environ[key]
    except KeyError:
        raise KeyError(f"The required environment variable `{key}` is missing")
