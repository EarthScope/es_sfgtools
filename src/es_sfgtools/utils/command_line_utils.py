import platform
import re
import warnings
from pathlib import Path
from typing import Tuple

# Local imports
from ..logging import ProcessLogger as logger
from .custom_warnings_exceptions import (
    EXCEPTIONS_DICT_LINUX,
    EXCEPTIONS_DICT_MACOS,
    WARNINGS_DICT,
)

GOLANG_BINARY_BUILD_DIR = "src/golangtools/build"
SELF_PATH = Path(__file__).resolve()
# find src
for parent in SELF_PATH.parents:
    if parent.name == "src":
        GOLANG_BINARY_BUILD_DIR = parent.parent / GOLANG_BINARY_BUILD_DIR
        break


if not any(GOLANG_BINARY_BUILD_DIR.iterdir()):
    logger.logwarn(
        f'Golang binaries not built. Navigate to {GOLANG_BINARY_BUILD_DIR.parent} and run "make"'
    )


def remove_ansi_escape(text):
    ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", text)


def get_system_architecture() -> Tuple[str, str]:
    """Get the current system and architecture
    Returns:
        Tuple[str, str]: A tuple containing the system and architecture.
    """
    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    return system, arch

PRIDE_MESSAGE_0 = "input interval is shorter than observation interval"

def parse_error(string: str) -> Warning | None:
    for key, warning_ in WARNINGS_DICT.items():
        if key in string:
            return warning_
    return None

def raise_exception(string: str) -> Exception | None:
    string = string.strip()
    sys, _ = get_system_architecture()
    if sys == "linux":
        exceptions_dict = EXCEPTIONS_DICT_LINUX
    else:
        exceptions_dict = EXCEPTIONS_DICT_MACOS

    for key, exception in exceptions_dict.items():
        if key.strip() in string:
            return exception
    return None

def parse_cli_logs(result, logger):
    if result.stdout:
        stdout_decoded = result.stdout.decode("utf-8") if isinstance(result.stdout, bytes) else result.stdout
        stdout_decoded = remove_ansi_escape(stdout_decoded)
        logger.logdebug(stdout_decoded)
        result_message = stdout_decoded.split("msg=")
        for log_line in result_message:
            message = log_line.split("\n")[0]
            if "Processed" in message or "Created" in message:
                logger.loginfo(message)
            if (exception := raise_exception(message)) is not None:
                raise exception
    if result.stderr:
        stderr_decoded = result.stderr.decode("utf-8") if isinstance(result.stderr, bytes) else result.stderr
        stderr_decoded = remove_ansi_escape(stderr_decoded)
        if "error" in stderr_decoded.lower():
            logger.logerr(stderr_decoded)
            if (warning := parse_error(stderr_decoded)) is not None:
                logger.logwarn(warning.message)
                warnings.warn(warning.message, warning, 3)
        else:
            logger.logwarn(stderr_decoded)
        
        result_message = stderr_decoded.split("msg=")
        for log_line in result_message:
            message = log_line.split("\n")[0]
            if "Processing" in message or "Created" in message:
                logger.loginfo(message)
            if (exception := raise_exception(message)) is not None:
                raise exception