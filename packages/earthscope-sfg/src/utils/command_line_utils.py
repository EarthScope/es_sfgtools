import logging
import platform
import re
import subprocess
import warnings
from pathlib import Path
from typing import Dict, List, Tuple

from ..logging.loggers import _BaseLogger
from ..logging import ProcessLogger as logger
from .custom_warnings_exceptions import (
    EXCEPTIONS_DICT_LINUX,
    EXCEPTIONS_DICT_MACOS,
    WARNINGS_DICT,
)

GOLANG_BINARY_BUILD_DIR = Path(__file__).resolve()
# Walk up to the package root (parent of "src"), then into go/build
for parent in GOLANG_BINARY_BUILD_DIR.parents:
    if parent.name == "src":
        GOLANG_BINARY_BUILD_DIR = parent.parent / "go" / "build"
        break


if GOLANG_BINARY_BUILD_DIR.exists() and not any(GOLANG_BINARY_BUILD_DIR.iterdir()):
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


def parse_cli_logs(result, logger: _BaseLogger | logging.Logger):
    if result.stdout:
        stdout_decoded = (
            result.stdout.decode("utf-8")
            if isinstance(result.stdout, bytes)
            else result.stdout
        )
        stdout_decoded = remove_ansi_escape(stdout_decoded)
        if hasattr(logger, "logdebug"):
            logger.logdebug(stdout_decoded)
        else:
            logger.debug(stdout_decoded)
        result_message = stdout_decoded.split("msg=")
        for log_line in result_message:
            message = log_line.split("\n")[0]
            if "Processed" in message or "Created" in message:
                logger.loginfo(message)
            if (exception := raise_exception(message)) is not None:
                raise exception
    if result.stderr:
        stderr_decoded = (
            result.stderr.decode("utf-8")
            if isinstance(result.stderr, bytes)
            else result.stderr
        )
        stderr_decoded = remove_ansi_escape(stderr_decoded)
        if "error" in stderr_decoded.lower():
            if hasattr(logger, "logerr"):
                logger.logerr(stderr_decoded)
            else:
                logger.error(stderr_decoded)
            if (warning := parse_error(stderr_decoded)) is not None:
                if hasattr(logger, "logwarn"):
                    logger.logwarn(warning.message)
                else:
                    logger.warning(warning.message)
                warnings.warn(warning.message, warning, 3)
        else:
            if hasattr(logger, "logwarn"):
                logger.logwarn(stderr_decoded)
            else:
                logger.warning(stderr_decoded)

        result_message = stderr_decoded.split("msg=")
        for log_line in result_message:
            message = log_line.split("\n")[0]
            if "Processing" in message or "Created" in message:
                if hasattr(logger, "loginfo"):
                    logger.loginfo(message)
                else:
                    logger.info(message)
            if (exception := raise_exception(message)) is not None:
                raise exception


def get_binary_path(
    path_map: Dict[str, Path],
    binary_name: str,
) -> Path:
    """Resolve a platform-specific binary path from a ``{system_arch: path}`` map.

    Parameters
    ----------
    path_map : dict[str, Path]
        Mapping of ``"system_arch"`` keys (e.g. ``"darwin_arm64"``) to binary paths.
    binary_name : str
        Human-readable name used in error messages when the binary is missing.

    Returns
    -------
    Path
        Resolved binary path for the current platform.

    Raises
    ------
    FileNotFoundError
        If no binary is available for the current platform.
    """
    system, arch = get_system_architecture()
    binary_path = path_map.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(
            f"{binary_name} binary not found for {system} {arch}"
        )
    return binary_path


def run_binary(
    cmd: List[str],
    log: "_BaseLogger | logging.Logger | None" = None,
    cwd: "str | Path | None" = None,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    """Run an external binary, parse its CLI logs, and return the result.

    Parameters
    ----------
    cmd : list[str]
        Command and arguments to execute.
    log : logger, optional
        Logger instance for output parsing.  Falls back to module-level logger.
    cwd : str or Path, optional
        Working directory for the subprocess.
    capture : bool, optional
        Whether to capture stdout/stderr.  Defaults to True.

    Returns
    -------
    subprocess.CompletedProcess
    """
    if log is None:
        log = logger
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=capture,
    )
    if capture:
        parse_cli_logs(result, log)
    return result
