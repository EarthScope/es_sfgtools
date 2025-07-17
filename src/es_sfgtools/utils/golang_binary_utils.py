import platform
from pathlib import Path
from typing import Tuple

# Local imports
from ..logging import ProcessLogger as logger

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


def parse_golang_logs(result, logger):
    if result.stdout:
        logger.logdebug(result.stdout.decode("utf-8"))
        result_message = result.stdout.decode("utf-8").split("msg=")
        for log_line in result_message:
            message = log_line.split("\n")[0]
            if "Processed" in message or "Created" in message:
                logger.loginfo(message)

    if result.stderr:
        logger.logdebug(result.stderr.decode("utf-8"))
        result_message = result.stderr.decode("utf-8").split("msg=")
        for log_line in result_message:
            message = log_line.split("\n")[0]
            if "Processing" in message or "Created" in message:
                logger.loginfo(message)
