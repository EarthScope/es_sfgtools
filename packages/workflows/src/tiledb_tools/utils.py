# External imports
from pathlib import Path

# Local imports
from es_sfgtools.utils.command_line_utils import (
    GOLANG_BINARY_BUILD_DIR,
    get_binary_path,
)

TILE2RINEX_BIN_PATH = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_linux_arm64",
}


def get_tile2rinex_binary_path() -> Path:
    """Get the path to the tile2rinex golang binary based on the current platform."""
    return get_binary_path(TILE2RINEX_BIN_PATH, "TILE2RINEX")
