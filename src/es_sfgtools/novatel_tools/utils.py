# External imports
from pathlib import Path
import platform
import subprocess
from typing import Tuple
# Local imports
from ..logging import ProcessLogger as logger
from ..utils.command_line_utils import GOLANG_BINARY_BUILD_DIR,get_system_architecture, parse_cli_logs


RINEX_BIN_PATH = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "nova2rnxo_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "nova2rnxo_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "nova2rnxo_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "nova2rnxo_linux_arm64",
}

RINEX_BIN_PATH_BINARY = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "novb2rnxo_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "novb2rnxo_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "novb2rnxo_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "novb2rnxo_linux_arm64",
}

NOVA2TILE_BIN_PATH = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "nova2tile_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "nova2tile_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "nova2tile_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "nova2tile_linux_arm64",
}

NOVB2TILE_BIN_PATH = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "novab2tile_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "novab2tile_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "novab2tile_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "novab2tile_linux_arm64",
}

NOV0002TILE_BIN_PATH = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "nov0002tile_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "nov0002tile_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "nov0002tile_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "nov0002tile_linux_arm64",
}

def get_nova2rnxo_binary_path() -> Path:
    """Get the path to the nova2rnxo golang binary based on the current platform."""
    system, arch = get_system_architecture()
    binary_path = RINEX_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"NOVA2RNXO binary not found for {system} {arch}")

    return binary_path

def get_novb2rnxo_binary_path() -> Path:
    """Get the path to the novb2rnxo binary based on the current platform."""
    system, arch = get_system_architecture()
    binary_path = RINEX_BIN_PATH_BINARY.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"NOVB2RNXO binary not found for {system} {arch}")

    return binary_path      

def get_nova2tile_binary_path() -> Path:
    """Get the path to the nova2tile golang binary based on the current platform."""
    system, arch = get_system_architecture()
    binary_path = NOVA2TILE_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"NOVA2TILE binary not found for {system} {arch}")

    return binary_path

def get_nov_770_tile_binary_path() -> Path:
    """Get the path to the novb2tile golang binary based on the current platform."""
    system, arch = get_system_architecture()
    binary_path = NOVB2TILE_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"NOVB2TILE binary not found for {system} {arch}")

    return binary_path

def get_nov_000_tile_binary_path() -> Path:
    """Get the path to the nov0002tile golang binary based on the current platform."""
    system, arch = get_system_architecture()
    binary_path = NOV0002TILE_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"NOV0002TILE binary not found for {system} {arch}")

    return binary_path

def get_metadatav2(
    site: str,
    serialNumber: str = "XXXXXXXXXX",
    antennaPosition: list = [0, 0, 0],
    antennaeOffsetHEN: list = [0, 0, 0],
) -> dict:
    # TODO: these are placeholder values, need to use real metadata

    return {
        "rinex_version": "2.11",
        "rinex_type": "O",
        "rinex_system": "G",
        "marker_name": site,
        "marker_number": "0001",
        "markerType": "GEODETIC",
        "observer": "EarthScope",
        "agency": "EarthScope",
        "program": "gnsstools",
        "run_by": "",
        "date": "",
        "receiver_model": "NOV",
        "receiver_serial": serialNumber,
        "receiver_firmware": "0.0.0",
        "antenna_model": "TRM59800.00 SCIT",
        "antenna_serial": "987654321",
        "antenna_position": antennaPosition,
        "antenna_offsetHEN": antennaeOffsetHEN,
    }


def get_metadata(site: str, serialNumber: str = "XXXXXXXXXX") -> dict:
    # TODO: these are placeholder values, need to use real metadata
    return {
        "markerName": site,
        "markerType": "WATER_CRAFT",
        "observer": "PGF",
        "agency": "Pacific GPS Facility",
        "receiver": {
            "serialNumber": "XXXXXXXXXX",
            "model": "NOV OEMV1",
            "firmware": "4.80",
        },
        "antenna": {
            "serialNumber": "ACC_G5ANT_52AT1",
            "model": "NONE",
            "position": [
                0.000,
                0.000,
                0.000,
            ],  # reference position for site what ref frame?
            "offsetHEN": [0.0, 0.0, 0.0],  # read from lever arms file?
        },
    }
