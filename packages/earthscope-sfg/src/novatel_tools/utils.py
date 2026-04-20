# External imports
import datetime
import json
from pathlib import Path

from pydantic import BaseModel, Field

# Local imports
from ..utils.command_line_utils import (
    GOLANG_BINARY_BUILD_DIR,
    get_binary_path,
)

RINEX_BIN_PATH = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "nova2rnx_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "nova2rnx_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "nova2rnx_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "nova2rnx_linux_arm64",
}


RINEX_BIN_PATH_BINARY = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "novb2rnx_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "novb2rnx_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "novb2rnx_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "novb2rnx_linux_arm64",
}

RINEX_0000_PATH_BINARY = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "nov0002rnx_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "nov0002rnx_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "nov0002rnx_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "nov0002rnx_linux_arm64",
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

TDB2RNX_BIN_PATH = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_linux_arm64",
}


def get_nova2rnx_binary_path() -> Path:
    """Get the path to the nova2rnxo golang binary based on the current platform."""
    return get_binary_path(RINEX_BIN_PATH, "NOVA2RNXO")


def get_novb2rnxo_binary_path() -> Path:
    """Get the path to the novb2rnxo binary based on the current platform."""
    return get_binary_path(RINEX_BIN_PATH_BINARY, "NOVB2RNXO")


def get_nov0002rnx_binary_path() -> Path:
    """Get the path to the nov0002rnx binary based on the current platform."""
    return get_binary_path(RINEX_0000_PATH_BINARY, "NOV0002RNX")


def get_nova2tile_binary_path() -> Path:
    """Get the path to the nova2tile golang binary based on the current platform."""
    return get_binary_path(NOVA2TILE_BIN_PATH, "NOVA2TILE")


def get_nov_770_tile_binary_path() -> Path:
    """Get the path to the novb2tile golang binary based on the current platform."""
    return get_binary_path(NOVB2TILE_BIN_PATH, "NOVB2TILE")


def get_nov_000_tile_binary_path() -> Path:
    """Get the path to the nov0002tile golang binary based on the current platform."""
    return get_binary_path(NOV0002TILE_BIN_PATH, "NOV0002TILE")


def get_tdb2rnx_binary_path() -> Path:
    """Get the path to the tdb2rnx golang binary based on the current platform."""
    return get_binary_path(TDB2RNX_BIN_PATH, "TDB2RNX")


"""

// Settings - settings for RINEX header
type Settings struct {
	RinexVersion     RinexVersion         `json:"rinex_version"`
	RinexType        string               `json:"rinex_type"`
	RinexSystem      string               `json:"rinex_system"`
	Interval         float64              `json:"interval"`
	TimeOfFirst      time.Time            `json:"time_of_first"`
	TimeOfLast       time.Time            `json:"time_of_last"`
	MarkerName       string               `json:"marker_name"`
	MarkerNumber     string               `json:"marker_number"`
	MarkerType       string               `json:"marker_type"`
	Observer         string               `json:"observer"`
	Agency           string               `json:"agency"`
	Program          string               `json:"program"`
	RunBy            string               `json:"run_by"`
	Date             string               `json:"date"`
	Comments         []string             `json:"comments"`
	ReceiverModel    string               `json:"receiver_model"`
	ReceiverSerial   string               `json:"receiver_serial"`
	ReceiverFirmware string               `json:"receiver_firmware"`
	AntennaModel     string               `json:"antenna_model"`
	AntennaSerial    string               `json:"antenna_serial"`
	AntennaPosition  coordinates.Vector3D `json:"antenna_position"`
	AntennaOffsetHEN coordinates.Vector3D `json:"antenna_offsetHEN"`
	// Sets which observation types to output (e.g. range, phase, doppler, snr)
	OutputSettings *OutputSettings `json:"output_settings"`
	// This stores the order of the observation codes for each system.
	// The order of the observation codes is important for the RINEX 3&4 headers.
	// Important: RINEX 3 and 4 cannot be streamed because the header must be written before the data.
	ObservationsBySystem *observation.ObservationsBySystem `json:"observation_map"`
}
"""


class MetadataModel(BaseModel):
    marker_name: str = Field(..., description="Site name")
    rinex_version: str | None = Field(default="2.11", description="RINEX version")
    rinex_type: str | None = Field(default="O", description="RINEX type")
    rinex_system: str | None = Field(default="G", description="RINEX system")
    marker_number: str | None = Field(default="0001", description="Marker number")
    marker_type: str | None = Field(default="GEODETIC", description="Marker type")
    observer: str | None = Field(default="EarthScope", description="Observer name")
    agency: str | None = Field(default="EarthScope", description="Agency name")
    program: str | None = Field(default="gnsstools", description="Program name")
    run_by: str | None = Field(default="", description="Run by")
    date: str | None = Field(
        default_factory=lambda: (
            datetime.datetime.now(tz=datetime.UTC).isoformat()
        ),
        description="Date",
    )
    receiver_model: str | None = Field(default="NOV", description="Receiver model")
    receiver_serial: str | None = Field(
        default="XXXXXXXXXX", description="Receiver serial number"
    )
    antenna_position: list[float] | None = Field(
        default=[0.0, 0.0, 0.0], description="Antenna position [X, Y, Z]"
    )
    antenna_offsetHEN: list[float] | None = Field(
        default=[0.0, 0.0, 0.0], description="Antenna offset [H, E, N]"
    )
    antenna_model: str | None = Field(
        default="NOV850 NONE", description="Antenna model"
    )
    antenna_serial: str | None = Field(
        default="987654321", description="Antenna serial number"
    )


def get_metadatav2(
    site: str,
    serialNumber: str = "XXXXXXXXXX",
    antennaPosition: list = None,
    antennaeOffsetHEN: list = None,
) -> dict:
    # TODO: these are placeholder values, need to use real metadata
    if antennaPosition is None:
        antennaPosition = [0, 0, 0]
    if antennaeOffsetHEN is None:
        antennaeOffsetHEN = [0, 0, 0]

    return {
        "rinex_version": "2.11",
        "rinex_type": "O",
        "rinex_system": "G",
        "marker_name": site,
        "marker_number": "0001",
        "marker_type": "GEODETIC",
        "observer": "EarthScope",
        "agency": "EarthScope",
        "program": "gnsstools",
        "run_by": "",
        "date": "",
        "receiver_model": "NOV",
        "receiver_serial": serialNumber,
        "receiver_firmware": "0.0.0",
        "antenna_model": "NOV850 NONE",
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


def check_metadata_path(metadata_path: Path | str) -> str:
    """Validate and normalize a metadata file path.

    This function ensures that the given path points to an existing JSON
    metadata file and that its contents conform to the ``MetadataModel`` schema.

    Parameters
    ----------
    metadata_path (Path | str)
        The path to the metadata JSON file.

    Returns
    -------
    str
        The validated metadata file path as a string.

    Raises
    ------
    AssertionError
        If the metadata file does not exist.
    ValueError
        If the metadata file cannot be parsed into ``MetadataModel``.
    """
    if isinstance(metadata_path, str):
        metadata_path = Path(metadata_path)
    assert metadata_path.exists(), f"Metadata file {str(metadata_path)} does not exist"
    with open(metadata_path) as f:
        metadata_dict = json.load(f)
    try:
        _ = MetadataModel(**metadata_dict).model_dump()
        return str(metadata_path)
    except Exception as e:  # pragma: no cover - defensive logging
        raise ValueError(f"Error parsing metadata file {str(metadata_path)}: {e}") from e


def check_metadata(meta: dict | MetadataModel) -> dict:
    """Validate and normalize metadata input into a dictionary.

    This function accepts either a raw metadata dictionary or a `MetadataModel`
    instance, validates it against the `MetadataModel` schema, and returns a
    dictionary representation.

    If a dictionary is provided, it is used to instantiate `MetadataModel`; any
    validation error will raise a `ValueError`. If a `MetadataModel` instance is
    provided, it is converted to a dictionary via `model_dump()`.

    Parameters
    ----------
    meta : dict or MetadataModel
        Metadata to validate and normalize.

    Returns
    -------
    dict
        A dictionary representation of the validated metadata.

    Raises
    ------
    ValueError
        If `meta` is not a `dict` or `MetadataModel`, or if validation of the
        provided metadata fails.
    """
    if isinstance(meta, dict):
        try:
            _ = MetadataModel(**meta).model_dump()
            return meta
        except Exception as e:  # pragma: no cover - defensive logging
            raise ValueError(f"Error parsing metadata dictionary: {e}") from e
    elif isinstance(meta, MetadataModel):
        return meta.model_dump()
    else:
        raise ValueError(f"Metadata must be a dict or MetadataModel, got {type(meta)}")
