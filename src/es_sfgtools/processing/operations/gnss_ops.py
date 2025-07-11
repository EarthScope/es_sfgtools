from enum import Enum
import pandas as pd
from pydantic import BaseModel, Field, model_validator, ValidationError
import pandera as pa
from pandera.typing import DataFrame
from datetime import datetime
from typing import List, Optional, Union,Literal,Tuple
import logging
import julian
import os
import tempfile
import subprocess
from concurrent.futures import ProcessPoolExecutor as Pool
from functools import partial
import shutil
import json
import platform
from pathlib import Path
import re
import numpy as np
import uuid
from warnings import warn
import matplotlib.pyplot as plt

from ..assets.file_schemas import AssetEntry,AssetType
from ..assets.observables import GNSSDataFrame
from .pride_utils import get_nav_file,get_gnss_products

from es_sfgtools.utils.loggers import GNSSLogger as logger

RINEX_BINARIES = "src/golangtools/build"
SELF_PATH = Path(__file__).resolve()
# find src
for parent in SELF_PATH.parents:
    if parent.name == "src":
        RINEX_BINARIES = Path(str(parent.parent) +"/"+ RINEX_BINARIES)
        break


if not any(RINEX_BINARIES.iterdir()):
    logger.logwarn(f'Golang binaries not built. Navigate to {RINEX_BINARIES.parent} and run "make"' )
    #raising UserWarning prevents import of data handler
    #raise UserWarning(f'Golang binaries not built. Navigate to {RINEX_BINARIES.parent} and run "make"' )


PRIDE_PPP_LOG_INDEX = {
    0: "modified_julian_date",
    1: "second_of_day",
    2: "east",
    3: "north",
    4: "up",
    5: "latitude",
    6: "longitude",
    7: "height",
    8: "number_of_satellites",  # 8 is minimum number of satellites for accurate positioning
    9: "pdop",
}

RINEX_BIN_PATH = {
    "darwin_amd64": RINEX_BINARIES / "nova2rnxo_darwin_amd64",
    "darwin_arm64": RINEX_BINARIES / "nova2rnxo_darwin_arm64",
    "linux_amd64": RINEX_BINARIES / "nova2rnxo_linux_amd64",
    "linux_arm64": RINEX_BINARIES / "nova2rnxo_linux_arm64",
}


RINEX_BIN_PATH_BINARY = {
    "darwin_amd64": RINEX_BINARIES / "novb2rnxo_darwin_amd64",
    "darwin_arm64": RINEX_BINARIES / "novb2rnxo_darwin_arm64",
    "linux_amd64": RINEX_BINARIES / "novb2rnxo_linux_amd64",
    "linux_arm64": RINEX_BINARIES / "novb2rnxo_linux_arm64",
}

NOVA2TILE_BIN_PATH = {
    "darwin_arm64": RINEX_BINARIES / "nova2tile_darwin_arm64",
    "linux_amd64": RINEX_BINARIES / "nova2tile_linux_amd64",
    "linux_arm64": RINEX_BINARIES / "nova2tile_linux_arm64",
}

NOVB2TILE_BIN_PATH = {
    "darwin_arm64": RINEX_BINARIES / "novab2tile_darwin_arm64",
    "linux_amd64": RINEX_BINARIES / "novab2tile_linux_amd64",
    "linux_arm64": RINEX_BINARIES / "novab2tile_linux_arm64",
}

NOV0002TILE_BIN_PATH = {
    "darwin_arm64": RINEX_BINARIES / "nov0002tile_darwin_arm64",
    "linux_amd64": RINEX_BINARIES / "nov0002tile_linux_amd64",
    "linux_arm64": RINEX_BINARIES / "nov0002tile_linux_arm64",
}

TILE2RINEX_BIN_PATH = {
    "darwin_arm64": RINEX_BINARIES / "tdb2rnx_darwin_arm64",
    "linux_amd64": RINEX_BINARIES / "tdb2rnx_linux_amd64",
    "linux_arm64": RINEX_BINARIES / "tdb2rnx_linux_arm64",
}

class PridePPP(BaseModel):
    """
    Data class for PPP GNSS kinematic position output
    Docs: https://github.com/PrideLab/PRIDE-PPPAR
    """

    modified_julian_date: float = Field(ge=0)
    second_of_day: float = Field(ge=0, le=86400)
    east: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF X coordinate
    north: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF Y coordinate
    up: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF Z coordinate
    latitude: float = Field(ge=-90, le=90)  # WGS84 latitude
    longitude: float = Field(ge=0, le=360)  # WGS84 longitude
    height: float = Field(ge=-101, le=100)  # WGS84 height (m)
    number_of_satellites: int = Field(
        default=1, ge=0, le=125
    )  # Average Number of available satellites
    pdop: float = Field(default=0, ge=0, le=1000)  # Position Dilution of Precision
    time: Optional[datetime] = None

    class Config:
        coerce = True

    @model_validator(mode="before")
    def validate_time(cls, values):
        values["pdop"] = float(values.get("pdop", 0.0))
        return values

    @model_validator(mode="after")
    def populate_time(cls, values):
        """Convert from modified julian date and seconds of day to standard datetime format"""
        values = values
        julian_date = (
            values.modified_julian_date + (values.second_of_day / 86400) + 2400000.5
        )
        t = julian.from_jd(julian_date, fmt="jd")
        values.time = t

        return values

    @classmethod
    def from_kin_file(cls, data: List[str]) -> Union["PridePPP", ValidationError]:
        """
        Read kinematic position file and return a DataFrame
        """
        try:
            data_dict = {}
            if "*" in data:
                data.remove("*")

            if len(data) < 10:
                data.insert(-1, 1)  # account for missing number of satellites

            for i, item in enumerate(data):
                field = PRIDE_PPP_LOG_INDEX[i]
                data_dict[field] = item
            return cls(**data_dict)
        except ValidationError as e:
            raise Exception(f"Error parsing into PridePPP {e}")

# define system enum to use
class Constellations(str, Enum):
    GPS = "G"
    GLONASS = "R"
    GLAILEO = "E"
    BDS = "C"
    BDS_TWO = "2"
    BDS_THREE = "3"
    QZSS = "J"

    @classmethod
    def print_options(cls):
        print("System options are:")
        for option in cls:
            print(f"{option.value} for {option.name}")

class Tides(str, Enum):
    SOLID = "S"
    OCEAN = "O"
    POLAR = "P"

    @classmethod
    def print_options(cls):
        print("Tide options are:")
        for option in cls:
            print(f"{option.value} for {option.name}")

class PridePdpConfig(BaseModel):
    '''
    PridePdpConfig is a configuration class for setting up and generating commands to run the pdp3 GNSS processing tool.
    Attributes:
        system (str): The GNSS system(s) to use. Default is "GREC23J" which is “GPS/GLONASS/Galileo/BDS/BDS-2/BDS-3/QZSS”.
        frequency (list): The GNSS frequencies to use. Default is ["G12", "R12", "E15", "C26", "J12"]. Refer to Table 5-4 in PRIDE-PPP-AR v.3.0 manual for more options.
        loose_edit (bool): Disable strict editing mode, which should be used when high dynamic data quality is poor. Default is True.
        cutoff_elevation (int): The elevation cutoff angle in degrees (0-60 degrees). Default is 7.
        start (datetime): The start time used for processing. Default is None.
        end (datetime): The end time used for processing. Default is None.
        interval (float): Processing interval, values range from 0.02s to 30s. If this item is not specified and the configuration file is specified, the processing interval in the configuration file will be read, otherwise, the sampling rate of the observation file is used by default.
        high_ion (bool): Use 2nd ionospheric delay model with CODE's GIM product. When this option is not entered, no higher-order ionospheric correction is performed. Default is False.
        tides (str): Enter one or more of "S" "O" "P", e.g SO for solid, ocean, and polar tides. Default is "SOP", which uses all tides.
        local_pdp3_path (str): The path to the local pdp3 binary. Default is None.
    Methods:
        generate_pdp_command(site: str, local_file_path: str) -> List[str]:
            Generate the command to run pdp3 with the given parameters.
                site (str): The site identifier for the GNSS data.
                local_file_path (str): The local file path to the GNSS data.
            Returns:
                List[str]: The command to run pdp3 with the specified configuration.
    '''
    sample_frequency: float = 1
    system: str = "GREC23J"
    frequency: list = ["G12", "R12", "E15", "C26", "J12"]
    loose_edit: bool = True 
    cutoff_elevation: int = 7
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    interval: Optional[float] = None
    high_ion: Optional[bool] = None
    tides: str = "SOP"
    local_pdp3_path: Optional[str] = None
    override: bool = False
    override_products_download: bool = Field(
        False, title="Flag to Override Existing Products Download"
    )
    
    def __post_init__(self):
        # Check if system is valid
        system = self.system.upper() # Default to GREC23J which is “GPS/GLONASS/Galileo/BDS/BDS-2/BDS-3/QZSS”
        for char in system:
            if char not in Constellations._value2member_map_:
                Constellations.print_options()
                raise ValueError(f"Invalid constelation character: {char}")

        # If entered, check if tide characters are valid
        tides = self.tides.upper()
        for char in tides:
            if char not in Tides._value2member_map_:
                Tides.print_options()
                raise ValueError(f"Invalid tide character: {char}")
            

    def generate_pdp_command(self, site: str, local_file_path: str, start:datetime, end:datetime) -> List[str]:
        """
        Generate the command to run pdp3 with the given parameters
        """

        self.start = start if start is not None else self.start
        self.end = end if end is not None else self.end

        if self.local_pdp3_path:
            if 'pdp3' in self.local_pdp3_path:
                command = [self.local_pdp3_path]
            else:
                command = [os.path.join(self.local_pdp3_path, 'pdp3')]
        else:
            command = ["pdp3"]

        command.extend(["-m", "K"])

        command.extend(["-i", str(self.sample_frequency)])

        if self.system != "GREC23J":
            command.extend(["--system", self.system])

        if self.frequency != ["G12", "R12", "E15", "C26", "J12"]:
            command.extend(["--frequency", " ".join(self.frequency)])

        if self.loose_edit:
            command.append("--loose-edit")

        if self.cutoff_elevation != 7:
            command.extend(["--cutoff-elev", str(self.cutoff_elevation)])

        if self.start:
            command.extend(["--start", self.start.strftime("%Y-%m-%d %H:%M:%S")])

        if self.end:
            command.extend(["--end", self.end.strftime("%Y-%m-%d %H:%M:%S")])

        if self.interval:
            command.extend(["--interval", str(self.interval)])

        if self.high_ion:
            command.append("--high-ion")

        if self.tides != "SOP":
            command.extend(["--tide-off", self.tides])

        command.extend(["--site", site])
        command.append(str(local_file_path))

        return command


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


def _rinex_get_time(line):
    time_values = line.split("GPS")[0].strip().split()
    start_time = datetime(
        year=int(time_values[0]),
        month=int(time_values[1]),
        day=int(time_values[2]),
        hour=int(time_values[3]),
        minute=int(time_values[4]),
        second=int(float(time_values[5])),
    )
    return start_time


def rinex_get_meta(source:AssetEntry) ->AssetEntry:
    assert source.type == AssetType.RINEX, f"Expected RINEX file, got {source.type}"

    if source.timestamp_data_start is not None:
        year = str(source.timestamp_data_start.year)[2:]
    with open(source.local_path) as f:
        files = f.readlines()
        for line in files:
            if source.timestamp_data_start is None:
                if "TIME OF FIRST OBS" in line:
                    start_time = _rinex_get_time(line)
                    file_date = start_time.strftime("%Y%m%d%H%M")
                    source.timestamp_data_start = start_time
                    source.timestamp_data_end = start_time
                    year = str(source.timestamp_data_start.year)[2:]
                    break
            
        
            if source.timestamp_data_start is not None:
                # line sample: 23  6 24 23 59 59.5000000  0  9G21G27G32G08G10G23G24G02G18
                if line.strip().startswith(year):
                    date_line = line.strip().split()
                    try:
                        current_date = datetime(
                            year=2000 + int(date_line[0]),
                            month=int(date_line[1]),
                            day=int(date_line[2]),
                            hour=int(date_line[3]),
                            minute=int(date_line[4]),
                            second=int(float(date_line[5])),
                        )
                        if current_date > source.timestamp_data_start:
                            source.timestamp_data_end = current_date
                    except Exception as e:
                        pass
    if source.timestamp_data_start is not None and source.timestamp_data_end == source.timestamp_data_start:
        source.timestamp_data_end = datetime(
            year=source.timestamp_data_start.year,
            month=source.timestamp_data_start.month,
            day=source.timestamp_data_start.day,
            hour=23,
            minute=59,
            second=59,
        )
    return source


def _novatel_to_rinex(
    source_list: List[str],
    writedir: Path,
    site: str,
    source_type: Literal[AssetType.NOVATEL,AssetType.NOVATEL770],
    show_details: bool = False
) -> List[Path]:
    """
    Given a list of paths to NovAtel files, convert them to daily RINEX files.

    Args:
        source_list (List[str]): List of source file paths.
        writedir (Path): Directory where the generated RINEX files will be written.
        site (str): Site identifier.
        source_type (Literal[AssetType.NOVATEL,AssetType.NOVATEL770]): Type of source files.
        show_details (bool, optional): Flag to indicate whether to show conversion details. Defaults to False.

    Returns:
        List[Path]: List of paths to the generated RINEX files.

    Examples:
        >>> novatel_paths = ["/path/to/NCB1_09052024_NOV777.raw", "/path/to/NCB1_09062024_NOV777.raw"]
        >>> writedir = Path("/writedir")
        >>> site = "NCB1"
        >>> source_type = AssetType.NOVATEL
        >>> rinex_files: List[Path] = _novatel_to_rinex(novatel_paths, writedir, site, source_type)

    """
    # Sort sourcelist
    def sort_key(path: Path):
        name = path.name.replace('NOV', '').replace("_", "")
        # replace all non numeric characters with empty string
        name_non_numeric = re.sub(r"\D", "", name)
        return int(name_non_numeric)

    source_list = sorted(source_list, key=sort_key)

    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    if source_type in [AssetType.NOVATEL,AssetType.NOVATELPIN]:
        binary_path = RINEX_BIN_PATH[f"{system}_{arch}"]
    else:
        binary_path = RINEX_BIN_PATH_BINARY[f"{system}_{arch}"]

    metadata = get_metadata(site, serialNumber=uuid.uuid4().hex[:10])

    logger.loginfo(f"Converting and merging {len(source_list)} files of type {source_type.value} to RINEX")
    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        metadata_path = Path(workdir) / "metadata.json"
        with open(metadata_path, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)

        cmd = [
            str(binary_path),
            "-meta",
            str(metadata_path)
        ] + [str(x) for x in source_list]

        logger.loginfo(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, capture_output=True, cwd=workdir)

        if result.stdout:
            logger.logdebug(result.stdout.decode("utf-8"))

        if result.stderr:
            logger.logdebug(result.stderr.decode("utf-8"))
            result_message = result.stderr.decode("utf-8").split("msg=")
            for log_line in result_message:
                message = log_line.split("\n")[0]
                if "Processing" in message or "Created" in message:
                    logger.loginfo(message)

        rnx_files = list(Path(workdir).rglob(f"*{site}*"))
        logger.loginfo(
            f"Converted {len(source_list)} files of type {source_type.value} to {len(rnx_files)} Daily RINEX files"
        )
        rinex_files = []
        for rinex_file_path in rnx_files:
            new_rinex_path = writedir / rinex_file_path.name
            shutil.move(src=rinex_file_path, dst=new_rinex_path)
            logger.loginfo(f"Generated Daily RINEX file {str(new_rinex_path)}")
            rinex_files.append(new_rinex_path)
    return rinex_files


def novatel_to_rinex_batch(
    source: List[AssetEntry],
    writedir: Path|str = None,
    show_details: bool = False
) -> List[AssetEntry]:

    """
    Given a set of AssetEntry objects representing AssetType.NOVATEL or AssetType.NOVATEL770 files, convert them to daily RINEX files representing
    each distinct day of data.

    Args:
        source (List[AssetEntry]): List of AssetEntry objects representing the source files.
        writedir (Path|str, optional): Directory where the RINEX files will be written. If not provided, the RINEX files will be written to the same directory as the source files. Defaults to None.
        show_details (bool, optional): Flag indicating whether to show detailed conversion information. Defaults to False.
    Returns:
        List[AssetEntry]: List of AssetEntry objects representing the converted RINEX files.

    Examples:
        >>> asset_entry_0 = AssetEntry(local_path="/path/to/NCB1_09052024_NOV777.raw", type=AssetType.NOVATEL, network="NCB", station="NCB1", campaign="JULY2024")
        >>> asset_entry_1 = AssetEntry(local_path="/path/to/NCB1_09062024_NOV777.raw", type=AssetType.NOVATEL, network="NCB", station="NCB1", campaign="JULY2024")
        >>> writedir = Path("/writedir")
        >>> rinex_assets: List[AssetEntry] = novatel_to_rinex([asset_entry_0, asset_entry_1], writedir, show_details=True)
        >>> rinex_assets[0].model_dump()
        {'local_path': '/writedir/NCB1_09052024_NOV777.raw', 'type': 'rinex', 'network': 'NCB', 'station': 'NCB1', 'campaign': 'JULY2024', 'timestamp_created': datetime.datetime(2024, 7, 9, 12, 0, 0, 0)}
    """
    assert len(set([x.type for x in source])) == 1, "All sources must be of the same type"

    source_type = source[0].type
    site = source[0].station
    network = source[0].network
    campaign = source[0].campaign
    station = source[0].station

    if isinstance(writedir, str):
        writedir = Path(writedir)
    elif writedir is None:
        writedir = source[0].local_path.parent
    if site is None:
        site = "SIT1"
    rinex_paths = _novatel_to_rinex(
        source_list=[x.local_path for x in source],
        writedir=writedir,
        show_details=show_details,
        site=site,
        source_type=source_type
    )
    rinex_assets = []
    for rinex_path in rinex_paths:
        rinex_asset = AssetEntry(
            local_path=rinex_path,
            type=AssetType.RINEX,
            network=network,
            station=station,
            campaign=campaign,
            timestamp_created=datetime.now(),
        )
        rinex_asset = rinex_get_meta(rinex_asset)
        rinex_assets.append(rinex_asset)

    return rinex_assets

def novatel_to_rinex(
    source:str | List[str],
    show_details: bool = False
) -> List[str]:
    """
    Given a path to a Novatel ascii or raw file, convert it to daily RINEX files representing each distinct day of data.
    
    Parameters:
        source (str): The path to the Novatel GNSS file.
        show_details (bool, optional): Whether to show detailed information during the conversion process. Defaults to False.
    Returns:
        List[str]: A list of paths to the generated RINEX files.

    Examples:
        >>> source = "/path/to/NCB1_09052024_NOV777.raw"
        >>> rinex_files: List[str] = novatel_to_rinex(source, show_details=True)
        >>> rinex_files
        ["/path/to/NCB12450.24o", "/path/to/NCB12460.24o"]
    """
    source = Path(source)
    if "NOV770" in source.name:
        source_type = AssetType.NOVATEL770
    else:
        source_type = AssetType.NOVATEL
    site = "SIT1"

    if isinstance(source, str):
        source = [source]

    writedir = source.parent
    rinex_files = _novatel_to_rinex(
        source_list=source,
        writedir=writedir,
        show_details=show_details,
        site=site,
        source_type=source_type
    )
    return rinex_files

def remove_ansi_escape(text):
    ansi_escape = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)

# TODO: handle logging with multiprocessing https://signoz.io/guides/how-should-i-log-while-using-multiprocessing-in-python/
def rinex_to_kin(
    source: Union[AssetEntry,str,Path],
    writedir: Path,
    pridedir: Path,
    site="SIT1",
    pride_config: PridePdpConfig = None,
    show_details: bool = True
) -> Tuple[AssetEntry]:

    """
    Converts a RINEX file to a kin file.
    Args:
        source (Union[AssetEntry,str,Path]): The source RINEX file to convert.
        writedir (Path): The directory to write the converted kin file.
        pridedir (Path): The directory where PRIDE-PPP observables are stored.
        site (str, optional): The site name. Defaults to "SITE1".
        show_details (bool, optional): Whether to show conversion details. Defaults to True.
    Returns:
        Tuple[AssetEntry]: The generated kin and result files as AssetEntry objects.
    Raises:
        FileNotFoundError: If the PRIDE-PPP binary is not found.
        FileNotFoundError: If the source RINEX file is not found.
        UserWarning: If no kin file is generated from the RINEX file.

    Examples:
        >>> source = AssetEntry(local_path="/path/to/NCB12450.24o", type=AssetType.RINEX, network="NCB", station="NCB1", campaign="JULY2024")
        >>> writedir = Path("/writedir")
        >>> pridedir = Path("/pridedir")
        >>> pride_ouput: List[AssetEntry] = rinex_to_kin(source, writedir, pridedir, site="NCB1", show_details=True)
        >>> kin_asset = pride_ouput[0]
        >>> kin_asset.model_dump()
        {'local_path': '/writedir/NCB12450.24o.kin', 'type': 'kin', 'network': 'NCB', 'station': 'NCB1', 'campaign': 'JULY2024', 'timestamp_created': datetime.datetime(2024, 7, 9, 12, 0, 0, 0)}
        >>> res_asset = pride_ouput[1]
        >>> res_asset.model_dump()
        {'local_path': '/writedir/NCB12450.24o.res', 'type': 'kinresiduals', 'network': 'NCB', 'station': 'NCB1', 'campaign': 'JULY2024', 'timestamp_created': datetime.datetime(2024, 7, 9, 12, 0, 0, 0)}
    """

    # Check if the pride binary is in the path
    if not shutil.which("pdp3"):
        raise FileNotFoundError("PRIDE-PPP binary 'pdp3' not found in path")

    if isinstance(source,str) or isinstance(source,Path):
        source = AssetEntry(local_path=source,type=AssetType.RINEX)
    assert source.type == AssetType.RINEX, "Invalid source file type"

    logger.loginfo(f"Converting RINEX file {source.local_path} to kin file")

    if not source.local_path.exists():
        logger.logerr(f"RINEX file {source.local_path} not found")
        raise FileNotFoundError(f"RINEX file {source.local_path} not found")

    source = rinex_get_meta(source)
    # get_nav_file(rinex_path=source.local_path)
    # get_gnss_products(rinex_path=source.local_path,pride_dir=pridedir)
    if source.station is not None:
        site = source.station

    # If PridePdpConfig is not provided, use the default configuration
    if pride_config is None:
        pride_config = PridePdpConfig()

    pdp_command = pride_config.generate_pdp_command(site=site,
                                                    local_file_path=source.local_path,
                                                    start=source.timestamp_data_start,
                                                    end=source.timestamp_data_end)
    
    file_name = source.local_path.name
    
    logger.loginfo(f"Running pdp3 with command: {' '.join(pdp_command)}")
    # Run pdp3 in the pride directory
    process = subprocess.Popen(
        " ".join(pdp_command),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(pridedir),
        text=True
    )

    stdout, stderr = process.communicate()

    # Log stdout lines
    for line in stdout.splitlines():
        line = remove_ansi_escape(line.strip())
        # Add file name to the log line
        line = f"{file_name}: {line}"
        if "ERROR" in line or "error" in line or "line" in line:
            logger.logerr(line)
        elif "WARNING" in line or "warning" in line:
            logger.logwarn(line)
        else:
            logger.logdebug(line)

    # Log stderr lines
    for line in stderr.splitlines():
        line = remove_ansi_escape(line.strip())
        # Add file name to the log line
        line = f"{file_name}: {line}"
        logger.logerr(line)

    year, doy = (
        source.timestamp_data_start.year,
        source.timestamp_data_start.timetuple().tm_yday,
    )
    file_dir = Path(pridedir) / str(year) / str(doy)

    kin_file_path = file_dir / f"kin_{str(year)}{str(doy)}_{site.lower()}"
    res_file_path = file_dir / f"res_{str(year)}{str(doy)}_{site.lower()}"
    kin_file = None
    res_file = None

    if kin_file_path.exists():
        kin_file_new = writedir / (kin_file_path.name + ".kin")
        shutil.move(src=kin_file_path,dst=kin_file_new)
        kin_file = AssetEntry(
            type=AssetType.KIN,
            parent_id=source.id,
            timestamp_data_start=source.timestamp_data_start,
            timestamp_data_end=source.timestamp_data_end,
            timestamp_created=datetime.now(),
            local_path=kin_file_new,
            network=source.network,
            station=source.station,
            campaign=source.campaign,
        )
        logger.loginfo(f"Generated kin file {kin_file.local_path} from RINEX file {source.local_path}")

    if res_file_path.exists():
        res_file_new = writedir / (res_file_path.name + ".res")
        shutil.move(src=res_file_path,dst=res_file_new)
        res_file = AssetEntry(
            type=AssetType.KINRESIDUALS,
            parent_id=source.id,
            timestamp_data_start=source.timestamp_data_start,
            timestamp_data_end=source.timestamp_data_end,
            timestamp_created=datetime.now(),
            local_path=res_file_new,
            network=source.network,
            station=source.station,
            campaign=source.campaign,
        )
        logger.loginfo(f"Found PRIDE res file {res_file.local_path}")

    if not kin_file:
        response = f"No kin file generated from RINEX {source.local_path}"
        logger.logerr(response)
        warn(response)
        return None, None

    return kin_file, res_file


@pa.check_types(lazy=True)
def kin_to_gnssdf(source:AssetEntry) -> Union[DataFrame[GNSSDataFrame], None]:
    """
    Create an GNSSDataFrame from a kin file from PRIDE-PPP

    Parameters:
        file_path (str): The path to the kin file

    Returns:
        dataframe (GNSSDataFrame): An instance of the class.
    """
    assert source.type == AssetType.KIN, "Invalid source file type"

    with open(source.local_path, "r") as file:
        lines = file.readlines()

    end_header_index = next(
        (i for i, line in enumerate(lines) if line.strip() == "END OF HEADER"), None
    )

    # Read data from lines after the end of the header
    data = []
    if end_header_index is None:
        logger.logerr(f"GNSS: No header found in FILE {source.local_path}")
        return None
    for idx, line in enumerate(lines[end_header_index + 2 :]):
        split_line = line.strip().split()
        selected_columns = split_line[:9] + [
            split_line[-1]
        ]  # Ignore varying satellite numbers
        try:
            ppp: Union[PridePPP, ValidationError] = PridePPP.from_kin_file(
                selected_columns
            )
            data.append(ppp)
        except:
            pass

    # Check if data is empty
    if not data:
        logger.logerr(f"GNSS: No data found in FILE {source.local_path}")
        return None
    
    # TODO convert lat/long to ecef
    dataframe = pd.DataFrame([dict(pride_ppp) for pride_ppp in data])
    # dataframe.drop(columns=["modified_julian_date", "second_of_day"], inplace=True)

    logger.loginfo(f"GNSS Parser: {dataframe.shape[0]} shots from FILE {source.local_path}")
    dataframe["time"] = dataframe["time"].dt.tz_localize("UTC")
    return dataframe.drop(columns=["modified_julian_date", "second_of_day"])


def qcpin_to_novatelpin(source: AssetEntry, writedir: Path) -> AssetEntry:
    with open(source.local_path) as file:
        pin_data = json.load(file)

    range_headers = []
    time_stamps = []

    for data in pin_data.values():
        range_header = data.get("observations").get("NOV_RANGE")
        time_header = data.get("observations").get("NOV_INS").get("time").get("common")
        range_headers.append(range_header)
        time_stamps.append(time_header)

    time_sorted = np.argsort(time_stamps)
    timestamp_data_start = time_stamps[time_sorted[0]]
    timestamp_data_end = time_stamps[time_sorted[-1]]
    range_headers = [range_headers[i] for i in time_sorted]

    file_path = writedir / (str(source.id) + "_novpin.txt")
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as temp_file:
        for header in range_headers:
            temp_file.write(header)
            temp_file.write("\n")
        temp_file.seek(0)
        shutil.copy(temp_file.name, file_path)
        novatel_pin = AssetEntry(
            parent_id=source.id,
            local_path=file_path,
            type=AssetType.NOVATELPIN,
            timestamp_data_start=timestamp_data_start,
            timestamp_data_end=timestamp_data_end,
            timestamp_created=datetime.now(),
        )

    return novatel_pin


def read_kin_data(kin_path):
    with open(kin_path, "r") as kin_file:
        for i, line in enumerate(kin_file):
            if "END OF HEADER" in line:
                end_of_header = i + 1
                break
    cols = [
        "Mjd",
        "Sod",
        "*",
        "X",
        "Y",
        "Z",
        "Latitude",
        "Longitude",
        "Height",
        "Nsat",
        "G",
        "R",
        "E",
        "C2",
        "C3",
        "J",
        "PDOP",
    ]
    colspecs = [
        (0, 6),
        (6, 16),
        (16, 18),
        (18, 32),
        (32, 46),
        (46, 60),
        (60, 77),
        (77, 94),
        (94, 108),
        (108, 114),
        (114, 117),
        (117, 120),
        (120, 123),
        (123, 126),
        (126, 129),
        (129, 132),
        (132, 140),
    ]
    kin_df = pd.read_fwf(
        kin_path,
        header=end_of_header,
        colspecs=colspecs,
        names=cols,
        on_bad_lines="skip",
    )
    # kin_df = pd.read_csv(kin_path, sep="\s+", names=cols, header=end_of_header, on_bad_lines='skip')
    kin_df.set_index(
        pd.to_datetime(kin_df["Mjd"] + 2400000.5, unit="D", origin="julian")
        + pd.to_timedelta(kin_df["Sod"], unit="sec"),
        inplace=True,
    )
    return kin_df


# read res and caculate wrms
def get_wrms_from_res(res_path):
    with open(res_path, "r") as res_file:
        timestamps = []
        data = []
        wrms = 0
        sumOfSquares = 0
        sumOfWeights = 0
        line = res_file.readline()  # first line is header and we can throw away
        while True:
            if line == "":  # break at EOF
                break
            line_data = line.split()
            if line_data[0] == "TIM":  # for a given epoch
                sumOfSquares = 0
                sumOfWeights = 0
                # parse date fields and make a timestamp
                seconds = float(line_data[6])
                SS = int(seconds)
                f = str(seconds - SS).split(".")[-1]
                isodate = f"{line_data[1]}-{line_data[2].zfill(2)}-{line_data[3].zfill(2)}T{line_data[4].zfill(2)}:{line_data[5].zfill(2)}:{str(SS).zfill(2)}.{str(f).zfill(6)}"
                timestamp = datetime.fromisoformat(isodate)
                timestamps.append(timestamp)
                # loop through SV data for that epoch, stop at next timestamp
                line = res_file.readline()
                line_data = line.split()
                while not line.startswith("TIM"):
                    phase_residual = float(line_data[1])
                    phase_weight = float(line_data[3].replace("D", "E"))
                    sumOfSquares += phase_residual**2 * phase_weight
                    sumOfWeights += phase_weight
                    line = res_file.readline()
                    if line == "":
                        break
                    line_data = line.split()
                wrms = (sumOfSquares / sumOfWeights) ** 0.5 * 1000  # in mm
                data.append(wrms)
            else:
                line = res_file.readline()
    wrms_df = pd.DataFrame({"date": timestamps, "wrms": data})
    return wrms_df


def plot_kin_results_wrms(kin_df, title=None, save_as=None):
    size = 3
    bad_nsat = kin_df[kin_df["Nsat"] <= 4]
    bad_pdop = kin_df[kin_df["PDOP"] >= 5]
    fig, axs = plt.subplots(
        6,
        1,
        figsize=(10, 10),
        sharex=True,
    )
    axs[0].scatter(kin_df.index, kin_df["Latitude"], s=size)
    axs[0].set_ylabel("Latitude")
    axs[1].scatter(kin_df.index, kin_df["Longitude"], s=size)
    axs[1].set_ylabel("Longitude")
    axs[2].scatter(kin_df.index, kin_df["Height"], s=size)
    axs[2].set_ylabel("Height")
    axs[3].scatter(kin_df.index, kin_df["Nsat"], s=size)
    axs[3].scatter(bad_nsat.index, bad_nsat["Nsat"], s=size * 2, color="red")
    axs[3].set_ylabel("Nsat")
    axs[4].scatter(kin_df.index, kin_df["PDOP"], s=size)
    axs[4].scatter(bad_pdop.index, bad_pdop["PDOP"], s=size * 2, color="red")
    axs[4].set_ylabel("log PDOP")
    axs[4].set_yscale("log")
    axs[4].set_ylim(1, 100)
    axs[5].scatter(kin_df.index, kin_df["wrms"], s=size)
    axs[5].set_ylabel("wrms mm")
    axs[0].ticklabel_format(axis="y", useOffset=False, style="plain")
    axs[1].ticklabel_format(axis="y", useOffset=False, style="plain")
    for ax in axs:
        ax.grid(True, c="lightgrey", zorder=0, lw=1, ls=":")
    plt.xticks(rotation=70)
    fig.suptitle(f"PRIDE-PPPAR results for {os.path.basename(title)}")
    fig.tight_layout()
    if save_as is not None:
        plt.savefig(save_as)


def nov0002tile(files:List[AssetEntry],rangea_tdb:Path,n_procs:int=10) -> None:
    """Given a list of NOV000.bin files, get all the rangea logs and add them to a single tdb array

    Args:
        files (List[AssetEntry]):  List of asset entries to process
        rangea_tdb (Path): Path to the rangea tiledb array
        n_procs (int, optional): _description_. Defaults to 10.
    """

    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    binary_path = NOV0002TILE_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"NOV0002TILE binary not found for {system} {arch}")

    cmd = [str(binary_path), "-tdb", str(rangea_tdb),"-procs",str(n_procs)]
    for file in files:
        cmd.append(str(file.local_path))

    logger.loginfo(f"Running NOV0002TILE on {len(files)} files")
    logger.logdebug(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd)

    if result.stdout:
        logger.logdebug(result.stdout.decode("utf-8"))

    if result.stderr:
        logger.logdebug(result.stderr.decode("utf-8"))
        result_message = result.stderr.decode("utf-8").split("msg=")
        for log_line in result_message:
            message = log_line.split("\n")[0]
            if "Processing" in message or "Created" in message:
                logger.loginfo(message)


def nova2tile(files:List[AssetEntry],rangea_tdb:Path,n_procs:int=10) -> None:
    """Given a list of novatel ascii files, get all the rangea logs and add them to a single tdb array

    Args:
        files (List[AssetEntry]):  List of asset entries to process
        rangea_tdb (Path): Path to the rangea tiledb array
        n_procs (int, optional): _description_. Defaults to 10.
    """

    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    binary_path = NOVA2TILE_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"NOVA2TILE binary not found for {system} {arch}")

    cmd = [str(binary_path), "-tdb", str(rangea_tdb),"-procs",str(n_procs)]
    for file in files:
        cmd.append(str(file.local_path))

    logger.loginfo(f"Running NOVA2TILE on {len(files)} files")
    logger.loginfo(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd)

    if result.stdout:
        logger.logdebug(result.stdout.decode("utf-8"))

    if result.stderr:
        logger.logdebug(result.stderr.decode("utf-8"))
        result_message = result.stderr.decode("utf-8").split("msg=")
        for log_line in result_message:
            message = log_line.split("\n")[0]
            if "Processing" in message or "Created" in message:
                logger.loginfo(message)

def novb2tile(files:List[AssetEntry],rangea_tdb:Path,n_procs:int=10) -> None:
    """Given a list of novatel binary files, get all the rangea logs and add them to a single tdb array

    Args:
        files (List[AssetEntry]):  List of asset entries to process
        rangea_tdb (Path): Path to the rangea tiledb array
        n_procs (int, optional): _description_. Defaults to 10.
    """

    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    binary_path = NOVB2TILE_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"NOVAB2TILE binary not found for {system} {arch}")

    cmd = [str(binary_path), "-tdb", str(rangea_tdb),"-procs",str(n_procs)]
    for file in files:
        cmd.append(str(file.local_path))

    logger.loginfo(f"Running NOVB2TILE on {len(files)} files")
    logger.loginfo(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd)

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

def tile2rinex(rangea_tdb:Path,settings:Path,writedir:Path,time_interval:int=1,processing_year:int=0) -> List[AssetEntry]:

    """
    Converts GNSS tile data to RINEX format using the TILE2RINEX binary.

    Args:
        rangea_tdb (Path): Path to the GNSS tiledb array.
        settings (Path): Path to the RINEX settings file.
        writedir (Path): Directory where the generated RINEX files will be written.
        time_interval (int, optional): Time interval (hours) of GNSS epochs loaded into memory from the tiledb array found at rangea_tdb.
        processing_year (int, optional): Year of GNSS observations used to generate RINEX files from the tiledb array found at rangea_tdb. Defaults to 0.

    Returns:
        List[AssetEntry]: A list of AssetEntry objects representing the generated RINEX files.

    Raises:
        ValueError: If the platform or architecture is unsupported.
        FileNotFoundError: If the TILE2RINEX binary is not found for the current platform and architecture.
        subprocess.CalledProcessError: If the TILE2RINEX command fails during execution.

    Notes:
        - The function uses a temporary directory to ensure only newly created RINEX files are returned.
        - Logs are captured from the TILE2RINEX binary's stdout and stderr for debugging and informational purposes.
        - The generated RINEX files are moved to the specified `writedir` and metadata is extracted for each file.
        - time_interval is used to control the tradeoff between memory usage and speed. The larger the time_interval, the more memory is used, but the faster the generation.
        - processing_year is used to prevent RINEX generation from years outside of a given campaign. When set to 0, all found observations are used to generate daily RINEX files.
    """

    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    binary_path = TILE2RINEX_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"TILE2RINEX binary not found for {system} {arch}")


    os.environ["LD_LIBRARY_PATH"] = os.environ["CONDA_PREFIX"] + "/lib"
    os.environ["DYLD_LIBRARY_PATH"] = os.environ["CONDA_PREFIX"] + "/lib"
    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        # Use a temp dir so as to only return newly created rinex files
        cmd = [
            str(binary_path),
            "-tdb",
            str(rangea_tdb),
            "-settings",
            str(settings),
            "-timeint",
            str(time_interval),
            "-year",
            str(processing_year),
        ]

        logger.loginfo(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd, cwd=workdir
        )

        if result.stdout:
            logger.logdebug(result.stdout.decode("utf-8"))
            result_message = result.stdout.decode("utf-8").split("msg=")
            for log_line in result_message:
                message = log_line.split("\n")[0]
                if "Generating" in message or "Found" in message:
                    logger.loginfo(message)

        if result.stderr:
            logger.logdebug(result.stderr.decode("utf-8"))
            result_message = result.stderr.decode("utf-8").split("msg=")
            for log_line in result_message:
                message = log_line.split("\n")[0]
                if "Generating" in message or "Found" in message:
                    logger.loginfo(message)

        rinex_files = list(Path(workdir).rglob("*"))
        rinex_assets = []
        for rinex_file_path in rinex_files:
            new_rinex_path = writedir / rinex_file_path.name
            shutil.move(src=rinex_file_path, dst=new_rinex_path)
            rinex_asset = AssetEntry(
                local_path=new_rinex_path,
                type=AssetType.RINEX,
                timestamp_created=datetime.now(),
            )
            rinex_asset = rinex_get_meta(rinex_asset)
            rinex_assets.append(rinex_asset)

    return rinex_assets
