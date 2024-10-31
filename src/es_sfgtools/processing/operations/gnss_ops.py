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
from ..assets.file_schemas import AssetEntry,AssetType,MultiAssetEntry,MultiAssetPre
from ..assets.observables import GNSSDataFrame
from .pride_utils import get_nav_file,get_gnss_products
logger = logging.getLogger(__name__)

RINEX_BINARIES = Path(__file__).resolve().parent / "binaries/"

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
    "darwin_amd64": RINEX_BINARIES / "nova2rnxo-darwin-amd64",
    "darwin_arm64": RINEX_BINARIES / "nova2rnxo-darwin-arm64",
    "linux_amd64": RINEX_BINARIES / "nova2rnxo-linux-amd64",
    "linux_arm64": RINEX_BINARIES / "nova2rnxo-linux-arm64",
}


RINEX_BIN_PATH_BINARY = {
    "darwin_amd64": RINEX_BINARIES / "novb2rnxo-darwin-amd64",
    "darwin_arm64": RINEX_BINARIES / "novb2rnxo-darwin-arm64",
    "linux_amd64": RINEX_BINARIES / "novb2rnxo-linux-amd64",
    "linux_arm64": RINEX_BINARIES / "novb2rnxo-linux-arm64",
}

TEQC_BIN_PATH = {
    "darwin_amd64": RINEX_BINARIES / "teqc_OSX",
    "darwin_arm64": RINEX_BINARIES / "teqc_OSX",
    "linux_amd64": RINEX_BINARIES / "teqc_x86",
    "linux_arm64": RINEX_BINARIES / "teqc_x86",
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


def rinex_get_meta(source:AssetEntry | MultiAssetEntry) ->AssetEntry|MultiAssetEntry:
    assert source.type == AssetType.RINEX, f"Expected RINEX file, got {source.type}"
    with open(source.local_path) as f:
        files = f.readlines()
        for line in files:
            if "TIME OF FIRST OBS" in line:
                start_time = _rinex_get_time(line)
                file_date = start_time.strftime("%Y%m%d%H%M")
                source.timestamp_data_start = start_time
                break


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
    if show_details:
        print(f"Converting and merging {len(source_list)} files of type {source_type.value} to RINEX")

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        metadata_path = Path(workdir) / "metadata.json"
        with open(metadata_path, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)

        cmd = [
            str(binary_path),
            "-meta",
            str(metadata_path)
        ] + source_list

        result = subprocess.run(cmd, check=True, capture_output=True, cwd=workdir)
        if result.stderr:
            logger.error(result.stderr)
            if show_details:
                print(result.stderr)
        rnx_files = list(Path(workdir).rglob(f"*{site}*"))
        response = f"Converted {len(source_list)} files of type {source_type.value} to {len(rnx_files)} Daily RINEX files"
        logger.info(response)
        if show_details: print(response)
        rinex_files = []
        for rinex_file_path in rnx_files:
            new_rinex_path = writedir / rinex_file_path.name
            shutil.move(src=rinex_file_path, dst=new_rinex_path)
            if show_details:
                print(f"Generated Daily RINEX file {str(new_rinex_path)}")
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
        >>> asset_entry_0 = AssetEntry(local_path="/path/to/NCB1_09052024_NOV777.raw", type=AssetType.NOVATEL, network="NCB", station="NCB1", survey="JULY2024")
        >>> asset_entry_1 = AssetEntry(local_path="/path/to/NCB1_09062024_NOV777.raw", type=AssetType.NOVATEL, network="NCB", station="NCB1", survey="JULY2024")
        >>> writedir = Path("/writedir")
        >>> rinex_assets: List[AssetEntry] = novatel_to_rinex([asset_entry_0, asset_entry_1], writedir, show_details=True)
        >>> rinex_assets[0].model_dump()
        {'local_path': '/writedir/NCB1_09052024_NOV777.raw', 'type': 'rinex', 'network': 'NCB', 'station': 'NCB1', 'survey': 'JULY2024', 'timestamp_created': datetime.datetime(2024, 7, 9, 12, 0, 0, 0)}
    """
    assert len(set([x.type for x in source])) == 1, "All sources must be of the same type"

    source_type = source[0].type
    site = source[0].station
    network = source[0].network
    survey = source[0].survey
    station = source[0].station

    if isinstance(writedir, str):
        writedir = Path(writedir)
    elif writedir is None:
        writedir = source[0].local_path.parent

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
            survey=survey,
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


# TODO: handle logging with multiprocessing https://signoz.io/guides/how-should-i-log-while-using-multiprocessing-in-python/
def rinex_to_kin(
    source: Union[AssetEntry,str,Path],
    writedir: Path,
    pridedir: Path,
    site="SIT1",
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
        >>> source = AssetEntry(local_path="/path/to/NCB12450.24o", type=AssetType.RINEX, network="NCB", station="NCB1", survey="JULY2024")
        >>> writedir = Path("/writedir")
        >>> pridedir = Path("/pridedir")
        >>> pride_ouput: List[AssetEntry] = rinex_to_kin(source, writedir, pridedir, site="NCB1", show_details=True)
        >>> kin_asset = pride_ouput[0]
        >>> kin_asset.model_dump()
        {'local_path': '/writedir/NCB12450.24o.kin', 'type': 'kin', 'network': 'NCB', 'station': 'NCB1', 'survey': 'JULY2024', 'timestamp_created': datetime.datetime(2024, 7, 9, 12, 0, 0, 0)}
        >>> res_asset = pride_ouput[1]
        >>> res_asset.model_dump()
        {'local_path': '/writedir/NCB12450.24o.res', 'type': 'kinresiduals', 'network': 'NCB', 'station': 'NCB1', 'survey': 'JULY2024', 'timestamp_created': datetime.datetime(2024, 7, 9, 12, 0, 0, 0)}
    """

    # Check if the pride binary is in the path
    if not shutil.which("pdp3"):
        raise FileNotFoundError("PRIDE-PPP binary 'pdp3' not found in path")

    if isinstance(source,str) or isinstance(source,Path):
        source = AssetEntry(local_path=source,type=AssetType.RINEX)
    assert source.type == AssetType.RINEX, "Invalid source file type"

    logger.info(f"Converting RINEX file {source.local_path} to kin file")

    if not source.local_path.exists():
        logger.error(f"RINEX file {source.local_path} not found")
        raise FileNotFoundError(f"RINEX file {source.local_path} not found")

    get_nav_file(rinex_path=source.local_path)
    get_gnss_products(rinex_path=source.local_path,pride_dir=pridedir)
    if source.station is not None:
        site = source.station
    cmd = [
        "pdp3", 
        "--loose-edit", 
        "-m", 
        "K",
        "-i",
        "1", # 1hz decimation
        "-c",
        "15",
        "--site",
        site, 
        str(source.local_path)]

    # Run pdp3 in the pride directory
    result = subprocess.run(
        " ".join(cmd),
        shell=True,
        capture_output=True,
        cwd=str(pridedir),
    )
    pattern_error = r":\d+,\d+, merror:0m"
    pattern_warning = r":\d+,\d+, mwarning:0m"
    if result.stderr:
        stderr = result.stderr.decode("utf-8").split("\n")
        for line in stderr:
            if "warning" in line.lower():
                logger.warning(line)
            if "error" in line.lower():
                logger.error(line)
    stdout = result.stdout.decode("utf-8")
    stdout = re.sub(pattern_error, "ERROR ", stdout)
    stdout = re.sub(pattern_warning, "WARNING ", stdout)
    stdout = stdout.replace("\x1b[", "")
    stdout = stdout.split("\n")
    for line in stdout:
        if "failed" in line.lower():
            logger.error(line)
        if "please" in line.lower():
            logger.error(line)
        if "warning" in line.lower():
            logger.warning(line)
        if "error" in line.lower():
            logger.error(line)

    found_files = Path(pridedir).rglob(f"*{site.lower()}*")
    kin_file = None
    res_file = None
    for found_file in found_files:

        if "kin" in found_file.name:
            kin_file = found_file
            kin_file_new = writedir / (kin_file.name + ".kin")
            shutil.move(src=kin_file,dst=kin_file_new)
            kin_file = AssetEntry(
                type=AssetType.KIN,
                parent_id=source.id,
                timestamp_data_start=source.timestamp_data_start,
                timestamp_data_end=source.timestamp_data_end,
                timestamp_created=datetime.now(),
                local_path=kin_file_new,
                network=source.network,
                station=source.station,
                survey=source.survey,
            )
            response = f"Converted RINEX file {source.local_path} to kin file {kin_file.local_path}"
            logger.info(response)
            if show_details:
                print(response)

        if "res" in found_file.name:
            res_file = found_file
            res_file_new = writedir / (res_file.name + ".res")
            shutil.move(src=res_file,dst=res_file_new)
            res_file = AssetEntry(
                type=AssetType.KINRESIDUALS,
                parent_id=source.id,
                timestamp_data_start=source.timestamp_data_start,
                timestamp_data_end=source.timestamp_data_end,
                timestamp_created=datetime.now(),
                local_path=res_file_new,
                network=source.network,
                station=source.station,
                survey=source.survey,
            )
            response = f"Found PRIDE res file {res_file.local_path}"
            logger.info(response)
            if show_details:
                print(response)

    if not kin_file:
        response = f"No kin file generated from RINEX {source.local_path}"
        logger.error(response)
        warn(response)
        return None,None
    
    return kin_file,res_file


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
        error_msg = f"GNSS: No header found in FILE {source.local_path}"
        logger.error(error_msg)
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
        error_msg = f"GNSS: No data found in FILE {source.local_path}"
        logger.error(error_msg)
        return None
    
    # TODO convert lat/long to ecef
    dataframe = pd.DataFrame([dict(pride_ppp) for pride_ppp in data])
    # dataframe.drop(columns=["modified_julian_date", "second_of_day"], inplace=True)

    log_response = (
        f"GNSS Parser: {dataframe.shape[0]} shots from FILE {source.local_path}"
    )
    logger.info(log_response)
    dataframe["time"] = dataframe["time"].dt.tz_localize("UTC")
    return dataframe


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


def dev_merge_rinex(sources: List[AssetEntry],working_dir:Path) -> List[MultiAssetEntry]:
    """
    Merge multiple RINEX files into a single RINEX file

    Parameters:
        sources (List[AssetEntry]): A list of AssetEntry instances
        working_dir (Path): The working directory

    Returns:
        merged_files (List[MultiAssetEntry]): A list of MultiAssetEntry instances containing the merged RINEX files
    
    Raises:
        ValueError: If no merged files are found/created or if the binary is not found
    """

    # get system platform and architecture
    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    binary_path = TEQC_BIN_PATH[f"{system}_{arch}"]
    assert os.path.exists(binary_path), f"Binary not found: {binary_path}"

    # Gen rinex metadata
    #sources = [rinex_get_meta(source) for source in sources if source.timestamp_data_start is None else source]
    sources = sorted(sources, key=lambda x: x.timestamp_data_start)
    doy_filemap = {}
    for source in sources:
        doy_filemap.setdefault(source.timestamp_data_start.timetuple().tm_yday, []).append(
            str(source.id) if source.id is not None else str(source.parent_id)
        )
    survey = sources[0].station

    cmd = [
        str(binary_path),
        "+obs",
        "+",
        "-tbin",
        "1d", # Time binning interval
        survey,
    ] + [str(source.local_path) for source in sources]


    result = subprocess.run(" ".join(cmd), shell=True,cwd=str(working_dir))
    if result.stderr:
        logger.error(result.stderr)
        return None
    # get all merged rinex files
    # merged_files = list(Path(tempdir).rglob(f"{survey}*"))
    merged_files = []
    for doy,source_id_str in doy_filemap.items():
        merged_file = list(Path(working_dir).rglob(f"{survey}{doy:03d}0*"))
        if not merged_file:
            continue
        assert len(merged_file) == 1, f"Expected 1 merged file, got {len(merged_file)}"
        merged_file:Path = merged_file[0]
        new_merged_rinex_name = "-".join(source_id_str) + "_" + merged_file.name
        merged_file = merged_file.rename(working_dir / new_merged_rinex_name)
        merged_asset = MultiAssetEntry(
            parent_id=source_id_str,
            local_path=merged_file,
            type=AssetType.RINEX,
            network=sources[0].network,
            station=sources[0].station,
            survey=sources[0].survey,
            timestamp_created=datetime.now(),
        )
        merged_files.append(merged_asset)
    if not merged_files:
        raise ValueError("No merged files found")
    return merged_files

def dev_merge_rinex_multiasset(source:MultiAssetPre,working_dir:Path) -> MultiAssetEntry:

    # get system platform and architecture
    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    binary_path = TEQC_BIN_PATH[f"{system}_{arch}"]
    assert os.path.exists(binary_path), f"Binary not found: {binary_path}"

    doy = source.timestamp_data_start.timetuple().tm_yday
    survey = source.station
    cmd = [
        str(binary_path),
        "+obs",
        "+",
        "-tbin",
        "1d", # Time binning interval
        survey,
    ] + [str(x) for x in source.source_paths]
    result = subprocess.run(" ".join(cmd), shell=True, cwd=str(working_dir))
    if result.stderr:
        logger.error(result.stderr)
        return None

    merged_file = list(Path(working_dir).rglob(f"*{survey}{doy}0*"))
    if not merged_file:
        raise ValueError("No merged files found")

    merged_file = merged_file[0]
    parent_id_str = "-".join([str(x) for x in source.parent_id])
    new_merged_rinex_name = parent_id_str + "_" + merged_file.name
    merged_file = merged_file.rename(working_dir / new_merged_rinex_name)
    merged_asset = MultiAssetEntry(
        parent_id=source.parent_id,
        local_path=merged_file,
        type=AssetType.RINEX,
        network=source.network,
        station=source.station,
        survey=source.survey,
        timestamp_created=datetime.now(),
    )
    return rinex_get_meta(merged_asset)


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
    wrms_df = pd.DataFrame({"date": timestamps, "wrms": data}).set_index("date")
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
    plt.close()
