from enum import Enum
import pandas as pd
from pydantic import BaseModel, Field, model_validator, ValidationError
import pandera as pa
from pandera.typing import DataFrame
from datetime import datetime
from typing import List, Optional, Union,Literal,Tuple
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

from ...data_mgmt.file_schemas import AssetEntry,AssetType
from ...data_models.observables import GNSSDataFrame

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


# def _novatel_to_rinex(
#     source_list: List[str],
#     writedir: Path,
#     site: str,
#     source_type: Literal[AssetType.NOVATEL,AssetType.NOVATEL770],
#     show_details: bool = False
# ) -> List[Path]:
#     """
#     Given a list of paths to NovAtel files, convert them to daily RINEX files.

#     Args:
#         source_list (List[str]): List of source file paths.
#         writedir (Path): Directory where the generated RINEX files will be written.
#         site (str): Site identifier.
#         source_type (Literal[AssetType.NOVATEL,AssetType.NOVATEL770]): Type of source files.
#         show_details (bool, optional): Flag to indicate whether to show conversion details. Defaults to False.

#     Returns:
#         List[Path]: List of paths to the generated RINEX files.

#     Examples:
#         >>> novatel_paths = ["/path/to/NCB1_09052024_NOV777.raw", "/path/to/NCB1_09062024_NOV777.raw"]
#         >>> writedir = Path("/writedir")
#         >>> site = "NCB1"
#         >>> source_type = AssetType.NOVATEL
#         >>> rinex_files: List[Path] = _novatel_to_rinex(novatel_paths, writedir, site, source_type)

#     """
#     # Sort sourcelist
#     def sort_key(path: Path):
#         name = path.name.replace('NOV', '').replace("_", "")
#         # replace all non numeric characters with empty string
#         name_non_numeric = re.sub(r"\D", "", name)
#         return int(name_non_numeric)

#     source_list = sorted(source_list, key=sort_key)

#     system = platform.system().lower()
#     arch = platform.machine().lower()
#     if arch == "x86_64":
#         arch = "amd64"
#     if system not in ["darwin", "linux"]:
#         raise ValueError(f"Unsupported platform: {system}")
#     if arch not in ["amd64", "arm64"]:
#         raise ValueError(f"Unsupported architecture: {arch}")

#     if source_type in [AssetType.NOVATEL,AssetType.NOVATELPIN]:
#         binary_path = RINEX_BIN_PATH[f"{system}_{arch}"]
#     else:
#         binary_path = RINEX_BIN_PATH_BINARY[f"{system}_{arch}"]

#     metadata = get_metadata(site, serialNumber=uuid.uuid4().hex[:10])

#     logger.loginfo(f"Converting and merging {len(source_list)} files of type {source_type.value} to RINEX")
#     with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
#         metadata_path = Path(workdir) / "metadata.json"
#         with open(metadata_path, "w") as f:
#             json_object = json.dumps(metadata, indent=4)
#             f.write(json_object)

#         cmd = [
#             str(binary_path),
#             "-meta",
#             str(metadata_path)
#         ] + [str(x) for x in source_list]

#         result = subprocess.run(cmd, check=True, capture_output=True, cwd=workdir)

#         if result.stdout:
#             logger.logdebug(result.stdout.decode("utf-8"))

#         if result.stderr:
#             logger.logdebug(result.stderr.decode("utf-8"))
#             result_message = result.stderr.decode("utf-8").split("msg=")
#             for log_line in result_message:
#                 message = log_line.split("\n")[0]
#                 if "Processing" in message or "Created" in message:
#                     logger.loginfo(message)

#         rnx_files = list(Path(workdir).rglob(f"*{site}*"))
#         logger.loginfo(
#             f"Converted {len(source_list)} files of type {source_type.value} to {len(rnx_files)} Daily RINEX files"
#         )
#         rinex_files = []
#         for rinex_file_path in rnx_files:
#             new_rinex_path = writedir / rinex_file_path.name
#             shutil.move(src=rinex_file_path, dst=new_rinex_path)
#             logger.loginfo(f"Generated Daily RINEX file {str(new_rinex_path)}")
#             rinex_files.append(new_rinex_path)
#     return rinex_files


# def novatel_to_rinex_batch(
#     source: List[AssetEntry],
#     writedir: Path|str = None,
#     show_details: bool = False
# ) -> List[AssetEntry]:

#     """
#     Given a set of AssetEntry objects representing AssetType.NOVATEL or AssetType.NOVATEL770 files, convert them to daily RINEX files representing
#     each distinct day of data.

#     Args:
#         source (List[AssetEntry]): List of AssetEntry objects representing the source files.
#         writedir (Path|str, optional): Directory where the RINEX files will be written. If not provided, the RINEX files will be written to the same directory as the source files. Defaults to None.
#         show_details (bool, optional): Flag indicating whether to show detailed conversion information. Defaults to False.
#     Returns:
#         List[AssetEntry]: List of AssetEntry objects representing the converted RINEX files.

#     Examples:
#         >>> asset_entry_0 = AssetEntry(local_path="/path/to/NCB1_09052024_NOV777.raw", type=AssetType.NOVATEL, network="NCB", station="NCB1", campaign="JULY2024")
#         >>> asset_entry_1 = AssetEntry(local_path="/path/to/NCB1_09062024_NOV777.raw", type=AssetType.NOVATEL, network="NCB", station="NCB1", campaign="JULY2024")
#         >>> writedir = Path("/writedir")
#         >>> rinex_assets: List[AssetEntry] = novatel_to_rinex([asset_entry_0, asset_entry_1], writedir, show_details=True)
#         >>> rinex_assets[0].model_dump()
#         {'local_path': '/writedir/NCB1_09052024_NOV777.raw', 'type': 'rinex', 'network': 'NCB', 'station': 'NCB1', 'campaign': 'JULY2024', 'timestamp_created': datetime.datetime(2024, 7, 9, 12, 0, 0, 0)}
#     """
#     assert len(set([x.type for x in source])) == 1, "All sources must be of the same type"

#     source_type = source[0].type
#     site = source[0].station
#     network = source[0].network
#     campaign = source[0].campaign
#     station = source[0].station

#     if isinstance(writedir, str):
#         writedir = Path(writedir)
#     elif writedir is None:
#         writedir = source[0].local_path.parent
#     if site is None:
#         site = "SIT1"
#     rinex_paths = _novatel_to_rinex(
#         source_list=[x.local_path for x in source],
#         writedir=writedir,
#         show_details=show_details,
#         site=site,
#         source_type=source_type
#     )
#     rinex_assets = []
#     for rinex_path in rinex_paths:
#         rinex_asset = AssetEntry(
#             local_path=rinex_path,
#             type=AssetType.RINEX,
#             network=network,
#             station=station,
#             campaign=campaign,
#             timestamp_created=datetime.now(),
#         )
#         rinex_asset = rinex_get_meta(rinex_asset)
#         rinex_assets.append(rinex_asset)

#     return rinex_assets

# def novatel_to_rinex(
#     source:str | List[str],
#     show_details: bool = False
# ) -> List[str]:
#     """
#     Given a path to a Novatel ascii or raw file, convert it to daily RINEX files representing each distinct day of data.
    
#     Parameters:
#         source (str): The path to the Novatel GNSS file.
#         show_details (bool, optional): Whether to show detailed information during the conversion process. Defaults to False.
#     Returns:
#         List[str]: A list of paths to the generated RINEX files.

#     Examples:
#         >>> source = "/path/to/NCB1_09052024_NOV777.raw"
#         >>> rinex_files: List[str] = novatel_to_rinex(source, show_details=True)
#         >>> rinex_files
#         ["/path/to/NCB12450.24o", "/path/to/NCB12460.24o"]
#     """
#     source = Path(source)
#     if "NOV770" in source.name:
#         source_type = AssetType.NOVATEL770
#     else:
#         source_type = AssetType.NOVATEL
#     site = "SIT1"

#     if isinstance(source, str):
#         source = [source]

#     writedir = source.parent
#     rinex_files = _novatel_to_rinex(
#         source_list=source,
#         writedir=writedir,
#         show_details=show_details,
#         site=site,
#         source_type=source_type
#     )
#     return rinex_files


# def qcpin_to_novatelpin(source: AssetEntry, writedir: Path) -> AssetEntry:
#     with open(source.local_path) as file:
#         pin_data = json.load(file)

#     range_headers = []
#     time_stamps = []

#     for data in pin_data.values():
#         range_header = data.get("observations").get("NOV_RANGE")
#         time_header = data.get("observations").get("NOV_INS").get("time").get("common")
#         range_headers.append(range_header)
#         time_stamps.append(time_header)

#     time_sorted = np.argsort(time_stamps)
#     timestamp_data_start = time_stamps[time_sorted[0]]
#     timestamp_data_end = time_stamps[time_sorted[-1]]
#     range_headers = [range_headers[i] for i in time_sorted]

#     file_path = writedir / (str(source.id) + "_novpin.txt")
#     with tempfile.NamedTemporaryFile(mode="w+", delete=True) as temp_file:
#         for header in range_headers:
#             temp_file.write(header)
#             temp_file.write("\n")
#         temp_file.seek(0)
#         shutil.copy(temp_file.name, file_path)
#         novatel_pin = AssetEntry(
#             parent_id=source.id,
#             local_path=file_path,
#             type=AssetType.NOVATELPIN,
#             timestamp_data_start=timestamp_data_start,
#             timestamp_data_end=timestamp_data_end,
#             timestamp_created=datetime.now(),
#         )

    return novatel_pin








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


# def nova2tile(files:List[AssetEntry],rangea_tdb:Path,n_procs:int=10) -> None:
#     """Given a list of novatel ascii files, get all the rangea logs and add them to a single tdb array

#     Args:
#         files (List[AssetEntry]):  List of asset entries to process
#         rangea_tdb (Path): Path to the rangea tiledb array
#         n_procs (int, optional): _description_. Defaults to 10.
#     """

#     system = platform.system().lower()
#     arch = platform.machine().lower()
#     if arch == "x86_64":
#         arch = "amd64"
#     if system not in ["darwin", "linux"]:
#         raise ValueError(f"Unsupported platform: {system}")
#     if arch not in ["amd64", "arm64"]:
#         raise ValueError(f"Unsupported architecture: {arch}")

#     binary_path = NOVA2TILE_BIN_PATH.get(f"{system}_{arch}")
#     if not binary_path:
#         raise FileNotFoundError(f"NOVA2TILE binary not found for {system} {arch}")

#     cmd = [str(binary_path), "-tdb", str(rangea_tdb),"-procs",str(n_procs)]
#     for file in files:
#         cmd.append(str(file.local_path))

#     logger.loginfo(f"Running NOVA2TILE on {len(files)} files")
#     result = subprocess.run(cmd)

#     if result.stdout:
#         logger.logdebug(result.stdout.decode("utf-8"))

#     if result.stderr:
#         logger.logdebug(result.stderr.decode("utf-8"))
#         result_message = result.stderr.decode("utf-8").split("msg=")
#         for log_line in result_message:
#             message = log_line.split("\n")[0]
#             if "Processing" in message or "Created" in message:
#                 logger.loginfo(message)

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
    logger.logdebug(f"Running {cmd}")
    for file in files:
        cmd.append(str(file.local_path))
    logger.loginfo(f"Running NOVB2TILE on {len(files)} files")

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
