import pandas as pd
from pydantic import BaseModel, Field, model_validator, ValidationError
import pandera as pa
from pandera.typing import DataFrame
from datetime import datetime
from typing import List, Optional, Union,Annotated
import logging
import julian
import os
import tempfile
import subprocess
from concurrent.futures import ProcessPoolExecutor as Pool
from functools import partial
import sys
import shutil
import json
import platform
from pathlib import Path
import numpy as np
import uuid

from ..assets.file_schemas import AssetEntry,AssetType,MultiAssetEntry
from ..assets.observables import GNSSDataFrame

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


def rinex_get_meta(source:AssetEntry) ->AssetEntry:
    assert source.type == AssetType.RINEX, f"Expected RINEX file, got {source.type}"
    with open(source.local_path) as f:
        files = f.readlines()
        for line in files:
            if "TIME OF FIRST OBS" in line:
                start_time = _rinex_get_time(line)
                file_date = start_time.strftime("%Y%m%d%H%M")
                source.timestamp_data_start = start_time

            if "TIME OF LAST OBS" in line:
                end_time = _rinex_get_time(line)
                source.timestamp_data_end = end_time
                break
    return source


def novatel_to_rinex(
    source: Union[AssetEntry,str,Path],
    site: str,
    year: str = None,
    writedir: Path = None,
    source_type:AssetType = AssetType.NOVATEL,
    show_details: bool = False,

    **kwargs,
) -> AssetEntry:
    """
    Batch convert Novatel files to RINEX
    """
    if isinstance(source,str) or isinstance(source,Path):
        try:
            source_type = AssetType(source_type)
        except:
            raise ValueError("Argument source_type must be a valid AssetType ['novatel','novatel770','novatelpin']")

        source = AssetEntry(local_path=source,source_type=source_type)

    assert source.local_path.exists(), f"File not found: {source.local_path}"

    if writedir is None:
        writedir = source.local_path.parent

    # get system platform and architecture
    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    if source.type in [AssetType.NOVATEL,AssetType.NOVATELPIN]:
        binary_path = RINEX_BIN_PATH[f"{system}_{arch}"]
    else:
        binary_path = RINEX_BIN_PATH_BINARY[f"{system}_{arch}"]

    assert os.path.exists(binary_path), f"Binary not found: {binary_path}"

    metadata = get_metadata(site, serialNumber=uuid.uuid4().hex[:10])

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        metadata_path = Path(workdir)/ "metadata.json"
        with open(metadata_path, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)

        if source.timestamp_data_start is not None:
            file_date = source.timestamp_data_start.strftime("%Y%m%d%H%M")
        else:
            file_date = datetime.now().strftime("%Y%m%d%H%M")

        year_name = '23' if year is None else year

        rinex_outfile = Path(writedir)/f"{site}_{file_date}_rinex.{year_name}O"
        cmd = [
            str(binary_path),
            "-meta",
            str(metadata_path),
            "-out",
            str(rinex_outfile),
            str(source.local_path),
        ]

        result = subprocess.run(cmd, check=True, capture_output=True,cwd=workdir)
        if not rinex_outfile.exists():
            logger.error(result.stderr)
            return None
        rinex_asset = AssetEntry(
            parent_id=source.id,
            local_path=rinex_outfile,
            type=AssetType.RINEX,
            network=source.network,
            station=source.station,
            survey=source.survey,
            timestamp_created=datetime.now(),

        )

        rinex_asset = rinex_get_meta(rinex_asset)
        if year is None and rinex_asset.timestamp_data_start is not None:
            year_name = str(rinex_asset.timestamp_data_start.year)[-2:]
            file_date = rinex_asset.timestamp_data_start.strftime("%Y%m%d")
            start_time = rinex_asset.timestamp_data_start.strftime("%H%M%S")
            new_rinex_path = rinex_asset.local_path.parent / f"{site}_{file_date}_{start_time}rinex.{year_name}O"
            rinex_asset.local_path = rinex_asset.local_path.rename(new_rinex_path)

        if show_details:
            # logger.info("showing details")
            if len(result.stdout.decode()):
                print(
                    f"{source.local_path.name}: {result.stdout.decode().rstrip()}"
                )
            # print(result.stderr.decode())

    return rinex_asset


def rinex_to_kin(
    source: Union[AssetEntry,str,Path],
    writedir: Path,
    pridedir: Path,
    site="IVB1",
    show_details: bool = True,
) -> AssetEntry:
    """
    Convert a RINEX file to a position file
    """
    if isinstance(source,str) or isinstance(source,Path):
        source = AssetEntry(local_path=source,type=AssetType.RINEX)
    assert source.type == AssetType.RINEX, "Invalid source file type"

    logger.info(f"Converting RINEX file {source.local_path} to kin file")

    out = []

    # simul link the rinex file to the same file with file_uuid attached at the front

    if not source.local_path.exists():
        logger.error(f"RINEX file {source.local_path} not found")
        raise FileNotFoundError(f"RINEX file {source.local_path} not found")
    # tag = uuid.uuid4().hex[:4]
    # FROM JOHN "pdp3 -m K -i 1 -l -c15 rinex"
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

    result = subprocess.run(
        " ".join(cmd),
        shell=True,
        capture_output=True,
        cwd=str(pridedir),
    )

    if result.stderr:
        logger.error(result.stderr)
    if pd.isna(source.timestamp_data_start):
        try:
            ts = str(source.local_path.name).split("_")[1]
            year = ts[:4]
            ts = ts[4:]
            month = ts[:2]
            ts = ts[2:]
            day = max(1, int(ts[:2]))
            ts = ts[2:]
            hour = ts[-4:-2]
            minute = ts[-2:]
            source.timestamp_data_start = datetime(
                year=int(year), month=int(month), day=int(day), hour=int(hour)
            )
        except:
            logger.error(f"Error parsing timestamp from RINEX file {source.local_path}")
            pass

    #file_pattern = f"{source.timestamp_data_start.year}{source.timestamp_data_start.timetuple().tm_yday}"
    tag_files = pridedir.rglob(f"*{site.lower()}*")
    for tag_file in tag_files:
        # print("tag file:", tag_file)
        if "kin" in tag_file.name:
            kin_file = tag_file
            kin_file_new = writedir / (kin_file.name + ".kin")
            shutil.move(kin_file, kin_file_new)
            kin_file = AssetEntry(
                type=AssetType.KIN,
                parent_id=source.id,
                start_time=source.timestamp_data_start,
                local_path=kin_file_new,
            )
            response = f"Converted RINEX file {source.local_path} to kin file {kin_file.local_path}"
            logger.info(response)
            if show_details:
                print(response)
            break
        tag_file.unlink()

    try:
        return kin_file
    except:
        return None


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
            error_msg = f"Error parsing into PridePPP from line {idx} in FILE {source.local_path} \n"
            error_msg += f"Line: {line}"
            logger.error(error_msg)
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
            str(source.id)
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
        merged_file = list(Path(working_dir).rglob(f"{survey}{doy:03d}*"))
        if not merged_file:
            continue
        assert len(merged_file) == 1, f"Expected 1 merged file, got {len(merged_file)}"
        merged_asset = MultiAssetEntry(
            parent_id=source_id_str,
            local_path=merged_file[0],
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
