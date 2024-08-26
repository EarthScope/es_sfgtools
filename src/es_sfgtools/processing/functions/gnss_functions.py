import pandas as pd
from pydantic import BaseModel, Field, model_validator, ValidationError
import pandera as pa
from pandera.typing import DataFrame
from datetime import datetime
from typing import List, Optional, Union
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

from ..schemas.files.file_schemas import NovatelFile,RinexFile,KinFile,Novatel770File,DFPO00RawFile,QCPinFile,NovatelPinFile
from ..schemas.observables import PositionDataFrame

# logger = logging.getLogger(os.path.basename(__file__))
logger = logging.getLogger(__name__)

NOVATEL2RINEX_BINARIES = Path(__file__).resolve().parent / "binaries/"

PRIDE_PPP_LOG_INDEX = {
    0: "modified_julian_date",
    1: "second_of_day",
    2: "x",
    3: "y",
    4: "z",
    5: "latitude",
    6: "longitude",
    7: "height",
    8: "number_of_satellites",  # 8 is minimum number of satellites for accurate positioning
    9: "pdop",
}

RINEX_BIN_PATH = {
    "darwin_amd64": NOVATEL2RINEX_BINARIES/ "nova2rnxo-darwin-amd64",
    "darwin_arm64": NOVATEL2RINEX_BINARIES/ "nova2rnxo-darwin-arm64",
    "linux_amd64": NOVATEL2RINEX_BINARIES / "nova2rnxo-linux-amd64",
    "linux_arm64": NOVATEL2RINEX_BINARIES/ "nova2rnxo-linux-arm64",
}


RINEX_BIN_PATH_BINARY = {
    "darwin_amd64": NOVATEL2RINEX_BINARIES/ "novb2rnxo-darwin-amd64",
    "darwin_arm64": NOVATEL2RINEX_BINARIES/ "novb2rnxo-darwin-arm64",
    "linux_amd64": NOVATEL2RINEX_BINARIES/ "novb2rnxo-linux-amd64",
    "linux_arm64": NOVATEL2RINEX_BINARIES/ "novb2rnxo-linux-arm64",
}


class PridePPP(BaseModel):
    """
    Data class for PPP GNSS kinematic position output
    Docs: https://github.com/PrideLab/PRIDE-PPPAR
    """

    modified_julian_date: float = Field(ge=0)
    second_of_day: float = Field(ge=0, le=86400)
    x: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF X coordinate
    y: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF Y coordinate
    z: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF Z coordinate
    latitude: float = Field(ge=-90, le=90)  # WGS84 latitude
    longitude: float = Field(ge=0, le=360)  # WGS84 longitude
    height: float = Field(ge=-101, le=100)  # WGS84 height (m)
    number_of_satellites: int = Field(
        default=1, ge=0, le=125
    )  # Average Number of available satellites
    pdop: float = Field(default=0, ge=0, le=100)  # Position Dilution of Precision
    time: Optional[datetime] = None

    class Config:
        coerce = True

    @model_validator(mode="before")
    def validate_time(cls, values):
        values["pdop"] = float(values["pdop"])
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
        except ValidationError:
            raise Exception("Error parsing into PridePPP")


def get_metadata(site: str):
    #TODO: these are placeholder values, need to use real metadata
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


def _novatel_to_rinex(
    source:Union[NovatelFile,Novatel770File,NovatelPinFile],site: str, year: str = None,show_details: bool=False,**kwargs
) -> RinexFile:
    """
    Batch convert Novatel files to RINEX
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
    
    if type(source) in [NovatelFile,NovatelPinFile]:
        binary_path = RINEX_BIN_PATH[f"{system}_{arch}"]
    else:
        binary_path = RINEX_BIN_PATH_BINARY[f"{system}_{arch}"]

    assert os.path.exists(binary_path), f"Binary not found: {binary_path}"

    metadata = get_metadata(site)

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        metadata_path = os.path.join(workdir, "metadata.json")
        with open(metadata_path, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)
        file_date = os.path.splitext(os.path.basename(source.location))[0].split("_")[1]
        if year is None:
            year = file_date[2:4]
        rinex_outfile = os.path.join(workdir, f"{site}_{file_date}_rinex.{year}O")
        file_tmp_dest = shutil.copy(source.location, os.path.join(workdir, os.path.basename(source.location)))

        cmd = [
            str(binary_path),
            "-meta",
            metadata_path,
            "-out",
            rinex_outfile,
        ]
        cmd.extend([file_tmp_dest])
        result = subprocess.run(cmd, check=True, capture_output=True)
        if show_details:
            # logger.info("showing details")
            if len(result.stdout.decode()):
                print(f"{os.path.basename(source.location)}: {result.stdout.decode().rstrip()}")
            #print(result.stderr.decode())
        logger.info(f"Converted Novatel files to RINEX: {rinex_outfile}")
        rinex_data = RinexFile(parent_id=source.uuid)
        rinex_data.read(rinex_outfile)
        
       
    return rinex_data

def novatel_to_rinex(source:NovatelFile, site: str, year: str = None,outdir:str=None,show_details: bool=False,**kwargs) -> RinexFile:
    assert isinstance(source, NovatelFile), "Invalid source file type"
    rinex = _novatel_to_rinex(source,site,year,show_details=show_details,**kwargs)
    if outdir:
        rinex.write(outdir)
    return rinex

def novatel770_to_rinex(source:Novatel770File, site: str, year: str = None,outdir:str=None,show_details: bool=False,**kwargs) -> RinexFile:
    assert isinstance(source, Novatel770File), "Invalid source file type"
    rinex = _novatel_to_rinex(source,site,year,show_details=show_details,**kwargs)
    if outdir:
        rinex.write(outdir)
    return rinex


def novatelpin_to_rinex(
    source: NovatelPinFile,
    site: str,
    year: str = None,
    outdir: str = None,
    show_details: bool = False,
    **kwargs,
) -> RinexFile:
    assert isinstance(source, NovatelPinFile), "Invalid source file type"
    rinex = _novatel_to_rinex(source, site, year, show_details=show_details, **kwargs)
    if outdir:
        rinex.write(outdir)
    return rinex


def rinex_to_kin(source: RinexFile, site: str = "IVB1") -> KinFile:
    """
    Convert a RINEX file to a position file
    """
    assert isinstance(source, RinexFile), "Invalid source file type"

    logger.info(f"Converting RINEX file {source.location} to kin file")

    out = []

    with tempfile.TemporaryDirectory(dir="/tmp/",) as tmpoutdir:

        if not os.path.exists(source.location):
            logger.error(f"RINEX file {source.location} not found")
            return None
        result = subprocess.run(
            ["pdp3", "-m", "K", "--site", site, source.location],
            capture_output=True,
            cwd=tmpoutdir,
        )

    
        if result.stderr:
            logger.error(result.stderr)
  

        for root, _, files in os.walk(tmpoutdir):
            for file in files:
                if "kin_" in file:
                    source_path = os.path.join(root, file)
                    kin_file = KinFile(parent_id=source.uuid)
                    kin_file.read(source_path)
                    kin_file.location = os.path.basename(source_path)
                    logger.info(f"Converted RINEX file {source.location} to kin file {kin_file.location}")
                    break
    try:
        return kin_file
    except:
        return None


@pa.check_types        
def kin_to_gnssdf(source:KinFile) -> DataFrame[PositionDataFrame]:
    """
    Create an PositionDataFrame from a kin file from PRIDE-PPP

    Parameters:
        file_path (str): The path to the kin file

    Returns:
        dataframe (PositionDataFrame): An instance of the class.
    """

    with open(source.location, "r") as file:
        lines = file.readlines()

    end_header_index = next((i for i, line in enumerate(lines) if line.strip() == "END OF HEADER"), None)

    # Read data from lines after the end of the header
    data = []
    for idx,line in enumerate(lines[end_header_index + 2:]):
        split_line = line.strip().split()
        selected_columns = split_line[:9] + [split_line[-1]] # Ignore varying satellite numbers
        try:
            ppp : Union[PridePPP, ValidationError] = PridePPP.from_kin_file(selected_columns)
            data.append(ppp)
        except:
            error_msg = f"Error parsing into PridePPP from line {idx} in FILE {source.location} \n"
            error_msg += f"Line: {line}"
            logger.error(error_msg)
            pass

    # Check if data is empty
    if not data:
        error_msg = f"GNSS: No data found in FILE {source.location}"
        logger.error(error_msg)
        return None
    dataframe = pd.DataFrame([dict(pride_ppp) for pride_ppp in data])
    #dataframe.drop(columns=["modified_julian_date", "second_of_day"], inplace=True)

    log_response = f"GNSS Parser: {dataframe.shape[0]} shots from FILE {source.location}"
    logger.info(log_response)
    dataframe["time"] = dataframe["time"].dt.tz_localize("UTC")
    return dataframe

def qcpin_to_novatelpin(source:QCPinFile,outpath:Path) -> NovatelPinFile:
    with open(source.location) as file:
        pin_data = json.load(file)

    range_headers = []
    time_stamps = []

    for data in pin_data.values():
        range_header = data.get("observations").get("NOV_RANGE")
        time_header = data.get("observations").get("NOV_INS").get("time").get("common")
        range_headers.append(range_header)
        time_stamps.append(time_header)
        
    time_sorted = np.argsort(time_stamps)
    range_headers = [range_headers[i] for i in time_sorted]

    file_path = outpath/(source.uuid+"_novpin.txt")
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as temp_file:
        for header in range_headers:
            temp_file.write(header)
            temp_file.write("\n")
        temp_file.seek(0)
        novatel_pin = NovatelPinFile(location=temp_file.name)
        novatel_pin.read(path=temp_file.name)

    
    return novatel_pin
