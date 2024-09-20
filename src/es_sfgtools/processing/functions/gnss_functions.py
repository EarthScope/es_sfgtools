from enum import Enum
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
import uuid 

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
        except ValidationError:
            raise Exception("Error parsing into PridePPP")


def get_metadata(site: str,serialNumber:str="XXXXXXXXXX") -> dict:
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


def novatel_to_rinex(
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

    metadata = get_metadata(site,serialNumber=uuid.uuid4().hex[:10])

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        metadata_path = os.path.join(workdir, "metadata.json")
        with open(metadata_path, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)

       
        if source.timestamp_data_start is not None: 
            file_date = source.timestamp_data_start
        else:
            file_date = os.path.splitext(os.path.basename(source.local_path))[0].split("_")[-4]
        if year is None:
            year = file_date[2:4]
        rinex_outfile = os.path.join(workdir, f"{site}_{file_date}_rinex.{year}O")
        file_tmp_dest = shutil.copy(source.local_path, os.path.join(workdir, os.path.basename(source.local_path)))

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
                print(f"{os.path.basename(source.local_path)}: {result.stdout.decode().rstrip()}")
            # print(result.stderr.decode())
        logger.info(f"Converted Novatel files to RINEX: {rinex_outfile}")
        rinex_data = RinexFile(parent_id=source.uuid,location=rinex_outfile,site=site)
        rinex_data.read(rinex_outfile) #load into mmap 
        rinex_data.get_meta()

    return rinex_data


def rinex_to_kin(source: RinexFile, writedir:Path, pridedir:Path, site="IVB1", show_details:bool=True, pdp_command:list=None) -> KinFile:
    """
    Convert a RINEX file to a position file
    """
    assert isinstance(source, RinexFile), "Invalid source file type"

    logger.info(f"Converting RINEX file {source.local_path} to kin file")

    out = []

    # simul link the rinex file to the same file with file_uuid attached at the front

    if not os.path.exists(source.local_path):
        logger.error(f"RINEX file {source.local_path} not found")
        return None
    tag = uuid.uuid4().hex[:4]

    # Generate pdp command with default settings
    if not pdp_command:
        pdp_command = PridePdpConfig().generate_pdp_command(site=tag, 
                                                            local_file_path=source.local_path)
    result = subprocess.run(
        pdp_command,
        capture_output=True,
        cwd=str(pridedir),
    )
    # ["pdp3", "--loose-edit" , "-m", "K", "--site", tag, str(source.local_path)],


    if result.stderr:
        logger.error(result.stderr)
    if pd.isna(source.timestamp_data_start):
        ts = str(source.local_path.name).split("_")[1]
        year = ts[:4]
        ts = ts[4:]
        month = ts[:2]
        ts = ts[2:]
        day= max(1,int(ts[:2]))
        ts = ts[2:]
        hour = ts[-4:-2]
        minute = ts[-2:]
        source.timestamp_data_start = datetime(year=int(year),month=int(month),day=int(day),hour=int(hour))

    file_pattern = f"{source.timestamp_data_start.year}{source.timestamp_data_start.timetuple().tm_yday}"
    tag_files = pridedir.rglob(f"*{tag}*")
    for tag_file in tag_files:
        #print("tag file:", tag_file)
        if "kin" in tag_file.name:
            kin_file = tag_file
            kin_file_new = str(kin_file).split("_")
            kin_file_new = "_".join(kin_file_new)
            kin_file_new = writedir/kin_file_new
            shutil.move(kin_file,kin_file_new)
            kin_file = KinFile(parent_id=source.uuid,start_time=source.timestamp_data_start,site=site,local_path=kin_file_new)
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


@pa.check_types        
def kin_to_gnssdf(source:KinFile) -> Union[DataFrame[PositionDataFrame],None]:
    """
    Create an PositionDataFrame from a kin file from PRIDE-PPP

    Parameters:
        file_path (str): The path to the kin file

    Returns:
        dataframe (PositionDataFrame): An instance of the class.
    """

    with open(source.local_path, "r") as file:
        lines = file.readlines()

    end_header_index = next((i for i, line in enumerate(lines) if line.strip() == "END OF HEADER"), None)

    # Read data from lines after the end of the header
    data = []
    if end_header_index is None:
        error_msg = f"GNSS: No header found in FILE {source.local_path}"
        logger.error(error_msg)
        return None
    for idx,line in enumerate(lines[end_header_index + 2:]):
        split_line = line.strip().split()
        selected_columns = split_line[:9] + [split_line[-1]] # Ignore varying satellite numbers
        try:
            ppp : Union[PridePPP, ValidationError] = PridePPP.from_kin_file(selected_columns)
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
    dataframe = pd.DataFrame([dict(pride_ppp) for pride_ppp in data])
    #dataframe.drop(columns=["modified_julian_date", "second_of_day"], inplace=True)

    log_response = f"GNSS Parser: {dataframe.shape[0]} shots from FILE {source.local_path}"
    logger.info(log_response)
    dataframe["time"] = dataframe["time"].dt.tz_localize("UTC")
    return dataframe

def qcpin_to_novatelpin(source:QCPinFile,outpath:Path) -> NovatelPinFile:
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
    range_headers = [range_headers[i] for i in time_sorted]

    file_path = outpath/(str(source.uuid)+"_novpin.txt")
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as temp_file:
        for header in range_headers:
            temp_file.write(header)
            temp_file.write("\n")
        temp_file.seek(0)
        novatel_pin = NovatelPinFile(location=temp_file.name)
        novatel_pin.read(path=temp_file.name)

    return novatel_pin


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


class PridePdpConfig:
    def __init__(self, 
                 system: str = "GREC23J", 
                 frequency: list = ["G12", "R12", "E15", "C26", "J12"], 
                 loose_edit: bool = True, 
                 cutoff_elevation: int =7, 
                 start: datetime = None, 
                 end: datetime = None, 
                 interval: float = None, 
                 high_ion: bool = False, 
                 tides: str = "SOP"):
        """
        Initialize the PridePdpConfig class with the following parameters:

        Args:
        system (str): The GNSS system(s) to use. Default is "GREC23J" which is “GPS/GLONASS/Galileo/BDS/BDS-2/BDS-3/QZSS”
        frequency (str): The GNSS frequencies to use. Default is "G12 R12 E15 C26 J12, Refer to Table 5-4 in PRIDE-PPP-AR v.3.0 manual for more options"
        loose_edit (bool): disable strict editing mode, which should be used when high dynamic data quality is poor. Default is True. 
        cutoff_elevation (int): The elevation cutoff angle in degrees (0-60 degrees). Default is 7.
        start (datetime): The start time used for processing. Default is None.
        end (datetime): The end time used for processing. Default is None.
        interval (float): Processing interval, values range from 0.02s to 30s. If this item is not specified and the configuration file is specified, the processing interval in the configuration file will be read, otherwise, the sampling rate of the observation file is used by default.
        high_ion (bool): Use 2nd ionospheric delay model with CODE's GIM product. When this option is not entered, no higher-order ionospheric correction is performed. Default is False.
        tides (str): Enter one or more of "S" "O" "P", e.g SO for solid, ocean, and polar tides. Default is "SOP", which uses all tides.
        """

        # Check if system is valid
        system = system.upper() # Default to GREC23J which is “GPS/GLONASS/Galileo/BDS/BDS-2/BDS-3/QZSS”
        for char in system:
            if char not in Constellations._value2member_map_:
                Constellations.print_options()
                raise ValueError(f"Invalid constelation character: {char}")
        self.system = system

        self.frequency = frequency
        self.loose_edit = loose_edit
        self.cutoff_elevation = cutoff_elevation

        self.start = start
        self.end = end
        self.interval = interval
        self.high_ion = high_ion
        
        # If entered, check if tide characters are valid
        tides = tides.upper()
        for char in tides:
            if char not in Tides._value2member_map_:
                Tides.print_options()
                raise ValueError(f"Invalid tide character: {char}")
        self.tides = tides
    
    def generate_pdp_command(self, site: str, local_file_path: str) -> List[str]:
        """
        Generate the command to run pdp3 with the given parameters
        """
        command = ["pdp3", "-m", "K"]

        if self.system != "GREC23J":
            command.extend(["--system", self.system])

        if self.frequency != ["G12", "R12", "E15", "C26", "J12"]:
            command.extend(["--frequency", " ".join(self.frequency)])

        if self.loose_edit:
            command.append("--loose-edit")

        if self.cutoff_elevation != 7:
            command.extend(["--cutoff-elev", self.cutoff_elevation])

        if self.start:
            command.extend(["--start", self.start.strftime("%Y/%m/%d %H:%M:%S")])

        if self.end:
            command.extend(["--end", self.end.strftime("%Y/%m/%d %H:%M:%S")])
        
        if self.interval:
            command.extend(["--interval", self.interval])

        if self.high_ion:
            command.append("--high-ion")

        if self.tides != "SOP":
            command.extend(["--tide-off", self.tides])

        command.extend(["--site", site])
        command.append(str(local_file_path))

        return command
