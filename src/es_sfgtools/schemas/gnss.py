"""
Author: Rachel Terry
Date: 2024-03-21
Emails: rachel.terry@earthsocpe.org
"""

import pandas as pd
from pydantic import BaseModel, Field, model_validator, ValidationError
import pandera as pa
from pandera.typing import Series
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
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))
from .defaults import GNSS_START_TIME
from .file_schemas import KinFile
# Configure logging
logger = logging.getLogger(os.path.basename(__file__))


PRIDE_PPP_LOG_INDEX = {
    0 : "modified_julian_date",
    1 : "second_of_day",
    2 : "x",
    3 : "y",
    4 : "z",
    5 : "latitude",
    6 : "longitude",
    7 : "height",
    8 : "number_of_satellites", # 8 is minimum number of satellites for accurate positioning
    9 : "pdop"
}

def process_rinex_file(working_dir, rinex_file):
    if not os.path.exists(rinex_file):
        logger.error(f"RINEX file {rinex_file} not found")
        return None

    result = subprocess.run(
        ["pdp3", "-m", "K", "--site", "IVBN", rinex_file],
        capture_output=True,
        cwd=working_dir,
    )

    if result.stdout:
        logger.info(result.stdout.decode("utf-8"))

    if result.stderr:
        logger.error(result.stderr.decode("utf-8"))

    return None


class PridePPP(BaseModel):
    """
    Data class for PPP GNSS kinematic position output
    Docs: https://github.com/PrideLab/PRIDE-PPPAR
    """
    modified_julian_date: float = Field(ge=0)
    second_of_day: float = Field(ge=0, le=86400)
    x: float = Field(ge=-6378100, le=6378100,)         # ECEF X coordinate
    y: float = Field(ge=-6378100, le=6378100,)         # ECEF Y coordinate
    z: float = Field(ge=-6378100, le=6378100,)         # ECEF Z coordinate
    latitude: float = Field(ge=-90, le=90)             # WGS84 latitude
    longitude: float = Field(ge=0, le=360)             # WGS84 longitude
    height: float = Field(ge=-101, le=100)             # WGS84 height (m)
    number_of_satellites: int = Field(default=1,ge=0, le=125)    # Average Number of available satellites
    pdop: float = Field(default=0,ge=0, le=20)                   # Position Dilution of Precision
    time: Optional[datetime] = None
    class Config:
        coerce = True
    @model_validator(mode='after')
    def populate_time(cls, values):
        """ Convert from modified julian date and seconds of day to standard datetime format """
        values = values
        julian_date = values.modified_julian_date + (values.second_of_day/86400) + 2400000.5
        t = julian.from_jd(julian_date, fmt='jd')
        values.time = t

        return values

    @classmethod
    def from_kin_file(cls, data: List[str]) -> Union['PridePPP', ValidationError]:
        """
        Read kinematic position file and return a DataFrame
        """
        try:
            data_dict = {}
            if "*" in data:
                data.remove("*")
   
            if len(data) < 10:
                data.insert(-1,1) # account for missing number of satellites

            for i, item in enumerate(data):
                field = PRIDE_PPP_LOG_INDEX[i]
                data_dict[field] = item
            return cls(**data_dict)
        except ValidationError:
            raise Exception("Error parsing into PridePPP")


class PositionDataFrame(pa.DataFrameModel):
    """
    Data frame Schema for GNSS Position Data
    """
    time: Series[datetime] = pa.Field(ge=GNSS_START_TIME.replace(tzinfo=None), coerce=True,
        description="Timestamp of the measurement in millisecond precision (UTC) [Y-M-D-H-M-S]")
    x : Series[float] = pa.Field(ge=-6378100, le=6378100, coerce=True,
        description="ECEF X coordinate [m]")
    y : Series[float] = pa.Field(ge=-6378100, le=6378100, coerce=True,
        description="ECEF Y coordinate [m]")
    z : Series[float] = pa.Field(ge=-6378100, le=6378100, coerce=True,
        description="ECEF Z coordinate [m]")
    latitude :  Series[float] = pa.Field(ge=-90, le=90, coerce=True,
        description="Latitude from the GNSS receiver (WGS84) [degrees]")
    longitude :  Series[float] = pa.Field(ge=-180, le=360, coerce=True,
        description="Longitude from the GNSS receiver (WGS84) [degrees]")
    height :  Series[float] = pa.Field(ge=-101, le=100, coerce=True,        # todo unsure of the range, ex. mountain lake
        description="Ellipsoidal Height (WGS84) [m]")
    number_of_satellites :  Series[int] = pa.Field(ge=0, le=125, coerce=True, # todo unsure of the range, there are 125 GNSS satellites but obviously not all are visible
        description="Average number of satellites used in the position solution")
    pdop :  Series[float] = pa.Field(ge=0, le=20, coerce=True,  # todo unsure of the full range, below 4 is great, 4-8 acceptable, above 8 is poor (should we throw these out?)
        description="Position Dilution of Precision")

    @classmethod
    def from_ppp(cls, data: List[PridePPP]) -> Union['PositionDataFrame', ValidationError]:
        """
        Method to create an instance of the class from a list
        """
        dataframe = pd.DataFrame([dict(pride_ppp) for pride_ppp in data])
        dataframe_validated = cls.validate(dataframe, lazy=True)
        return dataframe_validated

    @classmethod
    def load(cls,source:Union[KinFile,str,pd.DataFrame]) -> 'PositionDataFrame':
        """
        Load a PositionDataFrame from a source
        """
        if isinstance(source, KinFile):
            return cls.from_kinfile(source.file)
        elif isinstance(source, str):
            assert os.path.exists(source), "GNSS: File does not exist"
            df = pd.read_csv(source)
            return cls.validate(df, lazy=True)
        elif isinstance(source, pd.DataFrame):
            return cls.validate(source, lazy=True)
        else:
            raise ValueError("Source must be a KinFile or a string path to a file")
    @classmethod
    def from_kinfile(cls, file_path: str) -> 'PositionDataFrame':
        """
        Create an PositionDataFrame from a kin file from PRIDE-PPP

        Parameters:
            file_path (str): The path to the kin file

        Returns:
            dataframe (PositionDataFrame): An instance of the class.
        """

        with open(file_path, "r") as file:
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
                error_msg = f"Error parsing into PridePPP from line {idx} in FILE {file_path} \n"
                error_msg += f"Line: {line}"
                logger.error(error_msg)
                pass

        # Check if data is empty
        if not data:
            error_msg = f"GNSS: No data found in FILE {file_path}"
            logger.error(error_msg)
            return None
        dataframe = cls.from_ppp(data)
        dataframe.drop(columns=["modified_julian_date", "second_of_day"], inplace=True)

        log_response = f"GNSS Parser: {dataframe.shape[0]} shots from FILE {file_path}"
        logger.info(log_response)
        return dataframe

    @classmethod
    def from_rinex(cls, rinex_files: Union[str, List[str]]) -> List["PositionDataFrame"]:
        """
        Convert a RINEX file to a position file
        """
        # helper function for testing
        if '.csv' in rinex_files and os.path.exists(rinex_files):
            
            df = pd.read_csv(rinex_files)
            return [cls.validate(df, lazy=True)]
            
        # Check if pride ppp is installed
        try:
            result = subprocess.run(["pdp3", "--version"], capture_output=True)
        except FileNotFoundError:
            response = "PRIDE-PPP is not installed/found. Please install PRIDE-PPP to use this function: https://github.com/PrideLab/PRIDE-PPPAR"
            logger.error(response)
            return None

        logger.info(f"Converting RINEX files {rinex_files} to 'PositionDataFrame' format")

        if not isinstance(rinex_files, list):
            rinex_files = [rinex_files]

        working_dir = "/tmp/pride_ppp"
        os.makedirs(working_dir, exist_ok=True)
        # dbug mode
        # dfs = []
        # for root, _, files in os.walk(working_dir):
        #     for file in files:
        #         if "kin_" in file:
        #             dfs.append(PositionDataFrame.from_file(os.path.join(root, file)))
        # if not dfs:
        with tempfile.TemporaryDirectory(dir=working_dir) as working_dir:
            with Pool() as p:
                process_func = partial(process_rinex_file, working_dir)
                p.map(process_func, rinex_files)

            dfs = []
            for root, _, files in os.walk(working_dir):
                for file in files:
                    if "kin_" in file:
                        dfs.append(PositionDataFrame.from_kinfile(os.path.join(root, file)))

        return dfs  


if __name__ == '__main__':
    import os
    logging.basicConfig(filename='gnss.log', level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    test_dir = "/Users/franklyndunbar/Project/SeaFloorGeodesy/seafloor-geodesy/tests/"
    gnss_df_path = os.path.join(test_dir, "resources/garpos_etl/test_gnss.csv")
    rinex_path = os.path.join(
        test_dir,
        "resources/garpos_etl/rinex/bcnovatel_20180605000000.18O",
    )
    gnss_dataframe = PositionDataFrame.from_rinex(rinex_path)
    gnss_dataframe = gnss_dataframe[0]
    print(gnss_dataframe.head())    

    gnss_dataframe.to_csv(gnss_df_path)
