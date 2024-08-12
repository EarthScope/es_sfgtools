"""
Author: Franklyn Dunbar
Date: 2024-02-26
Email: franklyn.dunbar@earthscope.org
"""

import pandas as pd
from pydantic import BaseModel,Field,model_validator,ValidationError
import pandera as pa
from pandera.typing import Series,Index,DataFrame
from datetime import datetime,timezone, timedelta
from typing import List,Optional,Union
import logging
import re 
import os

# Configure logging
logger = logging.getLogger(os.path.basename(__file__))

from .defaults import GNSS_START_TIME
from .file_schemas import NovatelFile

INSPVA_LOG_INDEX = {
    0 : "Week_GNSS",
    1 : "Seconds_GNSS",
    2 : "Latitude",
    3 : "Longitude",
    4 : "Height",
    5 : "NorthVelocity",
    6 : "EastVelocity",
    7 : "UpVelocity",
    8 : "Roll",
    9 : "Pitch",
    10 : "Azimuth"
}
class INSPVA(BaseModel):
    """

    Data class for INS Position, Velocity, and Attitude (PVA) log
    https://docs.novatel.com/OEM7/Content/SPAN_Logs/INSATT.htm#InertialSolutionStatus
    """
    Time: Optional[datetime] = None
    Week_GNSS : float = Field(ge=0, le=9999)
    Seconds_GNSS : float = Field(ge=0, le=604801)
    Latitude: float = Field(ge=-90, le=90)
    Longitude: float = Field(ge=-180, le=180)
    Height: float = Field(ge=-101, le=100)
    NorthVelocity: float = Field(ge=-10, le=10)
    EastVelocity: float = Field(ge=-10, le=10)
    UpVelocity: float = Field(ge=-10, le=10)
    Roll: float = Field(ge=-180, le=180)
    Pitch: float = Field(ge=-90, le=90)
    Azimuth: float = Field(ge=0, le=360)


    @model_validator(mode='after')
    def populate_time(cls, values):
        # Convert GNSS time to standard datetime format
        values = values
        values.Time = GNSS_START_TIME + timedelta(weeks=values.Week_GNSS, seconds=values.Seconds_GNSS)
        return values

    @classmethod
    def from_novatel(cls,data:List[str]) -> Union['INSPVA',ValidationError]:
        """
        Method to create an instance of the class from a list of strings
        """
        try:
            data_dict = {}
            for i, item in enumerate(data):
                field = INSPVA_LOG_INDEX[i]
                data_dict[field] = item
            return cls(**data_dict)
        
        except ValidationError as e:
            error_msg = f"An error occurred while creating an instance of the class: {e}"
          
            return ValidationError(error_msg)


class IMUDataFrame(pa.DataFrameModel):
    """Dataframe Schema for INS Position, Velocity, and Attitude (PVA) log data
    """
    Time: Series[datetime] = pa.Field(ge=GNSS_START_TIME.replace(tzinfo=None), coerce=True,
        description="Timestamp of the measurement in millisecond precision (UTC) [Y-M-D-H-M-S]")
    Latitude :  Series[float] = pa.Field(ge=-90, le=90,coerce=True,
        description="Latitude from the GNSS receiver (WGS84) [degrees]")
    Longitude :  Series[float] = pa.Field(ge=-180, le=180,coerce=True,
        description="Longitude from the GNSS receiver (WGS84) [degrees]")
    Height :  Series[float] = pa.Field(ge=-101, le=100,coerce=True,
        description="Ellipsoidal Height [m]")
    NorthVelocity : Series[float] = pa.Field(ge=-10, le=10,coerce=True,
        description="Velocity in a northerly direction (a negative value implies a southerly direction) [m/s]")
    EastVelocity :  Series[float] = pa.Field(ge=-10, le=10,coerce=True,
        description="Velocity in an easterly direction (a negative value implies a westerly direction) [m/s]")
    UpVelocity :  Series[float] = pa.Field(ge=-10, le=10,coerce=True,
        description="Velocity in an up direction [m/s]")
    Roll :  Series[float] = pa.Field(ge=-180, le=180,coerce=True,
        description="Right-handed rotation from local level around y‑axis in degrees")
    Pitch :  Series[float] = pa.Field(ge=-90, le=90,coerce=True,
        description="Right-handed rotation from local level around x‑axis in degrees")
    Azimuth :  Series[float] = pa.Field(ge=0, le=360,coerce=True,
        description="Left-handed rotation around z-axis in degrees clockwise from North. This is the inertial azimuth calculated from the IMU gyros and the SPAN filters.")

    @classmethod
    def from_inspva(cls, data: List[INSPVA]) -> 'IMUDataFrame':
        """
        Create an instance of the class from a list of INSPVA objects.

        Parameters:
            data (List[INSPVA]): A list of INSPVA objects.

        Returns:
            dataframe (IMUDataframe): An instance of the class.
        """
        dataframe = pd.DataFrame([dict(inspva) for inspva in data])
        return cls(dataframe)
    
    @classmethod
    def load(cls,source:Union[NovatelFile,str,pd.DataFrame]) -> DataFrame['IMUDataFrame']:
        if isinstance(source,NovatelFile):
            return cls.from_file(source.file)
        elif isinstance(source,str):
            df = pd.read_csv(source)
            return cls.validate(df,lazy=True)
        elif isinstance(source,pd.DataFrame):
            return cls.validate(source,lazy=True)
        else:
            raise ValueError("Source must be a NovatelFile or a string path to a file")

    @classmethod
    def from_file(cls, file_path: str,source:str=None) -> 'IMUDataFrame':
        """
        Create an IMUDataFrame object from a file.

        Args:
            file_path (str): The local path to the file containing the IMU data.
            source (str): The aws source of the data. Default is None.
        Returns:
            IMUDataFrame: An instance of the IMUDataFrame class.

        Raises:
            FileNotFoundError: If the file specified by file_path does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"IMU Parsing: The file {file_path} does not exist.")
        if source is None:
            source = file_path

        inspvaa_pattern = re.compile("#INSPVAA,")
        data_list = []
        line_number = 0
        with open(file_path) as inspva_file:
            while True:
                try:
                    line = inspva_file.readline()
                    line_number += 1
                    if not line:
                        break
                    if re.search(inspvaa_pattern, line):
                        inspva_data = line.split(';')[1].split(',')  # Get data after heading
                        inspva_data = inspva_data[:-1]  # Remove the status message
                        inspva: Union[INSPVA, ValidationError] = INSPVA.from_novatel(inspva_data)
                        if isinstance(inspva, INSPVA):
                            data_list.append(inspva)
                        else:
                            error_msg = f"IMU Parsing: An error occurred while parsing INVSPA data from FILE {source} at LINE {line_number} \n"
                            error_msg += f"Error: {line}"
                            logger.error(error_msg)
                            pass
                except UnicodeDecodeError as e:
                    error_msg = f"IMU Parsing:{e} | Error parsing FILE {source} at LINE {line_number}"
                    logger.error(error_msg)
                    pass
                    
        if not data_list:
            error_msg = f"IMU Parsing: No data was parsed from FILE {source}"
            logger.error(error_msg)
            return None
        
        dataframe = cls.from_inspva(data_list)
        dataframe = dataframe.drop(columns=["Week_GNSS", "Seconds_GNSS"])

        log_respnse = f"IMU Parser: {dataframe.shape[0]} rows from FILE {source}"
        logger.info(log_respnse)
        return dataframe

if __name__ == "__main__":
    # Test
    file_path = "/Users/franklyndunbar/Project/SeaFloorGeodesy/seafloor-geodesy/tests/resources/garpos_etl/novatel/bcnovatel_20180605000000.txt"
    print(IMUDataFrame.get_metadata())
    inspva_df = IMUDataFrame.from_file(file_path)
    inspva_df = inspva_df.iloc[0:11]
    print(inspva_df.head())
    inspva_df.to_csv(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/seafloor-geodesy/tests/resources/inspva_from_novatel.csv",
        index=False,
    )
