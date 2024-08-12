import pandas as pd
from pydantic import BaseModel, Field, model_validator, ValidationError
import pandera as pa
from pandera.typing import Series, Index, DataFrame
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Union
import logging
import re
import os
import json
import pymap3d as pm

from ..schemas.files.file_schemas import NovatelFile, DFPO00RawFile

logger = logging.getLogger(os.path.basename(__file__))

INSPVA_LOG_INDEX = {
    0: "Week_GNSS",
    1: "Seconds_GNSS",
    8: "Roll",
    9: "Pitch",
    10: "Azimuth",
}
GNSS_START_TIME = datetime(1980, 1, 6, tzinfo=timezone.utc)  # GNSS start time

class INSPVA(BaseModel):
    """

    Data class for INS Position, Velocity, and Attitude (PVA) log
    https://docs.novatel.com/OEM7/Content/SPAN_Logs/INSATT.htm#InertialSolutionStatus
    """

    Time: Optional[datetime] = None
    Week_GNSS: float = Field(ge=0, le=9999)
    Seconds_GNSS: float = Field(ge=0, le=604801)
    Roll: float = Field(ge=-180, le=180)
    Pitch: float = Field(ge=-90, le=90)
    Azimuth: float = Field(ge=0, le=360)

    @model_validator(mode="after")
    def populate_time(cls, values):
        # Convert GNSS time to standard datetime format
        values = values
        values.Time = GNSS_START_TIME + timedelta(
            weeks=values.Week_GNSS, seconds=values.Seconds_GNSS
        )
        return values

    @classmethod
    def from_novatel(cls, data: List[str]) -> Union["INSPVA", ValidationError]:
        """
        Method to create an instance of the class from a list of strings
        """
        try:
            data_dict = {}
            for i, item in enumerate(data):
                field = INSPVA_LOG_INDEX.get(i, None)
                if field is not None:
                    data_dict[field] = item
            return cls(**data_dict)

        except ValidationError as e:
            error_msg = (
                f"An error occurred while creating an instance of the class: {e}"
            )

            return ValidationError(error_msg)



def novatel_to_imudf(source:NovatelFile) -> pd.DataFrame:
    if not os.path.exists(source.location):
        raise FileNotFoundError(f"IMU Parsing: The file {source.location} does not exist.")

    inspvaa_pattern = re.compile("#INSPVAA,")
    data_list = []
    line_number = 0
    with open(source.location) as inspva_file:
        while True:
            try:
                line = inspva_file.readline()
                line_number += 1
                if not line:
                    break
                if re.search(inspvaa_pattern, line):
                    inspva_data = line.split(";")[1].split(
                        ","
                    )  # Get data after heading
                    inspva_data = inspva_data[:-1]  # Remove the status message
                    inspva: Union[INSPVA, ValidationError] = INSPVA.from_novatel(
                        inspva_data
                    )
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

    dataframe = pd.DataFrame([dict(inspva) for inspva in data_list])
    dataframe = dataframe.drop(columns=["Week_GNSS", "Seconds_GNSS"])

    log_respnse = f"IMU Parser: {dataframe.shape[0]} rows from FILE {source}"
    logger.info(log_respnse)
    return dataframe

def dfpo00_to_imudf(source:DFPO00RawFile) -> pd.DataFrame:
    """
    Create an IMUDataFrame from a DFPO00 file
    """
    imu_data = []
    with open(source.location) as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            if data.get("event") in ["interrogation","range"]:
                heading_data = data.get("observations").get("AHRS")
                
                if heading_data:
                    azimuth = heading_data.get("h",None)
                    pitch = heading_data.get("p",None)
                    roll = heading_data.get("r",None)
                    time = heading_data.get("time").get("common")
                    time_dt = datetime.fromtimestamp(time)
                    imu_data_dict = {
                        "Time":time_dt,
                        "Azimuth":azimuth,
                        "Pitch":pitch,
                        "Roll":roll,
                    }

                    imu_data.append(imu_data_dict)
    imu_df = pd.DataFrame(imu_data)
    # Drop duplicates found along time column
    imu_df = imu_df.drop_duplicates(subset=["Time"]).reset_index(drop=True)

    return imu_df