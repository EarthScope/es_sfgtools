"""
Author: Franklyn Dunbar
Date: 2024-03-07
Email: franklyn.dunbar@earthscope.org
"""

import pandas as pd
from pydantic import BaseModel, Field, ValidationError
import pandera as pa
from pandera.typing import Series, DataFrame
from typing import List, Dict, Union, Optional
from enum import Enum
from datetime import datetime, timezone, timedelta
import julian
import os
import re
import julian
import logging
import json
import pymap3d as pm
import numpy as np

from .constants import GNSS_START_TIME,TRIGGER_DELAY_SV2,TRIGGER_DELAY_SV3,ADJ_LEAP,STATION_OFFSETS,MASTER_STATION_ID

logger = logging.getLogger(__name__)


class DateOverlapWarning(UserWarning):
    message = "Ping-Reply sequence has overlapping dates"


def get_traveltime(
    range: np.ndarray, tat: np.ndarray, triggerDelay: float = TRIGGER_DELAY_SV3
) -> np.ndarray:
    assert (range > 0).all(), "Range must be greater than 0"
    assert (tat < 1).all(), "Turn around time must be less than 1"
    assert (tat >= 0).all(), "Turn around time must be greater than or equal to 0"

    tt = range - tat - triggerDelay
    return tt


def datetime_to_sod(dt: Union[datetime,np.ndarray]) -> float:
    """Converts a datetime object to seconds of day

    Args:
        dt (datetime): datetime object

    Returns:
        float: datetime in seconds of day
    """
    if isinstance(dt,datetime):
        dt = np.array([dt])
    for i in range(len(dt)):
        dt[i] = (dt[i] - datetime(dt[i].year, dt[i].month, dt[i].day)).total_seconds()
    return dt


def get_triggertime(dt: datetime, triggerDelay: float = TRIGGER_DELAY_SV3) -> datetime:
    return dt - timedelta(seconds=triggerDelay)


def check_sequence_overlap(df: pd.DataFrame) -> pd.DataFrame:
    filter_0 = df.pingTime > df.returnTime
    filter_1 = df.pingTime < 0

    filter_main = filter_0 | filter_1
    if filter_main.any():
        warn(DateOverlapWarning)
        logger.warning(DateOverlapWarning.message)
    found_bad = df[filter_main]
    logger.info(f"Found {found_bad.shape[0]} overlapping ping-reply sequences")
    return df[~filter_main]



class RangeData(BaseModel):
    transponderID: str
    dbv: float
    snr: float
    xc: float
    range: float
    tat: float = Field(ge=0, lt=1)  # turn around time in seconds
    time: datetime

    @classmethod
    def from_sv3(cls, data: dict, time: float) -> "RangeData":
        return cls(
            transponderID=data.get("cn").replace("IR", ""),
            dbv=data.get("diag").get("dbv")[0],
            snr=data.get("diag").get("snr")[0],
            xc=data.get("diag").get("xc")[0],
            range=data.get("range"),
            tat=data.get("tat") / 1000,
            timestamp=datetime.fromtimestamp(time),
        )
    @classmethod
    def from_sv2(cls,line:str,station_offsets:dict) -> List["RangeData"]:
        range_data_set = []
        log_header,range_data_str = line.split(">SI:")
        timestamp = datetime.strptime(log_header.split(",")[-1].strip(), "%Y/%m/%d %H:%M:%S.%f")
        
        transponder_data_set = []

        # parse transponder logs and ditch the header
        # 2003,327470,1527706670,2018/05/30 18:57:50.495 >SI:2010,INT1,IR5209;R4470626;[XC70,DBV-15],
        # IR5210;R3282120;[XC90,DBV0],IR5211;R5403623;[XC60,DBV-24]
        # -> ["5209;R4470626;[XC70,DBV-15],","5210;R3282120;[XC90,DBV0],","5211;R5403623;[XC60,DBV-24]"]
        transponder_logs = range_data_str.split("IR")[1:]
        if not transponder_logs:
            raise Exception(f"Expected 3 transponder logs, None Found")

        for transponder in transponder_logs:
            # "5210;R3282120;[XC90,DBV0]" -> "5209","R4470626","[XC70,DBV-15]"
            transponderID, travel_time, xc_db = transponder.split(";")

            # [XC70,DBV-15] -> "XC70","DBV-15"
            corr_score, dbv = xc_db.replace("[", "").replace("]", "").split(",")[:2]
            # "DBV-15" -> -15
            dbv = int(dbv.replace("DBV", ""))
            # "XC70" -> "70"
            corr_score = corr_score.replace("XC", "")

            # "R4470626" -> 4470626
            travel_time_micro = int(travel_time.replace("R", ""))

            # 4470626 -> 4.470626, convert from microseconds to seconds
            range_ = travel_time_micro / 1000000.000

            tat = station_offsets.get(transponderID, 0)/1000
            if tat == 0:
                raise Exception(f"Transponder {transponderID} not found in station offsets")
            if range_ > 0:
                    
                transponder_data_set.append(
                    cls(range=range_, transponderID=transponderID, dbv=dbv, snr=0, xc=corr_score, tat=tat, time=timestamp)
                )
        return transponder_data_set

class PositionData(BaseModel):
    time: datetime
    latitude: float
    longitude: float
    height: float
    east: Optional[float] = None
    north: Optional[float] = None
    up: Optional[float] = None
    roll: Optional[float] = None
    pitch: Optional[float] = None
    head: Optional[float] = None
    sdx: Optional[float] = None
    sdy: Optional[float] = None
    sdz: Optional[float] = None

    def update(data:dict):
        # update position,time, and standard deviation
        self.time = datetime.fromtimestamp(data.get("time").get("common"))
        self.latitude = data.get("latitude")
        self.longitude = data.get("longitude")
        self.height = data.get("hae")
        self.sdx = data.get("sdx")
        self.sdy = data.get("sdy")
        self.sdz = data.get("sdz")
        self.east, self.north, self.up = pm.geodetic2ecef(data.get("latitude"), data.get("longitude"), data.get("hae"))

    @classmethod
    def from_sv2_inspvaa(cls, line) -> "PositionData":
        # process sv2 line
        # 2001,97200,1526266800,2018/05/14 03:00:00.323 #INSPVAA,COM3,0,58.5,FINESTEERING,2001,97200.200,00000000,54e2,13386;
        # 2001,97200.200000000,55.02183281158,-156.71907213033,15.0975,-0.5983,-0.1397,0.1928,3.044402775,-0.986403548,308.979276345,INS_SOLUTION_GOOD*2ed757be

        # split line
        line = line.split(";")[1].split(",")
        gnss_week = int(line[0])
        week_seconds = float(line[1])
        latitude = float(line[2])
        longitude = float(line[3])
        height = float(line[4])
        roll = float(line[8])
        pitch = float(line[9])
        head = float(line[10])
        time = GNSS_START_TIME + timedelta(weeks=gnss_week, seconds=week_seconds)
        east, north, up = pm.geodetic2ecef(latitude, longitude, height)
        return cls(
            time=time,
            latitude=latitude,
            longitude=longitude,
            height=height,
            east=east,
            north=north,
            up=up,
            roll=roll,
            pitch=pitch,
            head=head,
        )

    @classmethod
    def from_sv3_novins(cls, dict) -> "PositionData":
        latitude = dict.get("latitude", 0)
        longitude = dict.get("longitude", 0)
        height = dict.get("hae", 0)
        pitch = dict.get("p", 0)
        roll = dict.get("r", 0)
        head = dict.get("h", 0)
        time = datetime.fromtimestamp(dict.get("time").get("common"))
        east, north, up = pm.geodetic2ecef(latitude, longitude, height)
        return cls(
            time=time,
            latitude=latitude,
            longitude=longitude,
            height=height,
            east=east,
            north=north,
            up=up,
            roll=roll,
            pitch=pitch,
            head=head,
        )

    @classmethod
    def from_sv3_novins_gnss(cls, novins: dict, gnss: dict) -> "PositionData":
        
        positiondata = PositionData.from_sv3_novins(novins)
        positiondata.update(gnss)
        return positiondata


# class SimultaneousInterrogation(BaseModel):
#     # TODO rename to simultaneious interrogation
#     responses: List[TransponderData]
#     pingData: PingData

#     def apply_offsets(self, offset_dict: Dict[str, float]):
#         """
#         Apply the given offsets to the transponder data.

#         Args:
#             offset_dict (Dict[str,float]): A dictionary of transponder offsets in milliseconds.

#         Returns:
#             None
#         """
#         for response in self.responses:
#             transponder_id = response.TransponderID
#             if transponder_id in offset_dict:
#                 response.correct_travel_time(offset_dict[transponder_id])

#     @classmethod
#     def from_line(
#         cls, line, pingdata: pingData
#     ) -> Union["SimultaneousInterrogation", Exception]:
#         # Input line sample
#         # 2003,327470,1527706670,2018/05/30 18:57:50.495 >SI:2010,INT1,IR5209;R4470626;[XC70,DBV-15],
#         # IR5210;R3282120;[XC90,DBV0],IR5211;R5403623;[XC60,DBV-24]
#         transponder_header = "IR"
#         transponder_data_set: List[TransponderData] = []

#         # parse transponder logs and ditch the header
#         # 2003,327470,1527706670,2018/05/30 18:57:50.495 >SI:2010,INT1,IR5209;R4470626;[XC70,DBV-15],
#         # IR5210;R3282120;[XC90,DBV0],IR5211;R5403623;[XC60,DBV-24]
#         # -> ["5209;R4470626;[XC70,DBV-15],","5210;R3282120;[XC90,DBV0],","5211;R5403623;[XC60,DBV-24]"]
#         transponder_logs = line.split(transponder_header)[1:]

#         if not transponder_logs:
#             return Exception(f"Expected 3 transponder logs, None Found")

#         for transponder in transponder_logs:
#             # "5210;R3282120;[XC90,DBV0]" -> "5209","R4470626","[XC70,DBV-15]"
#             transponderID, travel_time, xc_db = transponder.split(";")

#             # [XC70,DBV-15] -> "XC70","DBV-15"
#             corr_score, dbv = xc_db.replace("[", "").replace("]", "").split(",")[:2]

#             # "R4470626" -> 4470626
#             travel_time = int(travel_time.replace("R", ""))

#             # 4470626 -> 4.470626, convert from microseconds to seconds
#             travel_time = travel_time / 1000000.000

#             # "DBV-15" -> -15
#             dbv = int(dbv.replace("DBV", ""))

#             # "XC70" -> "70"
#             corr_score = corr_score.replace("XC", "")

#             # Computing return time from transponder travel time [s] and pingtime[julian date]
#             return_time = travel_time + pingdata.PingTime

#             transponder_data = replyData(
#                 transponderID=transponderID,
#                 tt=travel_time,
#                 returnTime=return_time,
#                 dbv=dbv,
#                 correlationScore=int(corr_score),
#             )
#             transponder_data_set.append(transponder_data)

#         simultaneous_interrogation = cls(
#             responses=transponder_data_set, pingData=pingdata
#         )

#         return simultaneous_interrogation


class SV3InterrogationData(BaseModel):
    head0: float
    pitch0: float
    roll0: float
    east0: float
    north0: float
    up0: float
    east0_std: Optional[float] = None
    north0_std: Optional[float] = None
    up0_std: Optional[float] = None
    triggerTime: datetime

    @classmethod
    def from_schemas(
        cls, positionData: PositionData, triggerTime: datetime
    ) -> "InterrogationData":
        return cls(
            head0=positionData.heading,
            pitch0=positionData.pitch,
            roll0=positionData.roll,
            east0=positionData.east,
            north0=positionData.north,
            up0=positionData.up,
            east0_std=positionData.east_std,
            north0_std=positionData.north_std,
            up0_std=positionData.up_std,
            triggerTime=triggerTime,
        )

    @classmethod
    def from_dfopoo_line(cls, line) -> "InterrogationData":
        position_data = PositionData.from_sv3_novins_gnss(
            novins=line.get("observations").get("NOV_INS"),
            gnss=line.get("observations").get("GNSS"),
        )
        pingTime_dt = datetime.fromtimestamp(line.get("time").get("common"))
        triggerTime_dt = get_triggertime(pingTime_dt)
        return cls.from_schemas(position_data, triggerTime_dt)

    @classmethod
    def from_qcpin_line(cls, line) -> "InterrogationData":
        position_data = PositionData.from_sv3_novins(
            line.get("observations").get("NOV_INS")
        )
        triggerTime_dt = get_triggertime(position_data.time)
        return cls.from_schemas(position_data, triggerTime_dt)


class SV3ReplyData(BaseModel):
    head1: float
    pitch1: float
    roll1: float
    east1: float
    north1: float
    up1: float
    east1_std: Optional[float] = None
    north1_std: Optional[float] = None
    up1_std: Optional[float] = None
    transponderID: str
    dbv: float
    snr: float
    xc: float
    tt: float
    tat: float
    returnTime: float
    pingTime: float

    @classmethod
    def from_schemas(
        cls,
        positionData: PositionData,
        rangeData: RangeData,
        travelTime: float,
        pingTime: float,
        returnTime: float,
    ) -> "ReplyData":
        return cls(
            head1=positionData.heading,
            pitch1=positionData.pitch,
            roll1=positionData.roll,
            east1=positionData.east,
            north1=positionData.north,
            up1=positionData.up,
            east1_std=positionData.east_std,
            north1_std=positionData.north_std,
            up1_std=positionData.up_std,
            transponderID=rangeData.transponderID,
            dbv=rangeData.dbv,
            snr=rangeData.snr,
            xc=rangeData.xc,
            tt=travelTime,
            tat=rangeData.tat,
            pingTime=pingTime,
            returnTime=returnTime,
        )

    @classmethod
    def from_dfopoo_line(cls, line) -> "RangeData":
        positionData = PositionData.sv3_from_dfop00(
            line.get("observations").get("AHRS"), line.get("observations").get("GNSS")
        )
        rangeData = RangeData.from_sv3(
            line.get("range"), line.get("time").get("common")
        )
        if rangeData.range == 0:
            return None
        travelTime = get_traveltime(rangeData.range, rangeData.tat)

        returnTime_sod = datetime_to_sod(
            datetime.fromtimestamp(line.get("time").get("common"))
        )
        pingTime_sod = returnTime_sod - travelTime
        return ReplyData.from_schemas(
            positionData, rangeData, travelTime, pingTime_sod, returnTime_sod
        )

    @classmethod
    def from_qcpin_line(cls, line) -> "RangeData":
        positionData = PositionData.sv3_from_qc(line.get("observations").get("NOV_INS"))
        rangeData = RangeData.from_sv3(
            line.get("range"), line.get("time").get("common")
        )
        if rangeData.range == 0:
            return None
        travelTime = get_traveltime(rangeData.range, rangeData.tat)
        returnTime_sod = datetime_to_sod(
            datetime.fromtimestamp(line.get("time").get("common"))
        )
        pingTime_sod = returnTime_sod - travelTime
        return ReplyData.from_schemas(
            positionData, rangeData, travelTime, pingTime_sod, returnTime_sod
        )
