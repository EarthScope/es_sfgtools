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
import warnings

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

    found_bad = df[filter_main]
    logger.info(f"Found {found_bad.shape[0]} overlapping ping-reply sequences")
    return df[~filter_main]

class BestGNSSPOSDATA(BaseModel):
    # https://docs.novatel.com/OEM7/Content/SPAN_Logs/BESTGNSSPOS.htm?tocpath=Commands%20%2526%20Logs%7CLogs%7CSPAN%20Logs%7C_____1
    sdx: float = None
    sdy: float = None
    sdz: float = None

    @classmethod
    def from_sv2(cls,line:str) -> "BestGNSSPOSDATA":
        data = line.split(";")[1].split(",")
        sdx = float(data[7])
        sdy = float(data[8])
        sdz = float(data[9])
        return cls(sdx=sdx,sdy=sdy,sdz=sdz)

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
            time=datetime.fromtimestamp(time),
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

    def update(self,data:dict):
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
        if isinstance(gnss,dict):
            positiondata.update(gnss)
        return positiondata

    @classmethod
    def from_sv3_gnss_ahrs(cls, gnss: dict, ahrs: dict) -> "PositionData":
        latitude = gnss.get("latitude", 0)
        longitude = gnss.get("longitude", 0)
        height = gnss.get("hae", 0)
        sdx = gnss.get("sdx", 0)
        sdy = gnss.get("sdy", 0)
        sdz = gnss.get("sdz", 0)
        time = datetime.fromtimestamp(gnss.get("time").get("common"))
        east, north, up = pm.geodetic2ecef(latitude, longitude, height)
        pitch = ahrs.get("p", 0)
        roll = ahrs.get("r", 0)
        head = ahrs.get("h", 0)
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
            sdx=sdx,
            sdy=sdy,
            sdz=sdz,
        )
      
class SV3InterrogationData(BaseModel):
    head0: float
    pitch0: float
    roll0: float
    east0: float
    north0: float
    up0: float
    east_std: Optional[float] = None
    north_std: Optional[float] = None
    up_std: Optional[float] = None
    triggerTime: datetime

    @classmethod
    def from_schemas(
        cls, positionData: PositionData, triggerTime: datetime
    ) -> "SV3InterrogationData":
        return cls(
            head0=positionData.head,
            pitch0=positionData.pitch,
            roll0=positionData.roll,
            east0=positionData.east,
            north0=positionData.north,
            up0=positionData.up,
            east_std=positionData.sdx,
            north_std=positionData.sdy,
            up_std=positionData.sdz,
            triggerTime=triggerTime,
        )

    @classmethod
    def from_DFOP00_line(cls, line) -> "SV3InterrogationData":
        nov_ins = line.get("observations").get("NOV_INS")
        gnss = line.get("observations").get("GNSS")
        
        if nov_ins is None:
            if gnss is None or gnss == "ERR3":
                return None
            position_data = PositionData.from_sv3_novins(gnss)
        else:
            position_data = PositionData.from_sv3_novins_gnss(
                novins=nov_ins,
                gnss=gnss,
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
    ) -> "ReplyData":

        if rangeData.range == 0:
            return None
        travelTime = get_traveltime(
            np.array([rangeData.range]),
            np.array([rangeData.tat]),
            triggerDelay=TRIGGER_DELAY_SV3,
        )[0]

        returnTime_sod = datetime_to_sod(
            rangeData.time
        )
        pingTime_sod = returnTime_sod - travelTime

        return cls(
            head1=positionData.head,
            pitch1=positionData.pitch,
            roll1=positionData.roll,
            east1=positionData.east,
            north1=positionData.north,
            up1=positionData.up,
            transponderID=rangeData.transponderID,
            dbv=rangeData.dbv,
            snr=rangeData.snr,
            xc=rangeData.xc,
            tt=travelTime,
            tat=rangeData.tat,
            pingTime=pingTime_sod,
            returnTime=returnTime_sod,
        )

    @classmethod
    def from_DFOP00_line(cls, line) -> "RangeData":
        NOV_INS = line.get("observations").get("NOV_INS")
        GNSS = line.get("observations").get("GNSS")
        AHRS = line.get("observations").get("AHRS")
        if NOV_INS is None:

            if GNSS is None or GNSS == "ERR3":
                return None
            if AHRS is None or AHRS == "ERR3":
                return None
            positionData = PositionData.from_sv3_gnss_ahrs(GNSS, AHRS)
        else:
            positionData = PositionData.from_sv3_novins_gnss(
                NOV_INS, GNSS
            )
        rangeData = RangeData.from_sv3(
            line.get("range"), line.get("time").get("common")
        )
        return cls.from_schemas(
            positionData, rangeData,
        )

    @classmethod
    def from_qcpin_line(cls, line) -> "RangeData":
        positionData = PositionData.from_sv3_novins(line.get("observations").get("NOV_INS"))
        rangeData = RangeData.from_sv3(
            line.get("range"), line.get("time").get("common")
        )
 
        return cls.from_schemas(
            positionData, rangeData
        )
