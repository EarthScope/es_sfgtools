"""
Author: Franklyn Dunbar
Date: 2024-03-07
Email: franklyn.dunbar@earthscope.org
"""

import pandas as pd
from pydantic import BaseModel, Field
from typing import List, Union, Optional
from datetime import datetime, timedelta
import pymap3d as pm
import numpy as np
from .constants import GNSS_START_TIME,TRIGGER_DELAY_SV2,TRIGGER_DELAY_SV3,ADJ_LEAP,STATION_OFFSETS,MASTER_STATION_ID
from es_sfgtools.utils.loggers import ProcessLogger as logger
from decimal import Decimal, getcontext
# Set precision for Decimal operations
getcontext().prec = 10

class DateOverlapWarning(UserWarning):
    message = "Ping-Reply sequence has overlapping dates"

def get_traveltime_range(
        range:Decimal,
        tat: Decimal = 0.0,
) -> Decimal:
    """Calculates the travel time of a ping-reply sequence from range and turn around time

    Args:
        range (Decimal): range in seconds
        tat (Decimal, optional): turn around time in seconds. Defaults to 0.0.

    Raises:
        ValueError: if travel time is negative

    Returns:
        Decimal: travel time in seconds
    """
    travelTime = range - tat
    if travelTime < 0:
        logger.logerr(f"Negative travel time detected: {travelTime} seconds")
        raise ValueError("Travel time cannot be negative")
    return travelTime
    
def datetime_to_sod(dt: Union[datetime,np.ndarray]) -> Decimal:
    """Converts a datetime object to seconds of day

    Args:
        dt (datetime): datetime object

    Returns:
        Decimal: datetime in seconds of day
    """
    if isinstance(dt,datetime):
        dt = np.array([dt])
    for i in range(len(dt)):
        dt[i] = (dt[i] - datetime(dt[i].year, dt[i].month, dt[i].day)).total_seconds()
    return dt


def getPingtime(dt: datetime, triggerDelay: Decimal = TRIGGER_DELAY_SV3) -> datetime:
    return dt + timedelta(seconds=triggerDelay)

# def check_sequence_overlap(df: pd.DataFrame) -> pd.DataFrame:
#     filter_0 = df.pingTime > df.returnTime
#     filter_1 = df.pingTime < 0
#     filter_main = filter_0 | filter_1
#     found_bad = df[filter_main]
#     if not found_bad.empty:
#         logger.loginfo(f"Found {found_bad.shape[0]} invalid ping-reply sequences")
#     return df[~filter_main]

class BestGNSSPOSDATA(BaseModel):
    # https://docs.novatel.com/OEM7/Content/SPAN_Logs/BESTGNSSPOS.htm?tocpath=Commands%20%2526%20Logs%7CLogs%7CSPAN%20Logs%7C_____1
    sdx: Decimal = None
    sdy: Decimal = None
    sdz: Decimal = None

    @classmethod
    def from_sv2(cls,line:str) -> "BestGNSSPOSDATA":
        data = line.split(";")[1].split(",")
        sdx = Decimal(data[7])
        sdy = Decimal(data[8])
        sdz = Decimal(data[9])
        return cls(sdx=sdx,sdy=sdy,sdz=sdz)

class RangeData(BaseModel):
    transponderID: str
    dbv: Decimal
    snr: Decimal
    xc: Decimal
    range: Decimal = Field(description="Two way travel time in seconds including beacons TAT", ge=0)
    tat: Decimal = Field(ge=0, lt=1,description="Beacons turn around time")  # turn around time in seconds
    time: Decimal = Field(ge=GNSS_START_TIME.timestamp(), description="Time of the range measurement in unix timestamp format")

    @classmethod
    def from_sv3(cls, data: dict, time: Decimal) -> "RangeData":
        return cls(
            transponderID=data.get("cn").replace("IR", ""),
            dbv=data.get("diag").get("dbv")[0],
            snr=data.get("diag").get("snr")[0],
            xc=data.get("diag").get("xc")[0],
            range=data.get("range"),
            tat=data.get("tat") / 1000,
            time=time,
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
    latitude: Decimal
    longitude: Decimal
    height: Decimal
    east: Optional[Decimal] = None
    north: Optional[Decimal] = None
    up: Optional[Decimal] = None
    roll: Optional[Decimal] = None
    pitch: Optional[Decimal] = None
    head: Optional[Decimal] = None
    sdx: Optional[Decimal] = None
    sdy: Optional[Decimal] = None
    sdz: Optional[Decimal] = None

    def update(self,data:dict):
        # logger.logdebug(f"Updating Position Data")
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
        week_seconds = Decimal(line[1])
        latitude = Decimal(line[2])
        longitude = Decimal(line[3])
        height = Decimal(line[4])
        roll = Decimal(line[8])
        pitch = Decimal(line[9])
        head = Decimal(line[10])
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
    head0: Decimal
    pitch0: Decimal
    roll0: Decimal
    east0: Decimal
    north0: Decimal
    up0: Decimal
    east_std: Optional[Decimal] = None
    north_std: Optional[Decimal] = None
    up_std: Optional[Decimal] = None
    pingTime: Decimal
  
    @classmethod
    def from_schemas(
        cls, positionData: PositionData, pingTime: datetime
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
            pingTime=pingTime,
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
        pingTime = line.get("time").get("common")
        return cls.from_schemas(position_data, pingTime)

    @classmethod
    def from_qcpin_line(cls, line) -> "SV3InterrogationData":
        position_data = PositionData.from_sv3_novins(
            line.get("observations").get("NOV_INS")
        )
        pingTime = line.get("time").get("common")
        return cls.from_schemas(position_data, pingTime)

class SV3ReplyData(BaseModel):
    head1: Decimal
    pitch1: Decimal
    roll1: Decimal
    east1: Decimal
    north1: Decimal
    up1: Decimal
    transponderID: str
    dbv: Decimal
    snr: Decimal
    xc: Decimal
    tt: Decimal
    tat: Decimal
    returnTime: Decimal

    @classmethod
    def from_schemas(
        cls,
        positionData: PositionData,
        rangeData: RangeData,
    ) -> "SV3ReplyData":

        if rangeData.range == 0:
            return None
        travelTime = float(rangeData.range) - float(rangeData.tat) - TRIGGER_DELAY_SV3

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
            returnTime=rangeData.time
        )

    @classmethod
    def from_DFOP00_line(cls, line) -> "RangeData":
        NOV_INS = line.get("observations").get("NOV_INS")
        GNSS = line.get("observations").get("GNSS")
        AHRS = line.get("observations").get("AHRS")
        if NOV_INS is None:

            if GNSS is None or GNSS == "ERR3":
                logger.logerr("GNSS data not found")
                return None
            if AHRS is None or AHRS == "ERR3":
                logger.logerr("AHRS data not found")
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
