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

from .constants import GNSS_START_TIME,TRIGGER_DELAY_SV2,TRIGGER_DELAY_SV3,ADJ_LEAP,STATION_OFFSETS,MASTER_STATION_ID

logger = logging.getLogger(__name__)

class GNSSData(BaseModel):
    """
    Represents GNSS data with timestamps and related attributes.

    Attributes:
        Time (datetime): Time of the GNSS data [datetime].
        Latitude (float): Latitude [degrees].
        Longitude (float): Longitude [degrees].
        Height (float): Height [m].
    """

    time: datetime
    latitude: float
    longitude: float
    hae: Optional[float] = None
    sdx: Optional[float] = None
    sdy: Optional[float] = None
    sdz: Optional[float] = None

    @classmethod
    def from_dict(cls, dict) -> "GNSSData":
        """
        Create a GNSSData instance from a line of text.

        Args:
            line (str): A line of text containing comma-separated values.

        Returns:
            GNSSData: An instance of GNSSData created from the provided line.

        """
        lat = dict.get("latitude", 0)
        lon = dict.get("longitude", 0)
        hae = dict.get("hae", 0)
        sdx = dict.get("sdx", 0)
        sdy = dict.get("sdy", 0)
        sdz = dict.get("sdz", 0)
        time = datetime.fromtimestamp(dict.get("time").get("common"))
        return cls(time=time, latitude=lat, longitude=lon, hae=hae, sdx=sdx, sdy=sdy, sdz=sdz)


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

    def update(data:GNSSData):
        # update position,time, and standard deviation
        self.time = data.time
        self.latitude = data.latitude
        self.longitude = data.longitude
        self.height = data.hae
        self.sdx = data.sdx
        self.sdy = data.sdy
        self.sdz = data.sdz
        self.east, self.north, self.up = pm.geodetic2ecef(data.latitude, data.longitude, data.hae)

        
    @classmethod
    def from_sv2(cls, line) -> "PositionData":
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


class PingData(BaseModel):
    """
    Represents Ping Data with timestamps and related attributes.

    Attributes:
        TriggerTime (datetime): Time when the ping was triggered [datetime].
        PingTime (float): Time when the ping was received in seconds of the day [s].
        PingOffset (float): Offset time between trigger and ping [s].
        ADJ_LEAP (float): Adjustment for leap time.
        TRIGGER_DELAY (float): Trigger delay time.
    """

    Sequence: Optional[int] = None
    PingTime: float = Field(ge=0, le=24 * 3600, default=None)
    TriggerTime: datetime = Field(ge=GNSS_START_TIME, default=None)
    PingOffset: float = Field(ge=-60, le=60, default=None)
    ADJ_LEAP: float = ADJ_LEAP

    @classmethod
    def from_line(cls, line) -> "PingData":
        """
        Create a PingData instance from a line of text.

        Args:
            line (str): A line of text containing comma-separated values.
            mode (WaveGlider, optional): The mode of the WaveGlider. Defaults to WaveGlider.SV2.

        Returns:
            PingData: An instance of PingData created from the provided line.

        Example:
            >>> line = "2003,327374,1527706574,2018/05/30 18:56:14.697 PING - Offset = 0.000"
            >>> PingData.from_line(line)
            PingData(PingTime=58268.539063657634, TriggerTime=datetime.datetime(2018, 5, 30, 12, 56, 14), PingOffset=0.0, ADJ_LEAP=1.0, TRIGGER_DELAY=0.1)
        """

        TRIGGER_DELAY = TRIGGER_DELAY_SV2

        # "2003,327374,1527706574,2018/05/30 18:56:14.697 PING - Offset = 0.000" -> ["2003","327374","1527706574","2018/05/30 18:56:14.697 PING - Offset = 0.000"]
        ping_data_parsed: List[str] = line.split(",")
        # 2018/05/30 18:56:14.697 -> datetime.datetime(2018, 5, 30, 18, 56, 14, 697000)
        trigger_time: datetime = datetime.strptime(
            ping_data_parsed[-1].split("PING")[0].strip(), "%Y/%m/%d %H:%M:%S.%f"
        )

        # ["2003","327374","1527706574","2018/05/30 18:56:14.697 PING - Offset = 0.000"] -> ["PING - Offset = 0.000"] -> "0.000" -> 0.000
        ping_offset: float = float(ping_data_parsed[-1].split(" ")[-1])

        # Compute time delta
        time_delta: float = TRIGGER_DELAY

        # 2018-05-30 18:56:14 + 0.1 + 0.0 = 2018-05-30 18:56:14.1
        ping_time = (
            trigger_time
            - datetime(trigger_time.year, trigger_time.month, trigger_time.day)
        ).total_seconds() + time_delta
        # ping_time: datetime = trigger_time + timedelta(seconds=time_delta)

        # ping_time_julian: float = julian.to_jd(ping_time, "mjd")

        return cls(TriggerTime=trigger_time, PingTime=ping_time, PingOffset=ping_offset)


class TransponderData(BaseModel):
    """
    Data class representing acoustic data measurements between a transducer and an indivudual transponder.

    Attributes:
        TransponderID (str): The unique identifier for the transponder.
        TwoWayTravelTime (float, optional): The two-way travel time in seconds [s].
        ReturnTime (float, optional): The return time julian day [days]
        DecibalVoltage (int): Signal relative to full scale voltage in dB.
        CorrelationScore (int): Correlation score.
    """

    TransponderID: str  # Transponder ID
    TwoWayTravelTime: float = Field(ge=0.0, le=600)  # Two-way Travel time [seconds]
    ReturnTime: float = Field(
        ge=0, le=3600 * 24
    )  # Return time since the start of day (modified Julian day) [days]
    DecibalVoltage: int = Field(
        ge=-100, le=100
    )  # Signal relative to full scale voltage [dB]
    CorrelationScore: int = Field(ge=0, le=100)  # Correlation score

    SignalToNoise: Optional[float] = Field(
        ge=0, le=100, default=0
    )  # Signal to noise ratio

    TurnAroundTime: Optional[float] = Field(
        ge=0, le=100, default=0
    )  # Turn around time [ms]

    def correct_travel_time(self, offset: float):
        """
        Corrects the travel time by applying the given offset.

        Args:
            offset (float): The offset value in milliseconds.

        Returns:
            None
        """
        offset_seconds = offset / 1000.0
        offset_fractional_days = offset_seconds / 86400.0
        # self.ReturnTime -= offset_fractional_days
        self.TwoWayTravelTime -= offset_seconds


class SimultaneousInterrogation(BaseModel):
    # TODO rename to simultaneious interrogation
    responses: List[TransponderData]
    pingData: PingData

    def apply_offsets(self, offset_dict: Dict[str, float]):
        """
        Apply the given offsets to the transponder data.

        Args:
            offset_dict (Dict[str,float]): A dictionary of transponder offsets in milliseconds.

        Returns:
            None
        """
        for response in self.responses:
            transponder_id = response.TransponderID
            if transponder_id in offset_dict:
                response.correct_travel_time(offset_dict[transponder_id])

    @classmethod
    def from_line(
        cls, line, pingdata: PingData
    ) -> Union["SimultaneousInterrogation", Exception]:
        # Input line sample
        # 2003,327470,1527706670,2018/05/30 18:57:50.495 >SI:2010,INT1,IR5209;R4470626;[XC70,DBV-15],
        # IR5210;R3282120;[XC90,DBV0],IR5211;R5403623;[XC60,DBV-24]
        transponder_header = "IR"
        transponder_data_set: List[TransponderData] = []

        # parse transponder logs and ditch the header
        # 2003,327470,1527706670,2018/05/30 18:57:50.495 >SI:2010,INT1,IR5209;R4470626;[XC70,DBV-15],
        # IR5210;R3282120;[XC90,DBV0],IR5211;R5403623;[XC60,DBV-24]
        # -> ["5209;R4470626;[XC70,DBV-15],","5210;R3282120;[XC90,DBV0],","5211;R5403623;[XC60,DBV-24]"]
        transponder_logs = line.split(transponder_header)[1:]

        if not transponder_logs:
            return Exception(f"Expected 3 transponder logs, None Found")

        for transponder in transponder_logs:
            # "5210;R3282120;[XC90,DBV0]" -> "5209","R4470626","[XC70,DBV-15]"
            transponderID, travel_time, xc_db = transponder.split(";")

            # [XC70,DBV-15] -> "XC70","DBV-15"
            corr_score, dbv = xc_db.replace("[", "").replace("]", "").split(",")[:2]

            # "R4470626" -> 4470626
            travel_time = int(travel_time.replace("R", ""))

            # 4470626 -> 4.470626, convert from microseconds to seconds
            travel_time = travel_time / 1000000.000

            # "DBV-15" -> -15
            dbv = int(dbv.replace("DBV", ""))

            # "XC70" -> "70"
            corr_score = corr_score.replace("XC", "")

            # Computing return time from transponder travel time [s] and pingtime[julian date]
            return_time = travel_time + pingdata.PingTime

            transponder_data = TransponderData(
                TransponderID=transponderID,
                TwoWayTravelTime=travel_time,
                ReturnTime=return_time,
                DecibalVoltage=dbv,
                CorrelationScore=int(corr_score),
            )
            transponder_data_set.append(transponder_data)

        simultaneous_interrogation = cls(
            responses=transponder_data_set, pingData=pingdata
        )

        return simultaneous_interrogation
