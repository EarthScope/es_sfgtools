import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime,  timedelta
from typing import Optional
import os
import logging
import json
import pandera as pa
from pandera.typing import DataFrame
import pymap3d as pm
from warnings import warn
from ..schemas.files import  DFPO00RawFile,QCPinFile
from ..schemas.observables.observation_schemas import ShotDataFrame

TRIGGER_DELAY_SV3 = 0.13  # SV3 trigger delay in seconds
logger = logging.getLogger(os.path.basename(__file__))

class DateOverlapWarning(UserWarning):
    message = "Ping-Reply sequence has overlapping dates"


def get_traveltime(range:float,tat:float,triggerDelay:float=TRIGGER_DELAY_SV3) -> float:
    assert range > 0, "Range must be greater than 0"
    assert tat < 1, "Turn around time must be less than 1"
    assert tat >= 0, "Turn around time must be greater than or equal to 0"

    tt = range - tat - triggerDelay
    return tt

def datetime_to_sod(dt:datetime) -> float:
    """Converts a datetime object to seconds of day

    Args:
        dt (datetime): datetime object

    Returns:
        float: datetime in seconds of day
    """
    return (
        dt - datetime(dt.year, dt.month, dt.day)
    ).total_seconds()

def get_triggertime(dt:datetime,triggerDelay:float=TRIGGER_DELAY_SV3) -> datetime:
    return dt - timedelta(seconds=triggerDelay)

class GNSSData(BaseModel):
    lattitude: float
    longitude: float
    hae: float
    sdx: Optional[float] = None
    sdy: Optional[float] = None
    sdz: Optional[float] = None
    timestamp: datetime

class AHRSData(BaseModel):
    heading: float
    pitch: float
    roll: float
    timestamp: datetime

class RangeData(BaseModel):
    transponderID: str
    dbv: float
    snr: float
    xc: float
    range: float
    tat: float = Field(ge=0,lt=1) # turn around time in seconds
    timestamp: datetime

    @classmethod
    def from_sv3(cls,data:dict,time:float) -> "RangeData":
        return cls(
            transponderID = data.get("cn").replace("IR",""),
            dbv = data.get("diag").get("dbv")[0],
            snr = data.get("diag").get("snr")[0],
            xc = data.get("diag").get("xc")[0],
            range = data.get("range"),
            tat = data.get("tat")/1000,
            timestamp = datetime.fromtimestamp(time)
        )


class InterrogationData(BaseModel):
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
    def from_schemas(cls,positionData:PositionData,triggerTime:datetime) -> "InterrogationData":
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
        position_data = PositionData.sv3_from_dfop00(line.get("observations").get("AHRS"),line.get("observations").get("GNSS"))
        pingTime_dt = datetime.fromtimestamp(line.get("time").get("common"))
        triggerTime_dt = get_triggertime(pingTime_dt)
        return cls.from_schemas(position_data,triggerTime_dt)
    
    @classmethod
    def from_qcpin_line(cls, line) -> "InterrogationData":
        position_data = PositionData.sv3_from_qc(line.get("observations").get("NOV_INS"))
        triggerTime_dt = get_triggertime(position_data.timestamp)
        return cls.from_schemas(position_data,triggerTime_dt)


class ReplyData(BaseModel):
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
    def from_schemas(cls,positionData:PositionData,rangeData:RangeData,travelTime:float,pingTime:float,returnTime:float) -> "ReplyData":
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
        positionData = PositionData.sv3_from_dfop00(line.get("observations").get("AHRS"),line.get("observations").get("GNSS"))
        rangeData = RangeData.from_sv3(line.get("range"),line.get("time").get("common"))
        if rangeData.range == 0:
            return None
        travelTime = get_traveltime(rangeData.range,rangeData.tat)

        returnTime_sod = datetime_to_sod(datetime.fromtimestamp(line.get("time").get("common")))
        pingTime_sod = returnTime_sod - travelTime
        return ReplyData.from_schemas(positionData,rangeData,travelTime,pingTime_sod,returnTime_sod)

    @classmethod
    def from_qcpin_line(cls, line) -> "RangeData":
        positionData = PositionData.sv3_from_qc(line.get("observations").get("NOV_INS"))
        rangeData = RangeData.from_sv3(line.get("range"),line.get("time").get("common"))
        if rangeData.range == 0:
            return None
        travelTime = get_traveltime(rangeData.range,rangeData.tat)
        returnTime_sod = datetime_to_sod(datetime.fromtimestamp(line.get("time").get("common")))
        pingTime_sod = returnTime_sod - travelTime
        return ReplyData.from_schemas(
            positionData, rangeData, travelTime,pingTime_sod, returnTime_sod
        )

def check_sequence_overlap(df:pd.DataFrame) -> bool:
    filter_0 = df.pingTime > df.returnTime
    filter_1 = df.pingTime < 0

    filter_main = filter_0 | filter_1
    if filter_main.any():
        warn(DateOverlapWarning)
        logger.warning(DateOverlapWarning.message)
    found_bad = df[filter_main]
    logger.info(f"Found {found_bad.shape[0]} overlapping ping-reply sequences")
    return df[~filter_main]

@pa.check_types
def check_df(df:pd.DataFrame) -> pd.DataFrame:
    df = check_sequence_overlap(df)
    return df


def dev_dfop00_to_shotdata(source: DFPO00RawFile) -> DataFrame[ShotDataFrame]:

    processed = []
    interrogation = None
    with open(source.local_path) as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            if data.get("event") == "interrogation":
                interrogation = InterrogationData.from_dfopoo_line(data)

            if data.get("event") == "range" and interrogation is not None:
                range_data = ReplyData.from_dfopoo_line(data)
                if range_data is not None:
                    processed.append((dict(interrogation) | dict(range_data)))
    df = pd.DataFrame(processed)
    return check_df(df)


def dev_qcpin_to_shotdata(source:QCPinFile) -> DataFrame[ShotDataFrame]:
    processed = []
    interrogation = None
    with open(source.local_path,'r') as f:
        try:
            data = json.load(f)
        except json.decoder.JSONDecodeError as e:
            logger.error(f"Error reading {source.local_path} {e}")
            return None
        for key, value in data.items():
            if key == "interrogation":
                interrogation = InterrogationData.from_qcpin_line(value)
            else:
                if interrogation is not None:
                    range_data = ReplyData.from_qcpin_line(value)
                    if range_data is not None:
                        processed.append((dict(interrogation) | dict(range_data)))
    df = pd.DataFrame(processed)
    if df.empty:
        return None
    return check_df(df)
