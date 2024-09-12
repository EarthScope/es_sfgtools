import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime,  timedelta
import os
import logging
import json

from ..schemas.files import  DFPO00RawFile,QCPinFile


TRIGGER_DELAY_SV3 = 0.13  # SV3 trigger delay in seconds
logger = logging.getLogger(os.path.basename(__file__))
class InterrogationData(BaseModel):
    head0: float
    pitch0: float
    roll0: float
    lat0: float
    lon0: float
    hae0: float
    # ping_time:float
    trigger_time: datetime

    @classmethod
    def from_dfopoo_line(cls, line) -> "InterrogationData":
        event_id = line.get("event_id")
        ahrs = line.get("observations").get("AHRS")
        head0 = ahrs.get("h")
        pitch0 = ahrs.get("p")
        roll0 = ahrs.get("r")
        gnss = line.get("observations").get("GNSS")
        lat0 = gnss.get("latitude")
        lon0 = gnss.get("longitude")
        hae0 = gnss.get("hae")
        ping_time_dt = datetime.fromtimestamp(line.get("time").get("common"))
        trigger_time_dt = ping_time_dt - timedelta(seconds=TRIGGER_DELAY_SV3)
       
        return cls(
            head0=head0,
            pitch0=pitch0,
            roll0=roll0,
            lat0=lat0,
            lon0=lon0,
            hae0=hae0,
            trigger_time=trigger_time_dt,
        )
    @classmethod
    def from_qcpin_line(cls, line) -> "InterrogationData":
        gnss  = line.get("observations").get("GNSS")
        ahrs = line.get("observations").get("NOV_INS")
        head0 = ahrs.get("h")
        pitch0 = ahrs.get("p")
        roll0 = ahrs.get("r")
        lat0 = gnss.get("latitude")
        lon0 = gnss.get("longitude")
        hae0 = gnss.get("hae")
        ping_time_dt = datetime.fromtimestamp(gnss.get("time").get("common"))
        trigger_time_dt = ping_time_dt - timedelta(seconds=TRIGGER_DELAY_SV3)
       
        return cls(
            head0=head0,
            pitch0=pitch0,
            roll0=roll0,
            lat0=lat0,
            lon0=lon0,
            hae0=hae0,
            trigger_time=trigger_time_dt,
        )
            


class RangeData(BaseModel):
    event_id: int
    head1: float
    pitch1: float
    roll1: float
    lat1: float
    lon1: float
    hae1: float
    transponder_id: str
    dbv: float
    snr: float
    xc: float
    tt: float
    tat: float
    ping_time: float
    reply_time: float

    @classmethod
    def from_dfopoo_line(cls, line) -> "RangeData":
        event_id = line.get("event_id")
        ahrs = line.get("observations").get("AHRS")
        head1 = ahrs.get("h")
        pitch1 = ahrs.get("p")
        roll1 = ahrs.get("r")
        gnss = line.get("observations").get("GNSS")
        lat1 = gnss.get("latitude")
        lon1 = gnss.get("longitude")
        hae1 = gnss.get("hae")
        range_data = line.get("range")
        transponder_id = range_data.get("cn", "").replace("IR", "")
        dbv = range_data.get("diag").get("dbv")[0]
        snr = range_data.get("diag").get("snr")[0]
        xc = range_data.get("diag").get("xc")[0]
        tat = range_data.get("tat")/1000
        tt = range_data.get("range") - tat- TRIGGER_DELAY_SV3
        reply_time = line.get("time").get("common")
        reply_time_dt = datetime.fromtimestamp(reply_time)
        reply_time_sod = (
            reply_time_dt
            - datetime(reply_time_dt.year, reply_time_dt.month, reply_time_dt.day)
        ).total_seconds()
        ping_time = reply_time_sod - tt
        return cls(
            event_id=event_id,
            head1=head1,
            pitch1=pitch1,
            roll1=roll1,
            lat1=lat1,
            lon1=lon1,
            hae1=hae1,
            transponder_id=transponder_id,
            dbv=dbv,
            snr=snr,
            xc=xc,
            tt=tt,
            tat=tat,
            reply_time=reply_time_sod,
            ping_time=ping_time,
        )

    @classmethod
    def from_qcpin_line(cls, line) -> "RangeData":
        nov_ins = line.get("observations").get("NOV_INS")
        range_data = line.get("range")
        head1 = nov_ins.get("h")
        pitch1 = nov_ins.get("p")
        roll1 = nov_ins.get("r")
        lat1 = nov_ins.get("latitude")
        lon1 = nov_ins.get("longitude")
        hae1 = nov_ins.get("hae")
        transponder_id = range_data.get("cn", "").replace("IR", "")
        dbv = range_data.get("diag").get("dbv")[0]
        snr = range_data.get("diag").get("snr")[0]
        xc = range_data.get("diag").get("xc")[0]
        tat = range_data.get("tat")/1000
        tt = range_data.get("range") - tat - TRIGGER_DELAY_SV3
        reply_time = line.get("time").get("common")
        reply_time_dt = datetime.fromtimestamp(reply_time)
        reply_time_sod = (
            reply_time_dt
            - datetime(reply_time_dt.year, reply_time_dt.month, reply_time_dt.day)
        ).total_seconds()
        ping_time = reply_time_sod - tt
        return cls(
            head1=head1,
            pitch1=pitch1,
            roll1=roll1,
            lat1=lat1,
            lon1=lon1,
            hae1=hae1,
            transponder_id=transponder_id,
            dbv=dbv,
            snr=snr,
            xc=xc,
            tt=tt,
            tat=tat,
            reply_time=reply_time_sod,
            ping_time=ping_time,
        )


def dev_dfop00_to_shotdata(source: DFPO00RawFile) -> pd.DataFrame:

    processed = []
    with open(source.local_path) as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            if data.get("event") == "interrogation":
                interrogation = InterrogationData.from_dfopoo_line(data)

            if data.get("event") == "range":
                range_data = RangeData.from_dfopoo_line(data)
                processed.append((dict(interrogation) | dict(range_data)))
    return pd.DataFrame(processed)


def dev_qcpin_to_shotdata(source:QCPinFile) -> pd.DataFrame:
    processed = []
    with open(source.local_path) as f:
        data = json.load(f)
        for key, value in data.items():
            if key == "interrogation":
                interrogation = InterrogationData.from_qcpin_line(value)
            else:
                range_data = RangeData.from_qcpin_line(value)
                processed.append((dict(interrogation) | dict(range_data)))
    return pd.DataFrame(processed)