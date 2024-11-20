from enum import Enum
from pydantic import BaseModel, Field
import datetime 
from typing import Optional

class REMOTE_TYPE(Enum):
    S3 = "s3"
    HTTP = "http"


class FILE_TYPE(Enum):
    SONARDYNE = "sonardyne"
    NOVATEL = "novatel"
    KIN = "kin"
    RINEX = "rinex"
    MASTER = "master"
    LEVERARM = "leverarm"
    SEABIRD = "svpavg"
    NOVATEL770 = "novatel770"
    DFPO00 = "dfop00"
    OFFLOAD = "offload"
    QCPIN = "pin"
    NOVATELPIN = "novatelpin"
    CTD = "ctd"
    @classmethod
    def to_schema(cls):
        return [x.name for x in cls]


FILE_TYPES = [x for x in FILE_TYPE]
ALIAS_MAP = {"nov770": "novatel770"}
ALIAS_MAP = ALIAS_MAP | {x: x for x in FILE_TYPES}


class DATA_TYPE(Enum):
    GNSS = "gnss"
    ACOUSTIC = "acoustic"
    SITECONFIG = "siteconfig"
    ATDOFFSET = "atdoffset"
    SVP = "svp"
    SHOTDATA = "shotdata"
    POSITION = "position"

    @classmethod
    def to_schema(cls):
        return [x.name for x in cls]


DATA_TYPES = [x.value for x in DATA_TYPE]

class DiscoveredFile(BaseModel):
    local_path: str = Field(title="Local path to file",default=None)
    type: str = Field(title="Type of file", enum=FILE_TYPES)
    timestamp_data_start: Optional[datetime.datetime] = Field(title="Timestamp of first data point")
    timestamp_data_end: Optional[datetime.datetime] = Field(
        title="Timestamp of last data point"
    )
    size:Optional[float] = Field(title="Size of file in bytes")
    remote_path:str = Field(title="Remote path to file",default=None)