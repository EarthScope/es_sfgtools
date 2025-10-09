"""
This module contains constants for the data management module.
"""
import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


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
    NOVATEL000 = "novatel000"
    CTD = "ctd"
    @classmethod
    def to_schema(cls):
        return [x.name for x in cls]

FILE_TYPES = [x for x in FILE_TYPE]
ALIAS_MAP = {"nov770": "novatel770"}
ALIAS_MAP = ALIAS_MAP | {x: x for x in FILE_TYPES}

class DOWNLOAD_TYPES(Enum):
    SONARDYNE = "sonardyne"
    NOVATEL000 = "novatel000"
    NOVATEL770 = "novatel770"
    DFPO00 =  "dfop00"
    CTD = "ctd"
    SEABIRD = "svpavg"
    @classmethod
    def to_schema(cls):
        return [x.name for x in cls]

DEFAULT_FILE_TYPES_TO_DOWNLOAD = [x for x in DOWNLOAD_TYPES]


class DATA_TYPE(Enum):
    KINPOSITION = "kinposition"
    ACOUSTIC = "acoustic"
    SITECONFIG = "siteconfig"
    ATDOFFSET = "atdoffset"
    SVP = "svp"
    SHOTDATA = "shotdata"
    IMUPOSITION = "imuposition"

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