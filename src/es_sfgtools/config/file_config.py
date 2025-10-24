"""
This module defines enumerations for various types used in data management.

It includes types for remote storage, file types, data types, and assets,
providing a centralized and consistent way to refer to them throughout the
codebase.
"""
from enum import Enum


class REMOTE_TYPE(Enum):
    """Enumeration for remote storage types."""

    S3 = "s3"
    HTTP = "http"


class FILE_TYPE(Enum):
    """Enumeration for raw file types that can be ingested."""

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
        """Returns a list of all enum member names."""
        return [x.name for x in cls]


FILE_TYPES = [x for x in FILE_TYPE]
ALIAS_MAP = {"nov770": "novatel770"}
ALIAS_MAP = ALIAS_MAP | {x: x for x in FILE_TYPES}


class DOWNLOAD_TYPES(Enum):
    """Enumeration for file types that are eligible for download."""

    SONARDYNE = "sonardyne"
    NOVATEL000 = "novatel000"
    NOVATEL770 = "novatel770"
    DFPO00 = "dfop00"
    CTD = "ctd"
    SEABIRD = "svpavg"

    @classmethod
    def to_schema(cls):
        """Returns a list of all enum member names."""
        return [x.name for x in cls]


DEFAULT_FILE_TYPES_TO_DOWNLOAD = [x for x in DOWNLOAD_TYPES]


class DATA_TYPE(Enum):
    """Enumeration for processed data types stored in TileDB arrays."""

    KINPOSITION = "kinposition"
    ACOUSTIC = "acoustic"
    SITECONFIG = "siteconfig"
    ATDOFFSET = "atdoffset"
    SVP = "svp"
    SHOTDATA = "shotdata"
    IMUPOSITION = "imuposition"

    @classmethod
    def to_schema(cls):
        """Returns a list of all enum member names."""
        return [x.name for x in cls]


DATA_TYPES = [x.value for x in DATA_TYPE]


class AssetType(Enum):
    """Enumeration for all asset types, both raw and processed."""

    NOVATEL = "novatel"
    NOVATEL770 = "novatel770"
    NOVATEL000 = "novatel000"
    DFOP00 = "dfop00"
    SONARDYNE = "sonardyne"
    RINEX = "rinex"
    KIN = "kin"
    SEABIRD = "seabird"
    CTD = "ctd"  # 2 column data
    LEVERARM = "leverarm"
    MASTER = "master"
    QCPIN = "qcpin"
    NOVATELPIN = "novatelpin"
    KINPOSITION = "kinposition"
    ACOUSTIC = "acoustic"
    SITECONFIG = "siteconfig"
    ATDOFFSET = "atdoffset"
    SVP = "svp"  # maybe doesn't work
    SHOTDATA = "shotdata"
    IMUPOSITION = "imuposition"
    KINRESIDUALS = "kinresiduals"
    GNSSOBSTDB = "GNSSOBSTDB"
    BCOFFLOAD = "bcoffload"

    _ = "default"