from pydantic import BaseModel,Field,field_validator
from typing import Optional,Union
from datetime import datetime
from pathlib import Path
import mmap
from enum import Enum

class BaseObservable(BaseModel):
    """
    Base class for observable objects.

    Attributes:
        local_path (Optional[Union[str,Path]]): The local_path of the data.
        id (Optional[str]): The ID of the object.
        epoch_id (Optional[str]): The ID of the epoch.
        campaign_id (Optional[str]): The ID of the campaign.
        timestamp_data_start (Optional[datetime]): The capture time of the data.
        data (Optional[mmap.mmap]): The data object.

    Methods:
        read(path: Union[str,Path]): Read the data from the local_path.
        write(dir: Union[str,Path]): Write the data to the local_path.

    Notes:
        This class is intended to be subclassed by other classes.
        read/write methods are used to interface between temporary files and the data object.
    """

    local_path: Optional[Union[str, Path]] = Field(default=None)
    uuid: Optional[int] = Field(default=None)
    epoch_id: Optional[str] = Field(default=None)
    campaign_id: Optional[str] = Field(default=None)
    timestamp_data_start: Optional[datetime] = Field(default=None)
    timestamp_data_end: Optional[datetime] = Field(default=None)
    data: Optional[mmap.mmap] = Field(default=None, exclude=True)

    class Config:
        arbitrary_types_allowed = True

    def read(self, path: Union[str, Path]):
        """
        Read the data from the local_path.

        Args:
            path (Union[str,Path]): The path to the data file.
        """
        with open(path, "r+b") as f:
            self.data = mmap.mmap(f.fileno(), 0)
        self.local_path = path
    def write(self, dir: Union[str, Path]):
        """
        Write the data to the local_path.

        Args:
            dir (Union[str,Path]): The directory to write the data file to.
        """

        path = Path(dir) / Path(self.local_path).name
        with open(path, "w+b") as f:
            f.write(self.data)
        self.local_path = path

class AssetType(Enum):
    NOVATEL = "novatel"
    NOVATEL770 = "novatel770"
    DFOPOO = "dfop00"
    SONARDYNE = "sonardyne"
    RINEX = "rinex"
    KIN = "kin"
    SEABIRD = "seabird"
    CTD = "ctd"
    LEVERARM = "leverarm"
    MASTER = "master"
    QCPIN = "qcpin"
    NOVATELPIN = "novatelpin"
    DEFAULT = "default"


class AssetEntry(BaseModel):
    local_path: Union[str,Path] = Field(default=None)
    type: Optional[AssetType] = Field(default=AssetType.DEFAULT)
    id: Optional[int] = Field(default=None)
    network: Optional[str] = Field(default=None)
    station: Optional[str] = Field(default=None)
    survey: Optional[str] = Field(default=None)

    timestamp_data_start: Optional[datetime] = Field(default=None)
    timestamp_data_end: Optional[datetime] = Field(default=None)
    timestamp_created: Optional[datetime] = Field(default=None)
    parent_id: Optional[int] = Field(default=None)
    size: Optional[float] = Field(default=None)

    @field_validator('local_path',mode='before')
    def _check_local_path(cls, v:Union[str,Path]):
        if v is None:
            raise ValueError("local_path must be set")
        if isinstance(v,str):
            v = Path(v)
        if not v.exists():
            raise ValueError(f"local_path {str(v)} does not exist")
        return v
    
    class Config:
        arbitrary_types_allowed = True

class NovatelFile(BaseObservable):
    """
    Represents a Novatel file from SV2.

    This class provides methods and attributes to handle Novatel files in the SeaFloorGeodesy project.
    Used to get acoustic/IMU data from Novatel files.

    Processing Functions:
        src.processing.functions.gnss_functions.novatel_to_rinex
        src.processing.functions.imu_functions.novatel_to_imudf

    """
    name:str = "novatel"


class Novatel770File(BaseObservable):
    """
    Represents a Novatel700 file from SV3.

    This class provides methods and attributes to handle Novatel files in the SeaFloorGeodesy project.
    Used to get acoustic/IMU data from Novatel files.

    Processing Functions:
        src.processing.functions.gnss_functions.novatel700_to_rinex
       

    """
    name:str = "novatel770"


class DFPO00RawFile(BaseObservable):
    """

    The DFOP00.raw file contains the real time amalgamation of all sensors using the common JSON style format. 
    For each range cycle there are multiple JSON entries created. 
    The first entry is the “Interrogate” entry which contains the GNSS, AHRS, INS & 
    TIME data from when the acoustic signal is transmitted.

    processing functions:
        src.processing.functions.acoustic_functions.dfpo00_to_imudf
        src.processing.functions.acoustic_functions.dfpo00_to_acousticdf
    """
    name:str = "dfpo00"


class SonardyneFile(BaseObservable):
    """

    Processing Functions:
        src.processing.functions.acoustic_functions.sonardyne_to_acousticdf
    """
    name:str = "sonardyne"


class RinexFile(BaseObservable):
    """
    Represents a RINEX file.

    Processing Functions:
        src.processing.functions.gnss_functions.rinex_to_kin

    Attributes:
        parent_id (Optional[str]): The ID of the parent file, if any.
    """
    name:str = "rinex"
    parent_uuid: Optional[str] = None
   
    site: Optional[str] = None
    basename: Optional[str] = None

 
    def _get_time(self,line):
        time_values = line.split("GPS")[0].strip().split()
        start_time = datetime(
            year=int(time_values[0]),
            month=int(time_values[1]),
            day=int(time_values[2]),
            hour=int(time_values[3]),
            minute=int(time_values[4]),
            second=int(float(time_values[5])),
        )
        return start_time
    
    def get_meta(self):
        with open(self.local_path) as f:
            files = f.readlines()
            for line in files:
                if "TIME OF FIRST OBS" in line:
                    start_time = self._get_time(line)
                    file_date = start_time.strftime("%Y%m%d%H%M%S")
                    self.timestamp_data_start = start_time
                    self.local_path = f"{self.site}_{file_date}_rinex.{str(start_time.year)[2:]}O"
                if "TIME OF LAST OBS" in line:
                    end_time = self._get_time(line)
                    self.timestamp_data_end = end_time
                    break
