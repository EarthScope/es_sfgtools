from pydantic import BaseModel,Field
from typing import Optional,Union
from datetime import datetime
from pathlib import Path
import mmap

class BaseObservable(BaseModel):
    """
    Base class for observable objects.

    Attributes:
        location (Optional[Union[str,Path]]): The location of the data.
        id (Optional[str]): The ID of the object.
        epoch_id (Optional[str]): The ID of the epoch.
        campaign_id (Optional[str]): The ID of the campaign.
        timestamp_data_start (Optional[datetime]): The capture time of the data.
        data (Optional[mmap.mmap]): The data object.

    Methods:
        read(path: Union[str,Path]): Read the data from the location.
        write(dir: Union[str,Path]): Write the data to the location.

    Notes:
        This class is intended to be subclassed by other classes.
        read/write methods are used to interface between temporary files and the data object.
    """

    location: Optional[Union[str, Path]] = Field(default=None)
    uuid: Optional[str] = Field(default=None)
    epoch_id: Optional[str] = Field(default=None)
    campaign_id: Optional[str] = Field(default=None)
    timestamp_data_start: Optional[datetime] = Field(default=None)
    timestamp_data_end: Optional[datetime] = Field(default=None)
    data: Optional[mmap.mmap] = Field(default=None, exclude=True)

    class Config:
        arbitrary_types_allowed = True

    def read(self, path: Union[str, Path]):
        """
        Read the data from the location.

        Args:
            path (Union[str,Path]): The path to the data file.
        """
        with open(path, "r+b") as f:
            self.data = mmap.mmap(f.fileno(), 0)
        self.location = path
    def write(self, dir: Union[str, Path]):
        """
        Write the data to the location.

        Args:
            dir (Union[str,Path]): The directory to write the data file to.
        """

        path = Path(dir) / Path(self.location).name
        with open(path, "w+b") as f:
            f.write(self.data)
        self.location = path


class BaseSite(BaseModel):
    """
    Represents a base site file for geodesy processing.

    Attributes:
        location (Union[str, Path]): The location of the base site.
        id (Optional[str]): The ID of the base site.
        site_id (Optional[str]): The site ID of the base site.
        campaign_id (Optional[str]): The campaign ID of the base site.
        timestamp_data_start (Optional[datetime]): The capture time of the base site.
    """
    location: Union[str, Path]
    uuid: Optional[str] = Field(default=None)
    site_id: Optional[str] = Field(default=None)
    campaign_id: Optional[str] = Field(default=None)
    timestamp_data_start: Optional[datetime] = Field(default=None)

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
        with open(self.location) as f:
            files = f.readlines()
            for line in files:
                if "TIME OF FIRST OBS" in line:
                    start_time = self._get_time(line)
                    file_date = start_time.strftime("%Y%m%H%m%S")
                    self.timestamp_data_start = start_time
                    self.location = f"{self.site}_{file_date}_rinex.{str(start_time.year)[2:]}O"
                if "TIME OF LAST OBS" in line:
                    end_time = self._get_time(line)
                    self.timestamp_data_end = end_time
                    break


class KinFile(BaseObservable):
    """
    Represents a Kin file, an intermediate file between RINEX and position files.

    Processing Functions:
        src.processing.functions.gnss_functions.kin_to_gnssdf

    Attributes:
        parent_id (Optional[str]): The parent ID of the Kin file.
    
    """
    name:str = "kin"
    extension:str =".kin"
    parent_uuid: Optional[str] = None
    start_time: Optional[datetime] = None


class SeaBirdFile(BaseSite):
    """
    Represents a SeaBird file. Used to parse out Sound Velocity Profile (SVP) data.

    Processing Functions:
        src.processing.functions.site_functions.seabird_to_svp
   
    """

    name:str = "seabird"

class CTDFile(BaseSite):
    """
    Represents a CTD file. Used to parse out Conductivity-Temperature-Depth (CTD) data.

    Processing Functions:
        src.processing.functions.site_functions.ctd_to_ctdprofile
    """
    name:str = "ctd"


class LeverArmFile(BaseSite):
    """
    Represents a lever arm file.
    Used to parse out Antenna-transducer-offset data
    
    Processing Functions:
        src.processing.functions.site_functions.leverarm_to_atdoffset
    """
    name:str = "leverarm"

class MasterFile(BaseSite):
    """
    Represents a master file for processing site data.
    
    Used to parse out site configuration data (transponder data and site center).

    Processing Functions:
        src.processing.functions.site_functions.masterfile_to_siteconfig
    """
    name:str = "master"

class QCPinFile(BaseObservable):
    name:str = "qcpin"

class NovatelPinFile(BaseObservable):
    name:str = "novatelpin"
