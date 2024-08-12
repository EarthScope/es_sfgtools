"""
Author: Franklyn Dunbar
Date: 2024-05-01
Email: franklyn.dunbar@earthscope.org, franklyn.dunbar@umontana.edu
"""

from pydantic import BaseModel
from typing import List, Optional, Union,Dict,Tuple,Generator,Callable,Any
import os
import logging
from pandera.typing import DataFrame
import re
from boto3 import client as boto3_client
from functools import wraps
import pandas as pd
import json 
import julian
import datetime 
from pathlib import Path
from ctypes import c_ubyte, c_uint16, c_uint32, c_uint64
import array

from ..utils import file_processing as filep
# Garpos Imports

from .generics import PositionLLH,PositionENU,SoundVelocityProfile,Transponder,ATDOffset
from .defaults import MASTER_STATION_ID,ADJ_LEAP
logger = logging.getLogger(__name__)

aws_logger = logging.getLogger("awslog")


class BaseFile(BaseModel):
    file: str = None
    load_method: Callable[[str],Any] = None
    date: Optional[datetime.datetime] = None
    check_path_key: Optional[str] = None

    # @checkinput
    def load(self):
        if not os.path.exists(self.file):
            logger.error(
                f"Schema {self.__class__.__name__} Loading File With {self.load.__name__} From {self.file} not found"
            )
            raise Exception(f"File {self.file} not found")

        if not self.check_path_key in self.file:
            logger.warning(
                f"Schema {self.__class__.__name__} File Check Key {self.check_path_key} Not I {self.file}"
            )
        self._get_date()
        return self.load_method(self.file)

    def load_remote(self,client:boto3_client,bucket:str,dest_dir:str):
        dest = os.path.join(dest_dir,os.path.basename(self.file))
        client.download_file(bucket,self.file,dest)
        if os.path.exists(dest):
            aws_logger.info(f"Downloaded {self.file} From Bucket {bucket} To {dest}")
            self.file = dest
            return self.load()

        else:
            response = f"Failed To Download {self.file} From Bucket {bucket} To {dest}"
            aws_logger.error(response)
            raise Exception(response)

    def _get_date(self):
        try:
            file_name = os.path.splitext(os.path.basename(self.file))[0]
            file_date = file_name.split("_")[-1]
            self.date = datetime.datetime.strptime(file_date, "%Y%m%d%H%M%S")
        except ValueError as e:
            pass


# class SonardyneFile(BaseFile):
#     load_method: Callable[[str],DataFrame[AcousticDataFrame]] = AcousticDataFrame.from_file
#     check_path_key:str = "sonardyne"

class SonardyneFile(BaseModel):
    file:str 


class KinFile(BaseModel):
    file:str
    site:Optional[str] = None

class RinexFile(BaseModel):
    file:str
    site:str

    def to_kin(self,outdir:str) -> KinFile:
        kin_file = filep.rinex_to_kin(self.file,outdir,self.site)
        if kin_file is None:
            raise FileNotFoundError(f"Kin File Not Found")
        return KinFile(file=kin_file,site=self.site)


class NovatelFile(BaseModel):
    file:str
    site:Optional[str] = None
    is_binary: bool = False
    year:Optional[str] = None

    def to_rinex(self,outdir:str) -> RinexFile:
        rinex_path = filep.novatel2rinex(self.file,outdir,self.site,binary=self.is_binary,year=self.year)
        rinex_file = RinexFile(file=rinex_path,site=self.site)
        return rinex_file

class NovatelFile000(BaseModel):
    file:str
    """ Class defaults """
    _dle = 16
    _stx = 2
    _etx = 3

    _got_dle = False
    _got_stx = False
    _buffer = array.array("B")

    _crc = c_ubyte(0)

    def to_novatel(self) -> NovatelFile:
        current_path = Path(self.file)
        processed_path = current_path.parent / str(current_path.name).replace("NOV000.bin","novatel.txt")
        if not current_path.exists():
            raise FileNotFoundError(f"File {self.file} not found")
        if not processed_path.exists():
            with open(self.file,'rb') as f:
                data = f.read()
            logs = data.split(b"\r\n")
            processed = []
            for log in logs:
                try:
                    processed.append('#'+log.split(b"#")[1].decode("utf-8"))
                except:
                    pass
            print(f"Processed {len(processed)} logs")
            if processed:
                with open(str(processed_path),'w') as f:
                    f.write("\n".join(processed))

        return NovatelFile(file=str(processed_path))


# class ObservationEpoch(BaseModel):
#     rinex: List[Union[RinexFile,str]]
#     novatel: List[Union[NovatelFile,str]]
#     sonardyne: List[Union[SonardyneFile,str]]

#     # Post Init
#     date: Optional[datetime] = None
#     timespan: Optional[datetime] = None

#     # Load
#     gnss: Optional[List[DataFrame[PositionDataFrame]]] = None
#     imu: Optional[List[DataFrame[IMUDataFrame]]] = None
#     acoustic: Optional[List[DataFrame[AcousticDataFrame]]] = None

#     def __post_init__(self):
#         self.rinex = [RinexFile(**rinex) for rinex in self.rinex].sort(key=lambda x: x.date)
#         self.novatel = [NovatelFile(**novatel) for novatel in self.novatel].sort(
#             key=lambda x: x.date
#         )
#         self.sonardyne = [
#             SonardyneFile(**sonardyne) for sonardyne in self.sonardyne
#         ].sort(key=lambda x: x.date)

#         dates = [file.date for file in self.rinex + self.novatel + self.sonardyne]
#         self.date = min(dates)
#         self.timespan = max(dates) - self.date

#     def __iter__(
#         self,
#     ) -> Generator[
#         Tuple[
#             DataFrame[IMUDataFrame],
#             DataFrame[PositionDataFrame],
#             DataFrame[AcousticDataFrame],
#         ],None,None
#     ]:
#         # return ziped generator of [novatel,rinex,sonardyne]
#         for novatel,rinex,sonardyne in zip(self.novatel,self.rinex,self.sonardyne):
#             yield (novatel.load(),rinex.load(),sonardyne.load())


class SeaBirdFile(BaseFile):
    load_method: Callable[[str],SoundVelocityProfile] = "SeaBirdFile._load_svp"
    check_path_key:str = "svp"

    def _get_date(self):
        """
        Get the file index from the sound speed file from the header

        * System UTC = May 15 2018 16:55:52
        # nquan = 7
        # nvalues = 2141                        
        # units = specified
        # name 0 = depSM: Depth [salt water, m]
        # name 1 = latitude: Latitude [deg]
        # name 2 = longitude: Longitude [deg]
        # name 3 = t090C: Temperature [ITS-90, deg C]
        # name 4 = sal00: Salinity, Practical [PSU]
        # name 5 = svCM: Sound Velocity [Chen-Millero, m/s]
        # name 6 = flag: flag
        # span 0 =      3.000,   2143.000       
        # span 1 =   54.34259,   54.34376       
        # span 2 = -158.43446, -158.42674       
        # span 3 =     1.8462,     6.5241       
        # span 4 =    32.2461,    34.6076       
        # span 5 =    1469.29,    1492.96       
        # span 6 = 0.0000e+00, 0.0000e+00       
        # interval = meters: 1                  
        # start_time = May 15 2018 16:54:01 [NMEA time, header]

        Raises:
            FileNotFoundError: _description_

        """
        if not os.path.exists(self.file):
            raise FileNotFoundError(f"File {self.file} not found")

        time_head = re.compile(r"start_time =")
        end_flag = re.compile("\*END\*")
        with open(self.file,'r') as f:
            lines = f.readlines()
            for line in lines:
                if time_head.search(line):
                    time = line.split("=")[1].strip().split("[")[0].strip()
                    self.date = datetime.datetime.strptime(time, "%B %d %Y %H:%M:%S")
                    break
                if end_flag.search(line):
                    break

    def _load_svp(self,_) -> DataFrame[SoundVelocityProfile]:
        """
        Read the sound velocity profile from a file
        fmt = [ Depth [m], Latitude [deg],Longitude [deg],Temperatures [deg C], Salinity [PSU] ,Speed [m/s]]

           *END*
         3.000   54.34259 -158.42674     6.4264    32.2921    1473.07 0.0000e+00
         4.000   54.34268 -158.42679     6.5241    32.2461    1473.41 0.0000e+00
         5.000   54.34266 -158.42679     6.5006    32.2566    1473.35 0.0000e+00
         6.000   54.34266 -158.42680     6.5028    32.2570    1473.38 0.0000e+00
         7.000   54.34266 -158.42680     6.4974    32.2562    1473.37 0.0000e+00
         8.000   54.34268 -158.42680     6.4987    32.2564    1473.39 0.0000e+00
         9.000   54.34268 -158.42680     6.4986    32.2575    1473.41 0.0000e+00
        10.000   54.34268 -158.42680     6.4905    32.2679    1473.41 0.0000e+00
        11.000   54.34268 -158.42680     6.4714    32.2786    1473.36 0.0000e+00
        12.000   54.34268 -158.42680     6.4070    32.3043    1473.16 0.0000e+00
        13.000   54.34268 -158.42680     6.2915    32.3382    1472.76 0.0000e+00
        14.000   54.34268 -158.42683     6.2515    32.3469    1472.63 0.0000e+00
        ...
        """
        with open(self.file, "r") as f:
            lines = f.readlines()
            data = []
            data_start = re.compile("\*END\*")
            while lines:
                line = lines.pop(0)
                if data_start.match(line):
                    break
            if not lines:
                logger.error(
                    f"No data found in the sound speed profile file {self.file}"
                )
                return None

            for line in lines:

                values = line.split()
                data.append(
                    {
                        "depth": float(values[0]),
                        "speed": float(values[5]),
                    }
                )
            df = pd.DataFrame(data)
            return SoundVelocityProfile.validate(df, lazy=True)

    def model_post_init(self, __context) -> None:
        self.load_method = self._load_svp


class LeverArmFile(BaseFile):
    """
    
    0.0 +0.575 -0.844

    """
    load_method: Callable[[str],ATDOffset] = ATDOffset.from_file
    check_path_key:str = "lever"


class MasterFile(BaseFile):
    load_method: Callable[[str],Tuple[List[Transponder],PositionLLH]] = None
    check_path_key:str = "master"
    """
        2018-06-01 00:00:00
    3
        0    16       54.316094952    -158.461771055    -2096.780      0.200000d0       1479.900
        1    17       54.335093588    -158.497431581    -2078.949      0.320000d0       1479.568
        2    18       54.344116381    -158.447751737    -2129.844      0.440000d0       1479.900
    6378137.d0  298.257222101d0   ! ellipsoid parameters  WGS84
    0.1d0              ! Maximum 3-D GPS positional sigma (m)
    0                   ! Input GPS positional uncertainties given as: if std. dev. = 0, if var. = 1
    n                   ! Constrain buoy-buoy baselines
    y                   ! Hold PXP geometry and orientation fixed
    n                   ! Hold PXP baseline lenghts fixed
    n                   ! Hold ellipsoid heights constant
    n                   ! Constrain PXP to move together vertically
    n                   ! Estimate body frame offsets
    n                   ! Compute residuals only
    0                   ! Correction to tabluated acoustic times (sec.)
    +10.300d0           ! Geoid undulation at sea surface point
    x                   ! =0 scale factor, =1 1-D ray trace
    1.d-10              ! xacc tolerance for ray trace bisection
    54.3324d0 -158.4692d0   ! Latitude/Longitude array center (decimal degrees)
    1.d-10              ! Variance (s**2) of PXP two-way travel time measurement
    0.1d0               ! Transponder Wait Time (seconds); ship = 0.0s, WG = 0.1s
    """
    def _load(self,_) -> Tuple[List[Transponder],PositionLLH]:

        if not os.path.exists(self.file):
            raise FileNotFoundError(f"File {self.file} not found")

        loginfo = f"Populating List[Transponder] and Site data from {self.file}"
        logger.info(loginfo)
        transponders = []

        lat_lon_line = re.compile(r"Latitude/Longitude array center")
        non_alphabet = re.compile("[a-c,e-z,A-Z]")
        geoid_undulation_pat = re.compile(r"Geoid undulation at sea surface point")

        with open(self.file,'r') as f:
            lines = f.readlines()[2:]

            for line in lines:
                if not non_alphabet.search(line):
                    line_processed = line.strip().split()
                    # 0    16       54.316094952    -158.461771055    -2096.780      0.200000d0       1479.900
                    id = line_processed[0]
                    lat = float(line_processed[2])
                    lon = float(line_processed[3])
                    height = float(line_processed[4])
                    id = MASTER_STATION_ID[id]
                    # TODO This is not the var
                    offset = float(line_processed[5].replace("d0","")) 
                  
                    position_llh = PositionLLH(latitude=lat, longitude=lon, height=height)
             
                    transponder = Transponder(id=id,position_llh=position_llh,tat_offset=offset)
                    transponders.append(transponder)
                  

                if geoid_undulation_pat.search(line):
                    # "+10.300d0           ! Geoid undulation at sea surface point"
                    geoid_undulation = float(line.split()[0].replace("d0","")) # TODO verify sign
              

                if lat_lon_line.search(line):
                    # 54.3324d0 -158.4692d0   ! Latitude/Longitude array center (decimal degrees)
                    line_processed = [
                        float(x.replace("d0", "")) for x in line.split("!")[0].strip().split()
                    ]
                    lat, lon = line_processed[0], line_processed[1]
                    center_llh = PositionLLH(latitude=lat,longitude=lon,height=geoid_undulation)
                    break


        if not center_llh:
            logger.error("Latitude/Longitude array center not found in masterfile")
            return
        if not transponders:
            logger.error("No transponders found in masterfile")
            return
        if geoid_undulation is None:
            logger.error("Geoid undulation not found in masterfile")
            return

        # Get array center elevation from average of transponder elevations
        #center_llh.height = geoid_undulation
        #center_llh.height = 0.0
        #Correct transponder height values for geoid undulation
        # for transponder in transponders:
        #     transponder.position_llh.height += geoid_undulation
        #     transponder.position_enu.up.value += geoid_undulation

        return (transponders,center_llh)

    def model_post_init(self, __context) -> None:
        self.load_method = self._load


class DFORaw(BaseModel):
    file:str
    wave_glider_delay:float = 0.13

    def load(self) -> Tuple[DataFrame,DataFrame]:
    
        acoustic_data = []
        imu_data = []
        with open(self.file) as f:
            lines = f.readlines()
            for line in lines:
                data = json.loads(line)
                if data.get("event") == "range":
                    range_data = data.get("range",None)
                    if range_data:
                        id:str = range_data.get("cn","").replace("IR","")
                        travel_time:float = range_data.get("range",0) # travel time [s]
                        tat:float = range_data.get("tat",0) # turn around time [ms]
                        travel_time_true = travel_time - tat - self.wave_glider_delay # travel time corrected for turn around time and trigger delay

                        dbv = range_data.get("diag").get("dbv")[0]
                        xc = range_data.get("diag").get("xc")[0]
                    time_data = data.get("time",None)
                    if time_data:
                        trigger_time:float = time_data.get("common",0) # Time since GNSS start [s]
                        trigger_time_dt = datetime.datetime.fromtimestamp(trigger_time)
                        ping_time = trigger_time_dt + datetime.timedelta(seconds=ADJ_LEAP)
                        # Convert to Julian date
                        ping_time_julian = julian.to_jd(ping_time, 'mjd')
                        travel_time_true_fdays  = travel_time_true / 86400.000
                        return_time = ping_time_julian + travel_time_true_fdays

                    acoustic_data.append(
                        {
                           "TriggerTime":trigger_time_dt, "TransponderID":id,"TwoWayTravelTime":travel_time,"ReturnTime":return_time,"DecibalVoltage":dbv,"CorrelationScore":xc
                        }
                           
                        )
                if data.get("event") == "interrogation":
                    heading_data = data.get("observations").get("AHRS")
                    if heading_data:
                        azimuth = heading_data.get("h",None)
                        pitch = heading_data.get("p",None)
                        roll = heading_data.get("r",None)
                        time = heading_data.get("time").get("common")
                        time_dt = datetime.datetime.fromtimestamp(time)
              
                        imu_data_dict = {
                            "Time":time_dt,
                            "Azimuth":azimuth,
                            "Pitch":pitch,
                            "Roll":roll
                        }

                        imu_data.append(imu_data_dict)

        acoustic_df = pd.DataFrame(acoustic_data)
        imu_df = pd.DataFrame(imu_data)

        return acoustic_df,imu_df
