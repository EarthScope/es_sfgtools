import mmap
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, field_serializer, field_validator, root_validator


class BaseObservable(BaseModel):
    """Base class for observable objects.

    Attributes
    ----------
    local_path : Optional[Union[str, Path]]
        The local_path of the data.
    uuid : Optional[int]
        The ID of the object.
    epoch_id : Optional[str]
        The ID of the epoch.
    campaign_id : Optional[str]
        The ID of the campaign.
    timestamp_data_start : Optional[datetime]
        The capture time of the data.
    timestamp_data_end : Optional[datetime]
        The end time of the data.
    data : Optional[mmap.mmap]
        The data object.

    Methods
    -------
    read(path: Union[str, Path])
        Read the data from the local_path.
    write(dir: Union[str, Path])
        Write the data to the local_path.

    Notes
    -----
    This class is intended to be subclassed by other classes.
    read/write methods are used to interface between temporary files and the
    data object.
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
        """Read the data from the local_path.

        Parameters
        ----------
        path : Union[str, Path]
            The path to the data file.
        """
        with open(path, "r+b") as f:
            self.data = mmap.mmap(f.fileno(), 0)
        self.local_path = path

    def write(self, dir: Union[str, Path]):
        """Write the data to the local_path.

        Parameters
        ----------
        dir : Union[str, Path]
            The directory to write the data file to.
        """

        path = Path(dir) / Path(self.local_path).name
        with open(path, "w+b") as f:
            f.write(self.data)
        self.local_path = path

class AssetType(Enum):
    NOVATEL = "novatel"
    NOVATEL770 = "novatel770"
    NOVATEL000 = "novatel000"
    DFOP00 = "dfop00"
    SONARDYNE = "sonardyne"
    RINEX = "rinex"
    KIN = "kin"
    SEABIRD = "seabird"
    CTD = "ctd"             # 2 column data
    LEVERARM = "leverarm"
    MASTER = "master"
    QCPIN = "qcpin"
    NOVATELPIN = "novatelpin"
    KINPOSITION = "kinposition"
    ACOUSTIC = "acoustic"
    SITECONFIG = "siteconfig"
    ATDOFFSET = "atdoffset"
    SVP = "svp"             # maybe doesn't work
    SHOTDATA = "shotdata"
    IMUPOSITION = "imuposition"
    KINRESIDUALS = "kinresiduals"
    GNSSOBSTDB = "GNSSOBSTDB"

    _ = "default"

class _AssetBase(BaseModel):
    local_path: Optional[Union[str, Path]] = Field(default=None)
    remote_path: Optional[str] = Field(default=None)
    remote_type: Optional[str] = Field(default=None)
    type: Optional[AssetType] = Field(default=None)
    id: Optional[int] = Field(default=None)
    network: Optional[str] = Field(default=None)
    station: Optional[str] = Field(default=None)
    campaign: Optional[str] = Field(default=None)
    is_processed: Optional[bool] = Field(default=False)

    timestamp_data_start: Optional[datetime] = Field(default=None)
    timestamp_data_end: Optional[datetime] = Field(default=None)
    timestamp_created: Optional[datetime] = Field(default=None)

    # @field_serializer("timestamp_data_start","timestamp_data_end","timestamp_created",when_used="always")
    # def serialize_timestamp_data_start(self, v: Optional[datetime]):
    #     if v is not None:
    #         return v.isoformat()
    #     return v
   
    # @field_validator("timestamp_data_start","timestamp_data_end","timestamp_created",mode="before")
    # def _validate_timestamp(cls,v:Optional[str]) 

    @field_validator("type", mode="before")
    def _check_type(cls, type_value: Union[str, AssetType]):
        if isinstance(type_value, str):
            type_value = AssetType(type_value)
        return type_value

    @field_serializer("type", when_used="always")
    def _serialize_type(self, type_value: Union[str, AssetType]):
        if isinstance(type_value, AssetType):
            return type_value.value
        return type_value
    
    @root_validator(pre=True)
    def _check_at_least_one(cls, values):
        if not values.get("local_path") and not values.get("remote_path"):
            raise ValueError("At least one of the following must be set: local_path, remote_path")
        
        if isinstance(values.get("local_path"), str):
            values["local_path"] = Path(values["local_path"])

        return values

    @field_serializer("local_path", when_used="always")
    def _serialize_local_path(self, local_path_value: Union[str, Path]):
        if isinstance(local_path_value, Path):
            return str(local_path_value)
        return local_path_value

    class Config:
        arbitrary_types_allowed = True


class AssetEntry(_AssetBase):
    parent_id: Optional[int] = Field(default=None)

    def to_update_dict(self) -> Dict[str, Any]:
        # Drop the id  
        model_dict = self.model_dump()
        model_dict.pop("id")
        return model_dict


class NovatelFile(BaseObservable):
    """Represents a Novatel file from SV2.

    This class provides methods and attributes to handle Novatel files in the
    SeaFloorGeodesy project. Used to get acoustic/IMU data from Novatel
    files.

    Processing Functions
    --------------------
    src.processing.functions.gnss_functions.novatel_to_rinex
    src.processing.functions.imu_functions.novatel_to_imudf
    """
    name:str = "novatel"


class Novatel770File(BaseObservable):
    """Represents a Novatel700 file from SV3.

    This class provides methods and attributes to handle Novatel files in the
    SeaFloorGeodesy project. Used to get acoustic/IMU data from Novatel
    files.

    Processing Functions
    --------------------
    src.processing.functions.gnss_functions.novatel700_to_rinex
    """
    name:str = "novatel770"


class DFPO00RawFile(BaseObservable):
    """Represents a DFOP00.raw file.

    The DFOP00.raw file contains the real time amalgamation of all sensors
    using the common JSON style format. For each range cycle there are
    multiple JSON entries created. The first entry is the “Interrogate”
    entry which contains the GNSS, AHRS, INS & TIME data from when the
    acoustic signal is transmitted.

    Processing Functions
    --------------------
    src.processing.functions.acoustic_functions.dfpo00_to_imudf
    src.processing.functions.acoustic_functions.dfpo00_to_acousticdf
    """
    name:str = "dfpo00"


class SonardyneFile(BaseObservable):
    """Represents a Sonardyne file.

    Processing Functions
    --------------------
    src.processing.functions.acoustic_functions.sonardyne_to_acousticdf
    """
    name:str = "sonardyne"


# class RinexFile(BaseObservable):
#     """
#     Represents a RINEX file.

#     Processing Functions:
#         src.processing.functions.gnss_functions.rinex_to_kin

#     Attributes:
#         parent_id (Optional[str]): The ID of the parent file, if any.
#     """
#     name:str = "rinex"
#     parent_uuid: Optional[str] = None

#     site: Optional[str] = None
#     basename: Optional[str] = None

# class ConstellationField(BaseModel):
#     system:str = Field(...,description="Constellation")
#     obstypes:str

#     @model_serializer(when_used='json')
#     def _serialize(self):
#         template = f"{self.system}    {self.obstypes}                      SYS / # / OBS TYPES"
#         return template

# class RinexHeader(BaseModel):
#     data:Dict[str,str] = Field(...,description="RINEX header data")
#     time_of_first_obs:datetime = Field(...,description="Time of first observation")
#     time_of_last_obs:datetime = Field(...,description="Time of last observation")

#     @root_validator(pre=True)
#     def _check_timespan(cls,values):
#         tofo = cls._get_time(values["data"]["TIME OF FIRST OBS"])
#         tolo = cls._get_time(values["data"]["TIME OF LAST OBS"])
#         values["time_of_first_obs"] = tofo
#         values["time_of_last_obs"] = tolo
#         return values

#     @staticmethod
#     def _get_time(line):
#         time_values = line.split("GPS")[0].strip().split()
#         start_time = datetime(
#             year=int(time_values[0]),
#             month=int(time_values[1]),
#             day=int(time_values[2]),
#             hour=int(time_values[3]),
#             minute=int(time_values[4]),
#             second=int(float(time_values[5])),
#         )
#         return start_time


#     @classmethod
#     def from_file(cls, filepath: str) -> "RinexHeader":
#         """Reads header data from a RINEX ASCII file."""
#         with open(filepath, "r") as f:
#             lines = f.readlines()

#         # Initialize attributes from lines
#         header_data = {}

#         for line in lines:
#             if "RINEX VERSION / TYPE" in line:
#                 header_data["version"] = line[0:9].strip()
#                 header_data["file_type"] = line[20:40].strip()
#                 header_data["satellite_system"] = line[40:60].strip()

#             elif "PGM / RUN BY / DATE" in line:
#                 header_data["program"] = line[0:20].strip()
#                 header_data["run_by"] = line[20:40].strip()
#                 header_data["date"] = line[40:60].strip()

#             elif "MARKER NAME" in line:
#                 header_data["marker_name"] = line[0:60].strip()

#             elif "MARKER NUMBER" in line:
#                 header_data["marker_number"] = line[0:60].strip()

#             elif "OBSERVER / AGENCY" in line:
#                 header_data["observer"] = line[0:20].strip()
#                 header_data["agency"] = line[20:60].strip()

#             elif "REC # / TYPE / VERS" in line:
#                 header_data["receiver_type"] = line[20:40].strip()
#                 header_data["receiver_version"] = line[40:60].strip()

#             elif "ANT # / TYPE" in line:
#                 header_data["antenna_type"] = line[40:60].strip()

#             elif "APPROX POSITION XYZ" in line:
#                 header_data["approx_position_xyz"] = list(
#                     map(float, line.split("APPROX")[0].strip().split())
#                 )

#             elif "ANTENNA: DELTA H/E/N" in line:
#                 header_data["antenna_delta_hen"] = list(map(float, line.split("ANTENNA")[0].strip().split()))

#             elif "SYS / # / OBS TYPES" in line:
#                 header_data.setdefault("sys_obs_types", []).append(line[0:60].strip())

#             elif "SIGNAL STRENGTH UNIT" in line:
#                 header_data["signal_strength_unit"] = line[0:60].strip()

#             elif "INTERVAL" in line:
#                 header_data["interval"] = float(line[0:60].strip())

#             elif "TIME OF FIRST OBS" in line:
#                 header_data["time_of_first_obs"] = line[0:60].strip()

#                 start_time = cls._get_time(line)
#                 header_data["timestamp_data_start"] = start_time

#             elif "TIME OF LAST OBS" in line:
#                 header_data["time_of_last_obs"] = line[0:60].strip()
#                 end_time = cls._get_time(line)
#                 header_data["timestamp_data_end"] = end_time

#             elif "SYS / PHASE SHIFT" in line:
#                 header_data.setdefault("phase_shifts", []).append(line[0:60].strip())

#             elif "GLONASS SLOT / FRQ #" in line:
#                 header_data.setdefault("glonass_slot_frq", []).append(
#                     line[0:60].strip()
#                 )

#             elif "LEAP SECONDS" in line:
#                 header_data["leap_seconds"] = int(line[0:60].strip()[0])

#             elif "END OF HEADER" in line:
#                 break

#         return cls(**header_data)

#     def to_file(self, filepath: str):
#         """Writes the header data to a RINEX ASCII file."""
#         with open(filepath, "w") as f:
#             # Write formatted header lines
#             f.write(
#                 f"{self.version:<9}       {self.file_type:<20}{self.satellite_system:<20}RINEX VERSION / TYPE\n"
#             )
#             f.write(
#                 f"{self.program:<20}{self.run_by:<20}{self.date:<20}PGM / RUN BY / DATE\n"
#             )
#             if self.marker_name:
#                 f.write(f"{self.marker_name:<60}MARKER NAME\n")
#             if self.marker_number:
#                 f.write(f"{self.marker_number:<60}MARKER NUMBER\n")
#             if self.observer and self.agency:
#                 f.write(f"{self.observer:<20}{self.agency:<40}OBSERVER / AGENCY\n")
#             if self.receiver_type and self.receiver_version:
#                 f.write(
#                     f"{'':<20}{self.receiver_type:<20}{self.receiver_version:<20}REC # / TYPE / VERS\n"
#                 )
#             if self.antenna_type:
#                 f.write(f"{'':<40}{self.antenna_type:<20}ANT # / TYPE\n")
#             if self.approx_position_xyz:
#                 f.write(
#                     f"{self.approx_position_xyz[0]:>14.4f} {self.approx_position_xyz[1]:>14.4f} {self.approx_position_xyz[2]:>14.4f}     APPROX POSITION XYZ\n"
#                 )
#             if self.antenna_delta_hen:
#                 f.write(
#                     f"{self.antenna_delta_hen[0]:>14.4f} {self.antenna_delta_hen[1]:>14.4f} {self.antenna_delta_hen[2]:>14.4f}     ANTENNA: DELTA H/E/N\n"
#                 )
#             for obs_type in self.sys_obs_types:
#                 f.write(f"{obs_type:<60}SYS / # / OBS TYPES\n")
#             if self.signal_strength_unit:
#                 f.write(f"{self.signal_strength_unit:<60}SIGNAL STRENGTH UNIT\n")
#             if self.interval:
#                 f.write(f"{self.interval:<60.3f}INTERVAL\n")
#             f.write(f"{self.time_of_first_obs:<60}TIME OF FIRST OBS\n")
#             if self.time_of_last_obs:
#                 f.write(f"{self.time_of_last_obs:<60}TIME OF LAST OBS\n")
#             for phase_shift in self.phase_shifts:
#                 f.write(f"{phase_shift:<60}SYS / PHASE SHIFT\n")
#             for glonass in self.glonass_slot_frq:
#                 f.write(f"{glonass:<60}GLONASS SLOT / FRQ #\n")
#             if self.leap_seconds is not None:
#                 f.write(f"{self.leap_seconds:<60}LEAP SECONDS\n")
#             f.write(f"{''<60}END OF HEADER\n")


#     def make_template(self)->str:
#         template = f"""
#     {self.version}            OBSERVATION DATA    M (MIXED)           RINEX VERSION / TYPE
#     NAS Convert 1.13.0  NovAtel             20231109 004337 UTC PGM / RUN BY / DATE
#     {self.approx_position_xyz[0]}  {self.approx_position_xyz[1]}   {self.approx_position_xyz[2]}     APPROX POSITION XYZ
#             {self.antenna_delta_hen[0]}         {self.antenna_delta_hen[1]}         {self.antenna_delta_hen[2]}     ANTENNA: DELTA H/E/N
#     G    8 C1C L1C D1C S1C C2W L2W D2W S2W                      SYS / # / OBS TYPES
#     R    8 C1C L1C D1C S1C C2P L2P D2P S2P                      SYS / # / OBS TYPES
#     J    8 C1C L1C D1C S1C C2S L2S D2S S2S                      SYS / # / OBS TYPES
#     DBHZ                                                        SIGNAL STRENGTH UNIT
#     0.100                                                       INTERVAL
#     {self.time_of_first_obs.year()}    {self.time_of_first_obs.month()}    {self.time_of_first_obs.day()}    {self.time_of_first_obs.hour()}    {self.time_of_first_obs.minute()}   {self.time_of_first_obs.second()}     GPS           TIME OF FIRST OBS
#     {self.time_of_last_obs.year()}    {self.time_of_last_obs.month()}    {self.time_of_last_obs.day()}    {self.time_of_last_obs.hour()}    {self.time_of_last_obs.minute()}   {self.time_of_last_obs.second()}     GPS           TIME OF LAST OBS
#     G L1C  0.00000  17 G25 G06 G20 G29 G24 G12 G02 G19 G05 G31  SYS / PHASE SHIFT
#     G18 G26 G23 G15 G13 G16 G10                                 SYS / PHASE SHIFT
#     G L2W  0.00000  17 G25 G06 G20 G29 G24 G12 G02 G19 G05 G31  SYS / PHASE SHIFT
#     G18 G26 G23 G15 G13 G16 G10                                 SYS / PHASE SHIFT
#     R L1C  0.00000  15 R21 R03 R19 R10 R20 R09 R04 R12 R05 R07  SYS / PHASE SHIFT
#     R23 R06 R22 R08 R14                                         SYS / PHASE SHIFT
#     R L2P  0.25000  12 R21 R03 R19 R20 R09 R04 R12 R05 R07 R22  SYS / PHASE SHIFT
#     R08 R14                                                     SYS / PHASE SHIFT
#     J L1C  0.00000  01 J02                                      SYS / PHASE SHIFT
#     J L2S  0.00000  01 J02                                      SYS / PHASE SHIFT
#     15 R21  4 R03  5 R19  3 R10 -7 R20  2 R09 -2 R04  6 R12 -1  GLONASS SLOT / FRQ #
#     R05  1 R07  5 R23  3 R06 -4 R22 -3 R08  6 R14 -7            GLONASS SLOT / FRQ #
#     0                                                           LEAP SECONDS
#                                                             END OF HEADER
#         """
#         return template

#     def merge(self, other: "RinexHeader") -> "RinexHeader":
#         """Merges another RinexHeader with this one, ensuring compatible fields."""
#         if (
#             self.version != other.version
#             or self.file_type != other.file_type
#             or self.receiver_type != other.receiver_type
#             or self.receiver_version != other.receiver_version
#             or self.satellite_system != other.satellite_system
#             or self.program != other.program
#             or self.observer != other.observer
#         ):
#             raise ValueError(
#                 "Cannot merge headers with different version, file type, or receiver information"
#             )

#         # Merge the timespan
#         start_time = min(self.timestamp_data_start, other.timestamp_data_start)
#         end_time = max(self.timestamp_data_end, other.timestamp_data_end)

#         # Merge observation types
#         sys_obs_types = list(set(self.sys_obs_types + other.sys_obs_types))
#         phase_shifts = list(set(self.phase_shifts + other.phase_shifts))
#         glonass_slot_frq = list(set(self.glonass_slot_frq + other.glonass_slot_frq))

#         return RinexHeader(
#             version=self.version,
#             file_type=self.file_type,
#             satellite_system=self.satellite_system,
#             program=self.program,
#             run_by=self.run_by,
#             date=self.date,
#             marker_name=self.marker_name ,
#             marker_number=self.marker_number,
#             observer=self.observer,
#             agency=self.agency,
#             receiver_type=self.receiver_type,
#             receiver_version=self.receiver_version,
#             antenna_type=self.antenna_type,
#             approx_position_xyz=self.approx_position_xyz,
#             antenna_delta_hen=self.antenna_delta_hen,
#             sys_obs_types=sys_obs_types,
#             signal_strength_unit=self.signal_strength_unit,
#             interval=self.interval,
#             time_of_first_obs=self.time_of_first_obs,
#             time_of_last_obs=other.time_of_last_obs,
#             timestamp_data_start=start_time,
#             timestamp_data_end=end_time,
#             phase_shifts=phase_shifts,
#             glonass_slot_frq=glonass_slot_frq,
#             leap_seconds=self.leap_seconds,
#         )


def make_template(version:float,x:float,y:float,z:float,
                  time_of_first_obs:datetime,
                  time_of_last_obs:datetime)->str:
    template = f"""
{version}            OBSERVATION DATA    M (MIXED)           RINEX VERSION / TYPE
NAS Convert 1.13.0  NovAtel             20231109 004337 UTC PGM / RUN BY / DATE
{x}  {y}   {z}     APPROX POSITION XYZ
        0.0000         0.0000         0.0000     ANTENNA: DELTA H/E/N
G    8 C1C L1C D1C S1C C2W L2W D2W S2W                      SYS / # / OBS TYPES
R    8 C1C L1C D1C S1C C2P L2P D2P S2P                      SYS / # / OBS TYPES
J    8 C1C L1C D1C S1C C2S L2S D2S S2S                      SYS / # / OBS TYPES
DBHZ                                                        SIGNAL STRENGTH UNIT
0.100                                                       INTERVAL
{time_of_first_obs.year()}    {time_of_first_obs.month()}    {time_of_first_obs.day()}    {time_of_first_obs.hour()}    {time_of_first_obs.minute()}   {time_of_first_obs.second()}     GPS           TIME OF FIRST OBS
{time_of_last_obs.year()}    {time_of_last_obs.month()}    {time_of_last_obs.day()}    {time_of_last_obs.hour()}    {time_of_last_obs.minute()}   {time_of_last_obs.second()}     GPS           TIME OF LAST OBS
G L1C  0.00000  17 G25 G06 G20 G29 G24 G12 G02 G19 G05 G31  SYS / PHASE SHIFT
G18 G26 G23 G15 G13 G16 G10                                 SYS / PHASE SHIFT
G L2W  0.00000  17 G25 G06 G20 G29 G24 G12 G02 G19 G05 G31  SYS / PHASE SHIFT
G18 G26 G23 G15 G13 G16 G10                                 SYS / PHASE SHIFT
R L1C  0.00000  15 R21 R03 R19 R10 R20 R09 R04 R12 R05 R07  SYS / PHASE SHIFT
R23 R06 R22 R08 R14                                         SYS / PHASE SHIFT
R L2P  0.25000  12 R21 R03 R19 R20 R09 R04 R12 R05 R07 R22  SYS / PHASE SHIFT
R08 R14                                                     SYS / PHASE SHIFT
J L1C  0.00000  01 J02                                      SYS / PHASE SHIFT
J L2S  0.00000  01 J02                                      SYS / PHASE SHIFT
15 R21  4 R03  5 R19  3 R10 -7 R20  2 R09 -2 R04  6 R12 -1  GLONASS SLOT / FRQ #
R05  1 R07  5 R23  3 R06 -4 R22 -3 R08  6 R14 -7            GLONASS SLOT / FRQ #
0                                                           LEAP SECONDS
                                                             END OF HEADER
    """
    return template

class SatelliteData(BaseModel):
    satellite_id: str = Field(
        ..., description="Satellite identifier (e.g., G09, G31, R12)"
    )
    pseudorange: float = Field(..., description="Pseudorange (m)")
    carrier_phase: float = Field(..., description="Carrier phase (cycles)")
    doppler_shift: Optional[float] = Field(None, description="Doppler shift (Hz)")
    signal_strength: Optional[float] = Field(None, description="Signal strength (dBHz)")


class RinexLog(BaseModel):
    timestamp: datetime = Field(..., description="Timestamp of the log entry")
    num_satellites: int = Field(..., description="Number of satellites in the entry")
    satellite_data: List[SatelliteData] = Field(
        ..., description="List of satellite data associated with the timestamp"
    )

    @classmethod
    def from_file(cls,path:str) -> List["RinexLog"]:
        logs = []
        with open(path,"r") as f:
            for line in f:
                if line.startswith(">"):
                    logs.append(
                        cls.from_log_entry(line)
                    )

        return logs

    @staticmethod
    def from_log_entry(log_entry: str) -> "RinexLog":
        lines = log_entry.strip().split("\n")

        # Extract the timestamp from the first line
        first_line = lines[0]
        timestamp_str = first_line[1:27].strip()
        timestamp = datetime.strptime(timestamp_str, "%Y %m %d %H %M %S.%f")

        # Number of satellites
        num_satellites = int(first_line[28:30].strip())

        # Parse satellite data from the following lines
        satellite_data = []
        for line in lines[1:]:
            satellite_id = line[:4].strip()
            pseudorange = float(line[5:23].strip())
            carrier_phase = float(line[24:42].strip())
            doppler_shift = float(line[43:57].strip())
            signal_strength = float(line[58:65].strip())
            satellite_data.append(
                SatelliteData(
                    satellite_id=satellite_id,
                    pseudorange=pseudorange,
                    carrier_phase=carrier_phase,
                    doppler_shift=doppler_shift,
                    signal_strength=signal_strength,
                )
            )

        return RinexLog(
            timestamp=timestamp,
            num_satellites=num_satellites,
            satellite_data=satellite_data,
        )

    def to_str(self) -> str:
        """Converts the log entry to a string."""
        lines = []
        lines.append(f"> {self.timestamp.strftime('%Y %m %d %H %M %S.%f')} {self.num_satellites}")
        for satellite in self.satellite_data:
            lines.append(
                f"{satellite.satellite_id:<4} {satellite.pseudorange:>18.3f} {satellite.carrier_phase:>18.3f} {satellite.doppler_shift:>14.3f} {satellite.signal_strength:>7.3f}"
            )
        return "\n".join(lines)

class RinexFileV3(BaseModel):
    header: RinexHeader = Field(..., description="RINEX header information")
    logs: List[RinexLog] = Field(..., description="List of RINEX log entries")

    @classmethod
    def from_file(cls, filepath: str) -> "RinexFileV3":
        header = RinexHeader.from_file(filepath)
        logs = RinexLog.from_file(filepath)
        return cls(header=header, logs=logs)

    def to_file(self, filepath: str):
        """Writes the RINEX file to a file."""
        with open(filepath, "w") as f:
            f.write(self.header.to_str() + "\n")
            for log in self.logs:
                f.write(log.to_str() + "\n")

    def merge(self, other: "RinexFileV3") -> "RinexFileV3":
        """Merges another RINEX file with this one."""
        header = self.header.merge(other.header)
        logs = self.logs + other.logs
        logs = sorted(logs, key=lambda x: x.timestamp)
        return RinexFileV3(header=header, logs=logs)

# Example usage:
# rinex_header = RinexHeader.from_file('example_rinex.obs')
# rinex_header.to_file('output_rinex_header.obs')