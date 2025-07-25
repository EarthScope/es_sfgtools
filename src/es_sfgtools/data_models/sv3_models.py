"""
Author: Franklyn Dunbar Email: franklyn.dunbar@earthscope.org

this file contains data models for parsing sonardyne sv3 data
"""

# External imports
from pydantic import BaseModel, Field,field_validator
from typing import Optional
from enum import Enum
from typing import Union,List

# Local imports
from .constants import Decimal,GNSS_START_TIME

class SV3GPSQuality(Enum):
    FIX_NOT_AVAILABLE = 0  # Fix not available
    SINGLE_POINT = 1  # Single Point
    PSEUDO_RANGE_DIFFERENTIAL = 2  # Pseudo-Range Differential
    REAL_TIME_KINEMATIC = 4  # Real-Time Kinematic
    FLOAT_RTK = 5  # Float RTK
    DEAD_RECKONING = 6  # Dead Reckoning
    MANUAL_INPUT_MODE = 7  # Manual Input Mode
    SIMULATION_MODE = 8  # Simulation Mode
    WAAS_SBAS = 9  # WAAS (SBAS)

class NovatelSolutionStatus(Enum):
    SOL_COMPUTED = 0  # Solution computed
    INSUFFICIENT_OBS = 1  # Insufficient observations
    NO_CONVERGENCE = 2  # No convergence
    SINGULARITY = 3  # Singularity at parameters matrix
    COV_TRACE = 4  # Covariance trace exceeds maximum (trace > 1000 m)
    TEST_DIST = 5  # Test distance exceeded (max 3 rejections if distance > 10 km)
    COLD_START = 6  # Not yet converged from cold start
    V_H_LIMIT = 7  # Height or velocity limits exceeded (export licensing restrictions)
    VARIANCE = 8  # Variance exceeds limits
    RESIDUALS = 9  # Residuals are too large
    # 10-12 Reserved
    INTEGRITY_WARNING = 13  # Large residuals make position unreliable
    # 14-17 Reserved
    PENDING = (
        18  # FIX position command entered, receiver is computing/validating position
    )
    INVALID_FIX = 19  # The fixed position (FIX POSITION command) is not valid
    UNAUTHORIZED = 20  # Position type is unauthorized
    # 21 Reserved
    INVALID_RATE = 22  # Selected logging rate not supported for this solution type

class NovatelPositionType(Enum):
    NONE = 0                # No solution
    FIXEDPOS = 1            # Position fixed by FIX position command or averaging
    FIXEDHEIGHT = 2         # Position fixed by FIX height or FIX auto command or averaging
    DOPPLER_VELOCITY = 8    # Velocity computed using instantaneous Doppler
    SINGLE = 16             # Single point position
    PSRDIFF = 17            # Pseudorange differential solution
    WAAS = 18               # Solution calculated using corrections from SBAS
    PROPAGATED = 19         # Propagated by a Kalman filter without new observations
    FLOAT_L1 = 32           # Floating L1 ambiguity solution
    NARROW_FLOAT = 34       # Floating narrow-lane ambiguity solution
    L1_INT = 48             # Integer L1 ambiguity solution
    WIDE_INT = 49           # Integer wide-lane ambiguity solution
    NARROW_INT = 50         # Integer narrow-lane ambiguity solution
    RTK_DIRECT_INS = 51     # RTK status from INS filter
    INS_SBAS = 52           # INS calculated position corrected for antenna
    INS_PSRSP = 53          # INS pseudorange single point solution (no DGPS)
    INS_PSRDIFF = 54        # INS pseudorange differential solution
    INS_RTKFLOAT = 55       # INS RTK floating point ambiguities solution
    INS_RTKFIXED = 56       # INS RTK fixed ambiguities solution
    PPP_CONVERGING = 68     # Converging TerraStar-C, TerraStar-C PRO or TerraStar-X solution
    PPP = 69                # Converged TerraStar-C, TerraStar-C PRO or TerraStar-X solution
    OPERATIONAL = 70        # Solution accuracy is within UAL operational limit
    WARNING = 71            # Solution accuracy is outside UAL operational limit but within warning limit
    OUT_OF_BOUNDS = 72      # Solution accuracy is outside UAL limits
    INS_PPP_CONVERGING = 73 # INS with TerraStar-C/PRO/X PPP solution converging
    INS_PPP = 74            # INS with TerraStar-C/PRO/X PPP solution
    PPP_BASIC_CONVERGING = 77   # Converging TerraStar-L solution
    PPP_BASIC = 78              # Converged TerraStar-L solution
    INS_PPP_BASIC_CONVERGING = 79   # INS with TerraStar-L PPP basic solution converging
    INS_PPP_BASIC = 80              # INS with TerraStar-L PPP basic solution

class TimeData(BaseModel):
    common: Decimal = Field(
        description="TZ unaware UNIX time",
        ge=GNSS_START_TIME.timestamp(),
    )
    instrument: Decimal = Field(
        description="Instrument time in seconds",
        ge=0
    )
    start_count: int = Field(
        description="Start count for the time",
        ge=0,
    )
    status: str = Field(
        description="Status of the time data",
      
    )

class NovatelHeadingData(BaseModel):
    gpst: Decimal = Field(
        description="GPS time in seconds since GNSS start time",
        ge=0,
    )
    h: Decimal = Field(
        description="GNSS Computed Heading in degrees",
        ge=-180,
        le=180,
    )
    p: Decimal = Field(
        description="GNSS Computed Pitch in degrees",
        ge=-90,
        le=90,
    )
    position_type: NovatelPositionType = Field(
        description="Type of position data"
    )
    receiver_status:str = Field(
        description="Status of the receiver",
    )
    sdh: Optional[Decimal] = Field(
        description="Standard deviation of heading in degrees",
        ge=0,
    )
    sdp: Optional[Decimal] = Field(
        description="Standard deviation of pitch in degrees",
        ge=0,
    )
    solution_type: NovatelSolutionStatus = Field(
        description="Solution type of the heading data",
    )
    sv_used: int = Field(
        description="Number of satellites used in the solution",
        ge=0
    )
    sv_visible: int = Field(
        description="Number of satellites visible",
        ge=0,
        alias="sv_visable",
    )
    time: TimeData = Field(
        description="Time data associated with the log",
    )

class NovatelINSData(BaseModel):
    gpst: Decimal = Field(
        description="GPS time in seconds since GNSS start time",
        ge=0,
    )
    h: Decimal = Field(
        description="SPAN INS Computed Heading in degrees",
        ge=-180,
        le=180,
    )
    p: Decimal = Field(
        description="SPAN INS Computed Pitch in degrees",
        ge=-90,
        le=90,
    )
    r: Decimal = Field(
        description="SPAN INS Computed Roll in degrees",
        ge=-180,
        le=180,
    )
    receiver_status: str = Field(
        description="Status of the receiver",
    )
    solution_type: NovatelSolutionStatus = Field(
        description="Solution type of the INS data",
    )
    time: TimeData = Field(
        description="Time data associated with the log",
    )
    velx: Decimal = Field(
        description="SPAN INS measured acceleration X axis in m/s^2",
        alias="vx",
    )
    vely: Decimal = Field(
        description="SPAN INS measured acceleration Y axis in m/s^2",
        alias="vy",
    )
    velz: Decimal = Field(
        description="SPAN INS measured acceleration Z axis in m/s^2",
        alias="vz",
    )

class NovatelRangeData(BaseModel):
    raw:str = Field(
        description="Raw range data as a string",
    )
    time: TimeData = Field(
        description="Time data associated with the range data",
    )

class NovatelGNSSData(BaseModel):
    hae: Decimal = Field(
        description="Height above ellipsoid in meters",
        ge=-1000,
        le=1000,
    )
    latitude: Decimal = Field(
        description="Latitude in degrees",
        ge=-90,
        le=90,
    )
    longitude: Decimal = Field(
        description="Longitude in degrees",
        ge=-180,
        le=180,
    )
    q: SV3GPSQuality = Field(
        description="Quality indicator",
    )

    sdx: Optional[Decimal] = Field(
        description="Standard deviation in east direction in meters",
        ge=0,
    )
    sdy: Optional[Decimal] = Field(
        description="Standard deviation in north direction in meters",
        ge=0,
    )
    sdz: Optional[Decimal] = Field(
        description="Standard deviation in up direction in meters",
        ge=0,
    )
    separation: Optional[Decimal] = Field(
        description="Separation"
    )
    time: TimeData = Field(
        description="Time data associated with the log",
    )

class NovatelAHRSData(BaseModel):
    acx: Decimal = Field(
        description="Acceleration X axis in m/s^2",
    )
    acy: Decimal = Field(
        description="Acceleration Y axis in m/s^2",
    )
    acz: Decimal = Field(
        description="Acceleration Z axis in m/s^2",
    )
    h: Decimal = Field(
        description="Heading in degrees",
        ge=Decimal(0),
        le= Decimal(360),
    )
    h_mag: Optional[Decimal] = Field(
        description="Magnetic heading in degrees",
        ge=Decimal(0),
        le=Decimal(360),
    )
    p: Decimal = Field(
        description="Pitch in degrees",
        ge=Decimal(-90),
        le=Decimal(90),
    )
    r: Decimal = Field(
        description="Roll in degrees",
        ge=Decimal(-180),
        le=Decimal(180),
    )
    time: TimeData = Field(
        description="Time data associated with the log",
    )

class NovatelRangeDiagnosticData(BaseModel):
    dbv: Decimal = Field(
        description="Decibel voltage in volts",
    )
    snr: Decimal = Field(
        description="Signal-to-noise ratio in dB",
    )
    
    xc: Decimal = Field(
        description="Cross-correlation % - Signal quality",
    )
    @field_validator("dbv", "snr", "xc", mode="before")
    def convert_from_list(cls,value:List[Union[int, float]]):
        if isinstance(value, list) and len(value) == 1:
            return Decimal(value[0])
        return Decimal(value)

class NovatelRangeReplyData(BaseModel):
    cn:str = Field(
    description="transponder ID",
    max_length=20
    )
    diag:NovatelRangeDiagnosticData = Field(
        description="Range diagnostic data",
    )
    range: Decimal = Field(
        description="Two way travel time in seconds, including beacons turn around time",
    )
    tat: Decimal = Field(
        description="Beacon turn around time in milli-seconds",
        ge=0,
    )

class NovatelObservations(BaseModel):
    AHRS: Optional[NovatelAHRSData] = Field(
        description="AHRS data",
    )
    GNSS: Optional[NovatelGNSSData] = Field(
        description="GNSS data",
    )
    NOV_HEADING: Optional[NovatelHeadingData] = Field(
        description="Novatel heading data",
    )
    NOV_INS: Optional[NovatelINSData] = Field(
        description="Novatel INS data",
    )
    NOV_RANGE: Optional[NovatelRangeData] = Field(
        description="Novatel range data",
    )

class NovatelRangeEvent(BaseModel):
    event:str = "range"
    event_id:int = Field(
        description="ID of the tracking cycle being used",
        ge=0,
    )
    observations: NovatelObservations = Field(
        description="Observations associated with the range event",
    )
    range: NovatelRangeReplyData = Field(
        description="Range reply data",
    )
    sequence: int = Field(
        description="Sequence ID of the tracking cycle",
        ge=0,
    )
    time: TimeData = Field(
        description="Time data associated with the range event",
    )
    uid: Optional[str] = Field(
        description="Unique identifier for the range event",
        max_length=50,
    )

class NovatelInterrogationEvent(BaseModel):
    event: str = "interrogation"
    event_id: int = Field(
        description="ID of the tracking cycle being used",
        ge=0,
    )
    observations: NovatelObservations = Field(
        description="Observations associated with the interrogation",
    )
    sequence: int = Field(
        description="Sequence ID of the tracking cycle",
        ge=0,
    )
    time: TimeData = Field(
        description="Time data associated with the interrogation",
    )
    type: str = Field(
        description="Interrogation type",
    )
