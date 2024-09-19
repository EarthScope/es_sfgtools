import pandera as pa
from pandera.typing import Series, DataFrame
from pandera.errors import SchemaErrors
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator,field_serializer,field_validator,ValidationError
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

from ...processing.schemas.observables import SoundVelocityDataFrame
from ...processing.schemas.site_config import ATDOffset, PositionENU, PositionLLH, Transponder

from garpos import LIB_DIRECTORY,LIB_RAYTRACE

logger = logging.getLogger(__name__)
class ObservationData(pa.DataFrameModel):
    """Observation data file schema

    Example data:

    ,SET,LN,MT,TT,ResiTT,TakeOff,gamma,flag,ST,ant_e0,ant_n0,ant_u0,head0,pitch0,roll0,RT,ant_e1,ant_n1,ant_u1,head1,pitch1,roll1
    0,S01,L01,M11,2.289306,0.0,0.0,0.0,False,30072.395125,-27.85291,1473.14423,14.73469,176.47,0.59,-1.39,30075.74594,-26.70998,1462.01803,14.32703,177.07,-0.5,-1.1
    1,S01,L01,M13,3.12669,0.0,0.0,0.0,False,30092.395725,-22.08296,1412.88729,14.59827,188.24,0.41,-2.13,30096.58392,-22.3514,1401.77938,14.65401,190.61,-0.1,-2.14
    2,S01,L01,M14,2.702555,0.0,0.0,0.0,False,30093.48579,-22.25377,1409.87685,14.67772,188.93,0.15,-1.7,30097.24985,-22.38458,1399.96509,14.55534,190.82,-0.39,-2.21
    3,S01,L01,M14,2.68107,0.0,0.0,0.0,False,30102.396135,-23.25514,1387.38992,14.75355,192.39,0.1,-1.79,30106.13871,-23.96613,1378.4627,14.58135,192.92,0.21,-1.7
    4,S01,L01,M11,2.218846,0.0,0.0,0.0,False,30103.4862,-23.57701,1384.73242,14.65861,192.62,-0.14,-1.5,30106.766555,-24.0478,1377.09283,14.68464,193.04,0.59,-1.81
    """

    SET: Series[str] = pa.Field(
        description="Set name", default="S01"
    )
    LN: Series[str] = pa.Field(
        description="Line name", default="L01"
    )
    MT: Series[str] = pa.Field(
        description="Station name", coerce=True
    )

    TT: Series[float] = pa.Field(description="Travel time [sec]")

    ST: Series[float] = pa.Field(
        description="Time of transmission of the acoustic signal in MJD [s]"
    )

    RT: Series[float] = pa.Field(
        description="Time of reception of the acoustic signal in MJD [s]"
    )

    ant_e0: Series[float] = pa.Field(
        description="Antenna position in east direction (ENU coords) at the time of the first measurement [m]"
    )

    ant_n0: Series[float] = pa.Field(
        description="Antenna position in north direction (ENU coords) at the time of the first measurement [m]"
    )

    ant_u0: Series[float] = pa.Field(
        description="Antenna position in up direction (ENU coords) at the time of the first measurement [m]"
    )

    head0: Series[float] = pa.Field(
        description="Antenna heading at the time of the first measurement [deg]"
    )

    pitch0: Series[float] = pa.Field(
        description="Antenna pitch at the time of the first measurement [deg]"
    )

    roll0: Series[float] = pa.Field(
        description="Antenna roll at the time of the first measurement [deg]"
    )

    ant_e1: Series[float] = pa.Field(
        description="Antenna position in east direction (ENU coords) at the time of the second measurement [m]"
    )

    ant_n1: Series[float] = pa.Field(
        description="Antenna position in north direction (ENU coords) at the time of the second measurement [m]"
    )

    ant_u1: Series[float] = pa.Field(
        description="Antenna position in up direction (ENU coords) at the time of the second measurement [m]"
    )

    head1: Series[float] = pa.Field(
        description="Antenna heading at the time of the second measurement [deg]"
    )

    pitch1: Series[float] = pa.Field(
        description="Antenna pitch at the time of the second measurement [deg]"
    )

    roll1: Series[float] = pa.Field(
        description="Antenna roll at the time of the second measurement [deg]"
    )

    flag: Series[bool] = pa.Field(
        default=False, description="Flag for mis-response in the data", coerce=True
    )
    latitude: Optional[Series[float]] = pa.Field(
        description="latitude of the antennae", alias="lat"
    )
    longitude: Optional[Series[float]] = pa.Field(
        description="longitude of the antennae", alias="lon"
    )

    gamma: Series[float] = pa.Field(
        default=0.0, description="Sound speed variation [m/s]", coerce=True
    )
    # These fields are populated after the model run
    ResiTT: Optional[Series[float]] = pa.Field(
        default=0.0, description="Residual travel time [ms]"
    )

    TakeOff: Optional[Series[float]] = pa.Field(
        default=0.0, description="Take off angle [deg]"
    )

    class Config:
        coerce = True
        add_missing_columns = True
    

class GarposObservationOutput(pa.DataFrameModel):
    MT: Series[str] = pa.Field(
        description="Station name", coerce=True
    )

    TT: Series[float] = pa.Field(description="Travel time [sec]")

    ST: Series[float] = pa.Field(
        description="Time of transmission of the acoustic signal in MJD [s]"
    )
    RT: Series[float] = pa.Field(
        description="Time of reception of the acoustic signal in MJD [s]"
    )

    flag: Series[bool] = pa.Field(
        default=False, description="Flag for mis-response in the data", coerce=True
    )
    # latitude: Series[float] = pa.Field(
    #     description="latitude of the antennae", alias="LAT"
    # )
    # longitude: Series[float] = pa.Field(
    #     description="longitude of the antennae", alias="LON"
    # )

    gamma: Series[float] = pa.Field(
        default=0.0, description="Sound speed variation [m/s]", coerce=True
    )
    # These fields are populated after the model run
    ResiTT: Series[float] = pa.Field(
        default=0.0, description="Residual travel time [ms]"
    )

    TakeOff: Series[float] = pa.Field(
        default=0.0, description="Take off angle [deg]"
    )
    head1: Series[float] = pa.Field(
        description="Antenna heading at the time of the second measurement [deg]"
    )
    ResiRange: Series[float] = pa.Field(
        default=0.0, description="Spatial residuals [m]"
    )
    dVO: Series[float] = pa.Field(
        default=0, description="Sound speed variation (for dV0)"
    )
    gradV1e: Series[float] = pa.Field(
        default=0, description="Sound speed variation (for dV0)"
    )
    gradV1n: Series[float] = pa.Field(
        default=0, description="Sound speed variation (for north component of grad(V1))"
    )
    gradV2e: Series[float] = pa.Field(
        default=0, description="Sound speed variation (for east component of grad(V2))"
    )
    gradV2n: Series[float] = pa.Field(
        default=0, description="Sound speed variation (for north component of grad(V2))"
    )
    dV: Series[float] = pa.Field(
        default=0,
        description="Correction term transformed into sound speed variation (gamma x V0)",
    )
    LogResidual: Series[float] = pa.Field(
        default=0,
        description="Actual residuals in estimation (log(TT) - log(calculated TT)",
    )

    class Config:
        coerce = True
        add_missing_columns = True

class InversionType(Enum):
    positions = 0  # solve only positions
    gammas = 1  # solve only gammas (sound speed variation)
    both = 2  # solve both positions and gammas


class InversionParams(BaseModel):
    spline_degree: int = Field(default=3)
    log_lambda: List[float] = Field(
        default=[-2], description="Smoothness paramter for backgroun perturbation"
    )
    log_gradlambda: float = Field(
        default=-1, description="Smoothness paramter for spatial gradient"
    )
    mu_t: List[float] = Field(
        default=[0.0],
        description="Correlation length of data for transmit time [minute]",
    )
    mu_mt: List[float] = Field(
        default=[0.5],
        description="Data correlation coefficient b/w the different transponders",
    )

    knotint0: int = Field(
        default=5,
        description="Typical Knot interval (in min.) for gamma's component (a0, a1, a2)",
    )
    knotint1: int = Field(
        default=0,
        description="Typical Knot interval (in min.) for gamma's component (a0, a1, a2)",
    )
    knotint2: int = Field(
        default=0,
        description="Typical Knot interval (in min.) for gamma's component (a0, a1, a2)",
    )
    rejectcriteria: float = Field(
        default=2, description="Criteria for the rejection of data (+/- rsig * Sigma)"
    )
    inversiontype: InversionType = Field(
        default=InversionType(value=0), description="Inversion type"
    )
    positionalOffset: Optional[List[float]] = Field(
        default=[0.0, 0.0, 0.0], description="Positional offset for the inversion"
    )
    traveltimescale: float = Field(
        default=1.0e-4,
        description="Typical measurement error for travel time (= 1.e-4 sec is recommended in 10 kHz carrier)",
    )
    maxloop: int = Field(default=100, description="Maximum loop for iteration")
    convcriteria: float = Field(
        default=5.0e-3, description="Convergence criteria for model parameters"
    )
    deltap: float = Field(
        default=1.0e-6, description="Infinitesimal values to make Jacobian matrix"
    )
    deltab: float = Field(
        default=1.0e-6, description="Infinitesimal values to make Jacobian matrix"
    )

    class Config:
        coerce = True

    @model_validator(mode="after")
    def validate(cls, values):
        match values.inversiontype:
            case InversionType.gammas:
                if any([x <= 0 for x in values.positionalOffset]):
                    logger.error(
                        "positionalOffset is required for InversionType.positions"
                    )
            case [InversionType.positions, InversionType.both]:
                if any([x > 0 for x in values.positionalOffset]):
                    values.positionalOffset = [0.0, 0.0, 0.0]
                    logger.error(
                        "positionalOffset is not required for InversionType.gammas"
                    )

        return values


class GarposObservation(BaseModel):
    campaign: str
    date_utc: datetime
    date_mjd: float
    ref_frame: str = "ITRF2014"
    shot_data: DataFrame[ObservationData]
    sound_speed_data: DataFrame[SoundVelocityDataFrame]

    @field_serializer("date_utc")
    def serialize_date(self, value):
        return str(value.isoformat())

    @field_serializer("shot_data")
    def serialize_shot_data(self, value):
        return value.to_json(orient="records")

    @field_serializer("sound_speed_data")
    def serialize_sound_speed_data(self, value):
        return value.to_json(orient="records")

    @field_validator("shot_data", mode="before")
    def validate_shot_data(cls, value):
        try:
            if isinstance(value, str):
                value = pd.read_json(value)

            return ObservationData.validate(value, lazy=True)
        except ValidationError as e:
            raise ValueError(f"Invalid shot data: {e}")

    @field_validator("sound_speed_data", mode="before")
    def validate_sound_speed_data(cls, value):
        try:
            if isinstance(value, str):
                value = pd.read_json(value)
            return SoundVelocityDataFrame.validate(value, lazy=True)
        except SchemaErrors as err:
            raise ValueError(f"Invalid sound speed data: {err.data}")

    @field_validator("date_utc", mode="before")
    def validate_date_utc(cls, value):
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError as e:
                raise ValueError(f"Invalid date format: {e}")
        return value


class GarposSite(BaseModel):
    name: str
    atd_offset: ATDOffset
    center_enu: Optional[PositionENU] = None
    center_llh: PositionLLH
    transponders: List[Transponder]
    delta_center_position: PositionENU = PositionENU()


class GarposInput(BaseModel):
    observation: GarposObservation
    site: GarposSite
    shot_data_file: Optional[Path] = None
    sound_speed_file: Optional[Path] = None


class GarposFixed(BaseModel):
    lib_directory: str = LIB_DIRECTORY
    lib_raytrace: str = LIB_RAYTRACE
    inversion_params: InversionParams = InversionParams()

class GarposResults(BaseModel):
    center_llh: PositionLLH
    delta_center_position: PositionENU
    transponders: list[Transponder]
    shot_data: DataFrame[GarposObservationOutput]

    @field_serializer("shot_data")
    def serialize_shot_data(self, value):
        return value.to_json(orient="records")
    
    @field_validator("shot_data", mode="before")
    def validate_shot_data(cls, value):
        try:
            if isinstance(value, str):
                value = pd.read_json(value)

            return GarposObservationOutput.validate(value, lazy=True)
        except ValidationError as e:
            raise ValueError(f"Invalid shot data: {e}")
