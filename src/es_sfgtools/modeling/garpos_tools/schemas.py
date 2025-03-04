import pandera as pa
from pandera.typing import Series, DataFrame
from pandera.errors import SchemaErrors
from enum import Enum
from typing import List, Optional,Union
from pydantic import BaseModel, Field, model_validator,field_serializer,field_validator,ValidationError
from pathlib import Path
import pandas as pd
from datetime import datetime
from configparser import ConfigParser
from ...processing.assets.observables import SoundVelocityDataFrame
from ...processing.assets.siteconfig import GPATDOffset, GPPositionENU, GPPositionLLH, GPTransponder
from es_sfgtools.utils.loggers import GarposLogger as logger
import julian

from garpos import LIB_DIRECTORY,LIB_RAYTRACE

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
    delta_center_position : GPPositionENU = Field(
        default=GPPositionENU(east_sigma=1,north_sigma=1), description="Delta center position"
    )


    class Config:
        coerce = True

    @model_validator(mode="after")
    def validate(cls, values):
        match values.inversiontype:
            case InversionType.gammas:
                if any([x <= 0 for x in values.positionalOffset]):
                    logger.logerr(
                        "positionalOffset is required for InversionType.positions"
                    )
            case [InversionType.positions, InversionType.both]:
                if any([x > 0 for x in values.positionalOffset]):
                    values.positionalOffset = [0.0, 0.0, 0.0]
                    logger.logerr(
                        "positionalOffset is not required for InversionType.gammas"
                    )

        return values


class GarposFixed(BaseModel):
    lib_directory: str = LIB_DIRECTORY
    lib_raytrace: str = LIB_RAYTRACE
    inversion_params: InversionParams = InversionParams()

    def _to_datafile(
        self,path: Path
    ) -> None:
        """
        Generates a data file with fixed parameters for the inversion process.
        This method creates a configuration file with hyperparameters and inversion parameters
        required for the inversion process. The generated file is written to the specified path.
        Args:
            self.inversion_params (InversionParams): An instance of InversionParams containing the
                parameters for the inversion process.
            path (Path): The file path where the generated configuration file will be saved.
        Returns:
            None
        """

        
        fixed_str = f"""[HyperParameters]
    # Hyperparameters
    #  When setting multiple values, ABIC-minimum HP will be searched.
    #  The delimiter for multiple HP must be "space".

    # Smoothness parameter for background perturbation (in log10 scale)
    Log_Lambda0 = {" ".join([str(x) for x in self.inversion_params.log_lambda])}

    # Smoothness parameter for spatial gradient ( = Lambda0 * gradLambda )
    Log_gradLambda = {self.inversion_params.log_gradlambda}

    # Correlation length of data for transmit time (in min.)
    mu_t = {" ".join([str(x) for x in self.inversion_params.mu_t])}

    # Data correlation coefficient b/w the different transponders.
    mu_mt = {" ".join([str(x) for x in self.inversion_params.mu_mt])}

    [Inv-parameter]
    # The path for RayTrace lib.
    lib_directory = {self.lib_directory}
    lib_raytrace = {self.lib_raytrace}

    # Typical Knot interval (in min.) for gamma's component (a0, a1, a2).
    #  Note ;; shorter numbers recommended, but consider the computational resources.
    knotint0 = {self.inversion_params.knotint0}
    knotint1 = {self.inversion_params.knotint1}
    knotint2 = {self.inversion_params.knotint2}

    # Criteria for the rejection of data (+/- rsig * Sigma).
    # if = 0, no data will be rejected during the process.
    RejectCriteria = {self.inversion_params.rejectcriteria}

    # Inversion type
    #  0: solve only positions
    #  1: solve only gammas (sound speed variation)
    #  2: solve both positions and gammas
    inversiontype = {self.inversion_params.inversiontype.value}

    # Typical measurement error for travel time.
    # (= 1.e-4 sec is recommended in 10 kHz carrier)
    traveltimescale = {self.inversion_params.traveltimescale}

    # Maximum loop for iteration.
    maxloop = {self.inversion_params.maxloop}

    # Convergence criteria for model parameters.
    ConvCriteria = {self.inversion_params.convcriteria}

    # Infinitesimal values to make Jacobian matrix.
    deltap = {self.inversion_params.deltap}
    deltab = {self.inversion_params.deltab}"""

        with open(path, "w") as f:
            f.write(fixed_str)

class GarposInput(BaseModel):
    site_name: str
    campaign_id: str
    survey_id: str
    site_center_llh: GPPositionLLH
    array_center_enu: GPPositionENU
    transponders: List[GPTransponder]
    sound_speed_data: Optional[Path|str]
    atd_offset: GPATDOffset
    start_date: datetime
    end_date:datetime
    shot_data: Optional[Path| str]
    delta_center_position:GPPositionENU = GPPositionENU()
    ref_frame: str = "ITRF"
    n_shot:int

    @field_serializer("shot_data","sound_speed_data")
    def path_to_str(self,value):
        return str(value)
    
    @field_serializer("start_date","end_date")
    def dt_to_str(self,value):
        return value.isoformat()
    
    def to_datafile(self, path: Path) -> None:
        """
        Write a GarposInput to a datafile

        Args:
            garpos_input (GarposInput): The GarposInput object
            path (Path): The path to the datafile

        Returns:
            None
        """
        def datetime_to_mjd(dt: datetime) -> float:
            jd = julian.to_jd(dt, fmt='jd')
            mjd = jd - 2400000.5
            return mjd
        
        for transponder in self.transponders:
            if not "M" in transponder.id:
                transponder.id = "M" + transponder.id 
        # Write the data file
        center_enu: List[float] = self.array_center_enu.get_position()
        delta_center_position: List[float] = (
            self.delta_center_position.get_position()
            + self.delta_center_position.get_std_dev()
            + [0.0, 0.0, 0.0]
        )
        atd_offset = self.atd_offset.get_offset() + [0.0, 0.0, 0.0] * 2
        date_mjd = datetime_to_mjd(self.start_date)
        obs_str = f"""
[Obs-parameter]
    Site_name   = {self.site_name}
    Campaign    = {self.campaign_id}
    Date(UTC)   = {self.start_date.strftime('%Y-%m-%d')}
    Date(jday)  = {date_mjd}
    Ref.Frame   = {self.ref_frame}
    SoundSpeed  = {str(self.sound_speed_data)}

[Data-file]
    datacsv     = {str(self.shot_data)}
    N_shot      = {self.n_shot}
    used_shot   = {0}

[Site-parameter]
    Latitude0   = {self.site_center_llh.latitude}
    Longitude0  = {self.site_center_llh.longitude}
    Height0     = {self.site_center_llh.height}
    Stations    = {' '.join([transponder.id for transponder in self.transponders])}
    Center_ENU  = {center_enu[0]} {center_enu[1]} {center_enu[2]}

[Model-parameter]
    dCentPos    = {" ".join(map(str, delta_center_position))}
    ATDoffset   = {" ".join(map(str, atd_offset))}"""

        # Add the transponder data to the string
        for transponder in self.transponders:
            position = (
                transponder.position_enu.get_position()
                + transponder.position_enu.get_std_dev()
                + [0.0, 0.0, 0.0]
            )
            obs_str += f"""
    {transponder.id}_dPos    = {" ".join(map(str, position))}"""

        with open(path, "w") as f:
            f.write(obs_str)

    @classmethod
    def from_datafile(cls,path:Path) -> "GarposInput":
        config = ConfigParser()
        config.read(path)

        # Extract data from config
        observation_section = config["Obs-parameter"]
        site_section = config["Site-parameter"]
        model_section = config["Model-parameter"]
        data_section = config["Data-file"]
        # populate transponders
        transponder_list = []
        for key in model_section.keys():
            (
                east_value,
                north_value,
                up_value,
                east_sigma,
                north_sigma,
                up_sigma,
                cov_en,
                cov_ue,
                cov_nu,
            ) = [float(x) for x in model_section[key].split()]
            position = GPPositionENU(
                east=east_value,
                east_sigma=east_sigma,
                north=north_value,
                north_sigma=north_sigma,
                up=up_value,
                up_sigma=up_sigma,
                cov_en=cov_en,
                cov_ue=cov_ue,
                cov_nu=cov_nu,
            )
            if "dpos" in key:
                transponder_id = key.split("_")[0].upper()
                transponder = GPTransponder(id=transponder_id, position_enu=position)
                transponder_list.append(transponder)
            if "dcentpos" in key:
                delta_center_position = position
            if "atdoffset" in key:
                atd_offset = GPATDOffset(
                    forward=position.east,
                    rightward=position.north,
                    downward=position.up,
                )

        start_date = datetime.strptime(observation_section["Date(UTC)"], "%Y-%m-%d")
        date_mjd = float(observation_section["Date(jday)"])
        start_date = julian.from_jd(date_mjd + 2400000.5, fmt="jd")

        garpos_input = cls(
            site_name=observation_section["Site_name"],
            campaign_id=observation_section["Campaign"],
            survey_id=data_section.get("SurveyID", ""),
            site_center_llh=GPPositionLLH(
                latitude=float(site_section["Latitude0"]),
                longitude=float(site_section["Longitude0"]),
                height=float(site_section["Height0"]),
            ),
            array_center_enu=GPPositionENU(
                east=float(site_section["Center_ENU"].split()[0]),
                north=float(site_section["Center_ENU"].split()[1]),
                up=float(site_section["Center_ENU"].split()[2]),
            ),
            transponders=transponder_list,
            sound_speed_data=(
                Path(observation_section["SoundSpeed"])
                if observation_section["SoundSpeed"]
                else None
            ),
            atd_offset=atd_offset,
            start_date=start_date,
            end_date=start_date,  # Assuming end_date is not provided in the file
            shot_data=(
                Path(data_section["datacsv"]) if data_section["datacsv"] else None
            ),
            delta_center_position=delta_center_position,
            ref_frame=observation_section.get("Ref.Frame", "ITRF"),
            n_shot=data_section["N_shot"]
        )

        return garpos_input
