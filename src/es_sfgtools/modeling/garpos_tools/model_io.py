"""
Author: Franklyn Dunbar
Date: 2024-05-01
Email: franklyn.dunbar@earthscope.org, franklyn.dunbar@umontana.edu
"""

# External Imports
from typing import Optional, List, Dict, Tuple
from pydantic import BaseModel, Field
from datetime import datetime
import os
import pandas as pd
from configparser import ConfigParser
import matplotlib.pyplot as plt
import logging

# Local Imports

from ...es_sfgtools.schemas.generics import Point, PositionENU, PositionLLH,ATDOffset,Transponder
from ...es_sfgtools.schemas import AcousticDataFrame, SoundVelocityProfile
from .observation import GarposObservation, GarposSite,ObservationData 
from .hyper_params import InversionParams, InversionType

logger = logging.getLogger(__name__)


DIR = os.environ.get("GARPOS_DIR")
LIB_DIRECTORY = os.path.join(DIR, "bin/garpos_v101/f90lib/")
LIB_RAYTRACE = "lib_raytrace.so"


class GarposInput(BaseModel):
    observation: GarposObservation
    site: GarposSite
    shot_data_file: Optional[str] = None
    sound_speed_file: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)

        # Correct for transponder tat offset
        # Get offsets [seconds] for each transponder
        offsets: Dict[str,float] = {t.id:t.tat_offset for t in self.site.transponders}
        self.observation.shot_data.MT = self.observation.shot_data.MT.apply(lambda x: "M" + str(x) if x[0].isdigit() else x)
        # Apply offsets to the shot data
        # for row in self.observation.shot_data.itertuples():
        #     self.observation.shot_data.at[row.Index, "TT" ] -= offsets.get(row.MT,0.0)
        #     #self.observation.shot_data.at[row.Index, "RT"] -= offsets.get(row.MT,0.0)
     
        # self.observation.correct_for_site(self.site)
        logger.info(f"GarposSite Data: {self.site.center_llh}")

    def plot_enu_llh_side_by_side(self):
        # Create a figure with two subplots
        fig, axs = plt.subplots(1, 2, figsize=(20, 10))

        # Plot ENU plot on the first subplot
        ax_enu = axs[0]
        # Plot lines between antenna positions
        ax_enu.plot(
            [
                self.observation.shot_data["ant_e0"],
                self.observation.shot_data["ant_e1"],
            ],
            [
                self.observation.shot_data["ant_n0"],
                self.observation.shot_data["ant_n1"],
            ],
            color="black",
            linestyle="dashed",
            markersize=0.25,
            label="Antenna Positions",
        )
        # Plot transponder positions
        for transponder in self.site.transponders:
            ax_enu.scatter(
                transponder.position_enu.east.value,
                transponder.position_enu.north.value,
                label=transponder.id,
                marker="x",
                color="red",
                linewidths=5,
            )

        # Plot site center enu
        ax_enu.scatter(
            self.site.center_enu.east.value,
            self.site.center_enu.north.value,
            label="Center",
            marker="x",
            color="blue",
            linewidths=5,
        )
        ax_enu.set_xlabel("East (m)")
        ax_enu.set_ylabel("North (m)")
        ax_enu.set_title("Transponder and Antenna Positions (ENU)")
        ax_enu.grid(True)

        # Plot LLH plot on the second subplot
        ax_llh = axs[1]
        # Plot lines between antenna positions
        ax_llh.scatter(
            self.observation.shot_data["longitude"],
            self.observation.shot_data["latitude"],
            color="green",
            marker="o",
            linewidths=0.25,
        )
        # Plot transponder positions
        for transponder in self.site.transponders:
            ax_llh.scatter(
                transponder.position_llh.longitude,
                transponder.position_llh.latitude,
                label=transponder.id,
                marker="x",
                color="red",
                linewidths=5,
            )
        # Plot site center llh
        ax_llh.scatter(
            self.site.center_llh.longitude,
            self.site.center_llh.latitude,
            label="Center",
            marker="x",
            color="blue",
            linewidths=5,
        )
        ax_llh.set_xlabel("Longitude")
        ax_llh.set_ylabel("Latitude")
        ax_llh.set_title("Transponder and Antenna Positions (LLH)")
        ax_llh.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()

    def to_dat_file(self, dir_path: str, file_path: str) -> None:
        os.makedirs(dir_path, exist_ok=True)
        if not self.shot_data_file:
            self.shot_data_file = os.path.join(
                dir_path, f"{self.site.name}_shot_data.csv"
            )

        if not self.sound_speed_file:
            self.sound_speed_file = os.path.join(
                dir_path, f"{self.site.name}_sound_speed.csv"
            )

        center_enu = self.site.center_enu.get_position()
        delta_center_position = (
            self.site.delta_center_position.get_position()
            + self.site.delta_center_position.get_std_dev()
        )
        delta_center_position += [0.0, 0.0, 0.0]
        atd_offset = (
            self.site.atd_offset.get_offset()
            + self.site.atd_offset.get_std_dev()
            + [0.0, 0.0, 0.0]
        )
        obs_str = f"""
[Obs-parameter]
    Site_name   = {self.site.name}
    Campaign    = {self.observation.campaign}
    Date(UTC)   = {self.observation.date_utc.strftime('%Y-%m-%d')}
    Date(jday)  = {self.observation.date_mjd}
    Ref.Frame   = {self.observation.ref_frame}
    SoundSpeed  = {self.sound_speed_file}

[Data-file]
    datacsv     = {self.shot_data_file}
    N_shot      = {len(self.observation.shot_data.index)}
    used_shot   = {0}

[Site-parameter]
    Latitude0   = {self.site.center_llh.latitude}
    Longitude0  = {self.site.center_llh.longitude}
    Height0     = {self.site.center_llh.height}
    Stations    = {' '.join([transponder.id for transponder in self.site.transponders])}
    Center_ENU  = {center_enu[0]} {center_enu[1]} {center_enu[2]}

[Model-parameter]
    dCentPos    = {" ".join(map(str, delta_center_position))}
    ATDoffset   = {" ".join(map(str, atd_offset))}"""

        # Add the transponder data to the string
        for transponder in self.site.transponders:
            position = (
                transponder.position_enu.get_position()
                + transponder.position_enu.get_std_dev()
                + [0.0, 0.0, 0.0]
            )
            obs_str += f"""
    {transponder.id}_dPos    = {" ".join(map(str, position))}"""

        with open(os.path.join(dir_path, os.path.basename(file_path)), "w") as f:
            f.write(obs_str)
            logger.info(f"GarposInput written to {file_path}")
        self.observation.shot_data.MT = self.observation.shot_data.MT.apply(
            lambda x: "M" + str(x) if x[0].isdigit() else x
        )
        self.observation.shot_data.to_csv(self.shot_data_file)
        self.observation.sound_speed_data.to_csv(self.sound_speed_file, index=False)

        logger.info(f"Shot data written to {self.shot_data_file}")
        logger.info(f"Sound speed data written to {self.sound_speed_file}")

    @classmethod
    def from_dat_file(cls, file_path: str) -> "GarposInput":
        config = ConfigParser()
        config.read(file_path)

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
            position = PositionENU(
                east=Point(value=east_value, sigma=east_sigma),
                north=Point(value=north_value, sigma=north_sigma),
                up=Point(value=up_value, sigma=up_sigma),
                cov_en=cov_en,
                cov_ue=cov_ue,
                cov_nu=cov_nu,
            )
            if "dpos" in key:
                transponder_id = key.split("_")[0].upper()
                transponder = Transponder(id=transponder_id, position_enu=position)
                transponder_list.append(transponder)
            if "dcentpos" in key:
                delta_center_position = position
            if "atdoffset" in key:
                atd_offset = ATDOffset(
                    forward=position.east,
                    rightward=position.north,
                    downward=position.up,
                    cov_fr=cov_en,
                    cov_df=cov_ue,
                    cov_rd=cov_nu,
                )
        # Populate GarposSite
        site = GarposSite(
            name=observation_section["site_name"],
            atd_offset=atd_offset,
            center_enu=PositionENU(
                east=Point(value=float(site_section["center_enu"].split()[0])),
                north=Point(value=float(site_section["center_enu"].split()[1])),
                up=Point(value=float(site_section["center_enu"].split()[2])),
            ),
            center_llh=PositionLLH(
                latitude=float(site_section["latitude0"]),
                longitude=float(site_section["longitude0"]),
                height=float(site_section["height0"]),
            ),
            transponders=transponder_list,
            delta_center_position=delta_center_position,  # We'll handle this later
        )

        # Now handle shot_data_file and sound_speed_file
        shot_data_file = data_section["datacsv"]
        sound_speed_file = observation_section["soundspeed"]

        df = pd.read_csv(shot_data_file, skiprows=1, index_col=0)
        shot_data_results = ObservationData.validate(df,lazy=True)
        sound_speed_results = SoundVelocityProfile(pd.read_csv(sound_speed_file))
        # Populate GarposObservation
        observation = GarposObservation(
            campaign=observation_section["campaign"],
            date_utc=datetime.strptime(observation_section["date(utc)"], "%Y-%m-%d"),
            date_mjd=observation_section["date(jday)"],
            ref_frame=observation_section["ref.frame"],
            shot_data=shot_data_results,
            sound_speed_data=sound_speed_results,
        )

        # Instantiate GarposInput
        return cls(
            observation=observation,
            site=site,
            shot_data_file=shot_data_file,
            sound_speed_file=sound_speed_file,
        )


class GarposFixed(BaseModel):
    lib_directory: str = LIB_DIRECTORY
    lib_raytrace: str = LIB_RAYTRACE
    inversion_params: InversionParams = InversionParams()

    def to_dat_file(self, dir_path: str, file_path: str) -> None:
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

        with open(os.path.join(dir_path, os.path.basename(file_path)), "w") as f:
            f.write(fixed_str)

    @classmethod
    def from_dat_file(cls, file_path: str) -> "GarposFixed":
        config = ConfigParser()
        config.read(file_path)

        # Extract data from config
        hyperparameters = config["HyperParameters"]
        inv_parameters = config["Inv-parameter"]

        # Populate InversionParams
        inversion_params = InversionParams(
            log_lambda=[float(x) for x in hyperparameters["Log_Lambda0"].split()],
            log_gradlambda=float(hyperparameters["Log_gradLambda"]),
            mu_t=[float(x) for x in hyperparameters["mu_t"].split()],
            mu_mt=[float(hyperparameters["mu_mt"])],
            knotint0=int(inv_parameters["knotint0"]),
            knotint1=int(inv_parameters["knotint1"]),
            knotint2=int(inv_parameters["knotint2"]),
            rejectcriteria=float(inv_parameters["RejectCriteria"]),
            inversiontype=InversionType(int(inv_parameters["inversiontype"])),
            traveltimescale=float(inv_parameters["traveltimescale"]),
            maxloop=int(inv_parameters["maxloop"]),
            convcriteria=float(inv_parameters["ConvCriteria"]),
            deltap=float(inv_parameters["deltap"]),
            deltab=float(inv_parameters["deltab"]),
        )

        # Populate GarposFixed
        return cls(
            lib_directory=inv_parameters["lib_directory"],
            lib_raytrace=inv_parameters["lib_raytrace"],
            inversion_params=inversion_params,
        )

class GarposResults(BaseModel):
    rms: float
    used_shot: float

    @classmethod
    def from_dat_file(cls, file_path: str) -> "GarposResults":
        """
        Parse the results from a Garpos run

        Example:
            #Inversion-type 0 Loop  1- 1, RMS(TT) =   0.242981 ms, used_shot =  98.4%, reject =    6, Max(dX) =     3.2381, Hgt =  -2109.197
            #Inversion-type 0 Loop  1- 2, RMS(TT) =   0.237122 ms, used_shot =  97.6%, reject =    9, Max(dX) =     0.0119, Hgt =  -2109.198
            #Inversion-type 0 Loop  1- 3, RMS(TT) =   0.237104 ms, used_shot =  97.6%, reject =    9, Max(dX) =     0.0036, Hgt =  -2109.197
            #Inversion-type 0 Loop  1- 4, RMS(TT) =   0.237104 ms, used_shot =  97.6%, reject =    9, Max(dX) =     0.0000, Hgt =  -2109.197
            #  ABIC =        4396.132990  misfit =  1.969 test_L-02.0
            # lambda_0^2 =   0.01000000
            # lambda_g^2 =   0.00100000
            # mu_t =   0.00000000 sec.
            # mu_MT = 0.5000

            --> RMS = 0.237104 ms,used_shot = 97.6

                Args:
                    file_path (str): _description_

                Returns:
                    GarposResults: _description_
        """
        with open(file_path, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "RMS" in line:
                    # "Inversion-type 0 Loop  1- 4, RMS(TT) =   0.237104 ms, used_shot =  97.6%, reject =    9, Max(dX) =     0.0000, Hgt =  -2109.197" --->
                    # "0.237104 , used_shot =  97.6, reject =    9, Max(dX) =     0.0000, Hgt =  -2109.197"
                    results = line.split("RMS(TT) =")[-1].replace("ms", "").replace("%", "").strip()
                    # "0.237104 ms, used_shot =  97.6%, reject =    9, Max(dX) =     0.0000, Hgt =  -2109.197" -->
                    # ["0.237104", "used_shot", "97.6", "reject", "9,", "Max(dX)", "0.0000,", "Hgt", "-2109.197"]
                    results = [x for x in results.replace(","," ").split(" ") if x != '']
                    rms = float(results[0])
                    used_shot = float(results[3])



        return cls(rms=rms, used_shot=used_shot)
    