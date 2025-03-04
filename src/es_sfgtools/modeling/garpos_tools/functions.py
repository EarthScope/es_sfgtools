import pandera as pa
from pandera.typing import DataFrame
from pathlib import Path
from typing import List, Tuple, Union
import pandas as pd
from configparser import ConfigParser
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np
import sys
import os
import pymap3d as pm
import math
import julian
import logging
from scipy.stats import hmean as harmonic_mean
from scipy.stats import norm as normal_dist
import json
from matplotlib.colors import Normalize
from matplotlib.collections import LineCollection
import matplotlib.dates as mdates
import seaborn as sns

sns.set_theme()

import matplotlib.gridspec as gridspec

from es_sfgtools.processing.assets.observables import (
    ShotDataFrame,
    SoundVelocityDataFrame,
)
from es_sfgtools.processing.assets.siteconfig import (
    GPPositionENU,
    GPATDOffset,
    GPTransponder,
    GPPositionLLH,
    GPSiteConfig,
    Survey,
)
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposInput,
    GarposFixed,
    InversionParams,
    InversionType,
    ObservationData,
    GarposObservationOutput,
)
from es_sfgtools.utils.metadata.site import Site as MetaSite
from es_sfgtools.utils.loggers import GarposLogger as logger

from ...processing.assets.tiledb_temp import TDBShotDataArray

from garpos import drive_garpos


colors = [
    "blue",
    "green",
    "red",
    "cyan",
    "magenta",
    "yellow",
    "black",
    "brown",
    "orange",
    "pink",
]


def xyz2enu(x, y, z, lat0, lon0, hgt0, inv=1, **kwargs):
    """
    Rotates the vector of positions XYZ and covariance to
    the local east-north-up system at latitude and longitude
    (or XYZ coordinates) specified in origin.
    if inv = -1. then enu -> xyz

    Parameters
    ----------
    x : float
    y : float
    z : float
            Position in ECEF (if inv=-1, in ENU)
    lat0 : float
    lon0 : float
    Hgt0 : float
            Origin for the local system in degrees.
    inv : 1 or -1
            Switch (1: XYZ -> ENU, -1: ENU -> XYZ)

    Returns
    -------
    e : float
    n : float
    u : float
            Position in ENU (if inv=-1, in ECEF)
    """

    if inv != 1 and inv != -1:
        print("error in xyz2enu : ", inv)
        sys.exit(1)

    lat = lat0 * math.pi / 180.0 * inv
    lon = lon0 * math.pi / 180.0 * inv

    sphi = math.sin(lat)
    cphi = math.cos(lat)
    slmb = math.sin(lon)
    clmb = math.cos(lon)

    T1 = [-slmb, clmb, 0]
    T2 = [-sphi * clmb, -sphi * slmb, cphi]
    T3 = [cphi * clmb, cphi * slmb, sphi]

    e = x * T1[0] + y * T1[1] + z * T1[2]
    n = x * T2[0] + y * T2[1] + z * T2[2]
    u = x * T3[0] + y * T3[1] + z * T3[2]

    return e, n, u


class CoordTransformer:
    """
    A class to transform coordinates between different systems.

    Attributes:
        lat0 : float
            Latitude of the reference point.
        lon0 : float
            Longitude of the reference point.
        hgt0 : float
            Height of the reference point.
        X0 : float
            X coordinate of the reference point in ECEF.
        Y0 : float
            Y coordinate of the reference point in ECEF.
        Z0 : float
            Z coordinate of the reference point in ECEF.

    Methods:
        XYZ2ENU(X, Y, Z, **kwargs):
            Converts ECEF coordinates to ENU coordinates.
        LLH2ENU(lat, lon, hgt, **kwargs):
            Converts geodetic coordinates (latitude, longitude, height) to ENU coordinates.
        LLH2ENU_vec(lat: np.ndarray, lon: np.ndarray, hgt: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
            Converts arrays of geodetic coordinates to ENU coordinates.
        ECEF2ENU_vec(X: np.ndarray, Y: np.ndarray, Z: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
            Converts arrays of ECEF coordinates to ENU coordinates.
    """

    def __init__(self, latitude:float,longitude:float,elevation:float):
        """
        Initialize the object with a position in latitude, longitude, and height.
        Args:
            pos_llh (list | PositionLLH): The position in latitude, longitude, and height.
                                      It can be either a list [latitude, longitude, height]
                                      or an instance of PositionLLH class.
        """

      
        self.lat0 = latitude
        self.lon0 = longitude
        self.hgt0 = elevation

        self.X0, self.Y0, self.Z0 = pm.geodetic2ecef(self.lat0, self.lon0, self.hgt0)

    def XYZ2ENU(self, X: float, Y: float, Z: float) -> Tuple[float, float, float]:
        """
        Convert Cartesian coordinates (X, Y, Z) to East-North-Up (ENU) coordinates.

        Args:
            X (float): X coordinate in the Cartesian system.
            Y (float): Y coordinate in the Cartesian system.
            Z (float): Z coordinate in the Cartesian system.

        Returns:
            tuple: A tuple containing the East (e), North (n), and Up (u) coordinates.
        """

        dX, dY, dZ = X - self.X0, Y - self.Y0, Z - self.Z0
        e, n, u = xyz2enu(
            **{
                "x": dX,
                "y": dY,
                "z": dZ,
                "lat0": self.lat0,
                "lon0": self.lon0,
                "hgt0": self.hgt0,
            }
        )

        return e, n, u

    def LLH2ENU(self, lat: float, lon: float, hgt: float) -> Tuple[float, float, float]:
        """
        Convert latitude, longitude, and height (LLH) to East, North, Up (ENU) coordinates.
        This function converts geodetic coordinates (latitude, longitude, height) to local
        tangent plane coordinates (East, North, Up) relative to a reference point.

        Args:
            lat (float): Latitude in degrees.
            lon (float): Longitude in degrees.
            hgt (float): Height in meters.
        Returns:
            Tuple[float, float, float]: A tuple containing the East, North, and Up coordinates in meters.
        """

        X, Y, Z = pm.geodetic2ecef(lat, lon, hgt)
        dX, dY, dZ = X - self.X0, Y - self.Y0, Z - self.Z0
        e, n, u = xyz2enu(
            **{
                "x": dX,
                "y": dY,
                "z": dZ,
                "lat0": self.lat0,
                "lon0": self.lon0,
                "hgt0": self.hgt0,
            }
        )

        return e, n, u

    def LLH2ENU_vec(
        self, lat: np.ndarray, lon: np.ndarray, hgt: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Convert latitude, longitude, and height (LLH) coordinates to East-North-Up (ENU) coordinates.

        Args:
            lat : np.ndarray
                Array of latitudes in degrees.
            lon : np.ndarray
                Array of longitudes in degrees.
            hgt : np.ndarray
                Array of heights in meters.
        Returns:
            Tuple[np.ndarray, np.ndarray, np.ndarray]
                Tuple containing arrays of East, North, and Up coordinates in meters.
        """

        X, Y, Z = pm.geodetic2ecef(lat, lon, hgt)
        dX, dY, dZ = X - self.X0, Y - self.Y0, Z - self.Z0
        e, n, u = xyz2enu(
            **{
                "x": dX,
                "y": dY,
                "z": dZ,
                "lat0": self.lat0,
                "lon0": self.lon0,
                "hgt0": self.hgt0,
            }
        )

        return e, n, u

    def ECEF2ENU_vec(
        self, X: np.ndarray, Y: np.ndarray, Z: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Convert ECEF coordinates to ENU coordinates.

        Args:
            X : np.ndarray
                Array of X coordinates in meters.
            Y : np.ndarray
                Array of Y coordinates in meters.
            Z : np.ndarray
                Array of Z coordinates in meters.
        Returns:
            Tuple[np.ndarray, np.ndarray, np.ndarray]
                Tuple containing arrays of East, North, and Up coordinates in meters.
        """
        dX, dY, dZ = X - self.X0, Y - self.Y0, Z - self.Z0
        e, n, u = xyz2enu(
            **{
                "x": dX,
                "y": dY,
                "z": dZ,
                "lat0": self.lat0,
                "lon0": self.lon0,
                "hgt0": self.hgt0,
            }
        )

        return e, n, u


def garposinput_to_datafile(garpos_input: GarposInput, path: Path) -> None:
    """
    Write a GarposInput to a datafile

    Args:
        garpos_input (GarposInput): The GarposInput object
        path (Path): The path to the datafile

    Returns:
        None
    """

    logger.loginfo("Writing Garpos input to datafile")
    # Write the data file
    center_enu: List[float] = garpos_input.site.center_enu.get_position()
    delta_center_position: List[float] = (
        garpos_input.site.delta_center_position.get_position()
        + garpos_input.site.delta_center_position.get_std_dev()
        + [0.0, 0.0, 0.0]
    )
    atd_offset = garpos_input.site.atd_offset.get_offset() + [0.0, 0.0, 0.0] * 2

    obs_str = f"""
[Obs-parameter]
    Site_name   = {garpos_input.site.name}
    Campaign    = {garpos_input.observation.campaign}
    Date(UTC)   = {garpos_input.observation.date_utc.strftime('%Y-%m-%d')}
    Date(jday)  = {garpos_input.observation.date_mjd}
    Ref.Frame   = {garpos_input.observation.ref_frame}
    SoundSpeed  = {garpos_input.sound_speed_file}

[Data-file]
    datacsv     = {garpos_input.shot_data_file}
    N_shot      = {len(garpos_input.observation.shot_data.index)}
    used_shot   = {0}

[Site-parameter]
    Latitude0   = {garpos_input.site.center_llh.latitude}
    Longitude0  = {garpos_input.site.center_llh.longitude}
    Height0     = {garpos_input.site.center_llh.height}
    Stations    = {' '.join([transponder.id for transponder in garpos_input.site.transponders])}
    Center_ENU  = {center_enu[0]} {center_enu[1]} {center_enu[2]}

[Model-parameter]
    dCentPos    = {" ".join(map(str, delta_center_position))}
    ATDoffset   = {" ".join(map(str, atd_offset))}"""

    # Add the transponder data to the string
    for transponder in garpos_input.site.transponders:
        position = (
            transponder.position_enu.get_position()
            + transponder.position_enu.get_std_dev()
            + [0.0, 0.0, 0.0]
        )
        obs_str += f"""
    {transponder.id}_dPos    = {" ".join(map(str, position))}"""

    with open(path, "w") as f:
        f.write(obs_str)

    logger.info(f"Garpos input written to {path}")


def datafile_to_garposinput(path: Path) -> GarposInput:
    """
    Read a GarposInput from a datafile

    Args:
        path (Path): The path to the datafile

    Returns:
        GarposInput: The GarposInput object
    """

    logger.loginfo("Reading Garpos input from datafile")
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

    site = GarposSite(
        name=observation_section["Site_name"],
        center_llh=GPPositionLLH(
            latitude=float(site_section["Latitude0"]),
            longitude=float(site_section["Longitude0"]),
            height=float(site_section["Height0"]),
        ),
        center_enu=GPPositionENU(
            east=float(site_section["Center_ENU"].split()[0]),
            north=float(site_section["Center_ENU"].split()[1]),
            up=float(site_section["Center_ENU"].split()[2]),
        ),
        atd_offset=atd_offset,
        transponders=transponder_list,
        delta_center_position=delta_center_position,
    )

    # Now handle shot_data_file and sound_speed_file
    logger.info(f"Reading shot and sound speed data")
    shot_data_file = data_section["datacsv"]
    sound_speed_file = observation_section["soundspeed"]

    try:
        shot_data_results = ObservationData.validate(
            pd.read_csv(shot_data_file), lazy=True
        )
    except:
        shot_data_results = ObservationData.validate(
            pd.read_csv(shot_data_file, skiprows=1), lazy=True
        )
    sound_speed_results = SoundVelocityDataFrame.validate(
        pd.read_csv(sound_speed_file), lazy=True
    )

    # Populate GarposObservation
    logger.loginfo("Populating Garpos Observation")
    observation = GarposObservation(
        campaign=observation_section["campaign"],
        date_utc=(
            date_utc := datetime.strptime(observation_section["date(UTC)"], "%Y-%m-%d")
        ),
        date_mjd=julian.to_jd(date_utc, fmt="jd"),
        ref_frame=observation_section["ref.frame"],
        shot_data=shot_data_results,
        sound_speed_data=sound_speed_results,
    )

    logger.loginfo("Garpos input read from datafile, returning GarposInput object")

    return GarposInput(
        site=site,
        observation=observation,
        shot_data_file=shot_data_file,
        sound_speed_file=sound_speed_file,
    )


def garposfixed_to_datafile(garpos_fixed: GarposFixed, path: Path) -> None:
    """
    Write a GarposFixed to a datafile

    Args:
        garpos_fixed (GarposFixed): The GarposFixed object
        path (Path): The path to the datafile

    Returns:
        None
    """

    logger.loginfo("Writing Garpos fixed parameters to datafile")
    fixed_str = f"""[HyperParameters]
# Hyperparameters
#  When setting multiple values, ABIC-minimum HP will be searched.
#  The delimiter for multiple HP must be "space".

# Smoothness parameter for background perturbation (in log10 scale)
Log_Lambda0 = {" ".join([str(x) for x in garpos_fixed.inversion_params.log_lambda])}

# Smoothness parameter for spatial gradient ( = Lambda0 * gradLambda )
Log_gradLambda = {garpos_fixed.inversion_params.log_gradlambda}

# Correlation length of data for transmit time (in min.)
mu_t = {" ".join([str(x) for x in garpos_fixed.inversion_params.mu_t])}

# Data correlation coefficient b/w the different transponders.
mu_mt = {" ".join([str(x) for x in garpos_fixed.inversion_params.mu_mt])}

[Inv-parameter]
# The path for RayTrace lib.
lib_directory = {garpos_fixed.lib_directory}
lib_raytrace = {garpos_fixed.lib_raytrace}

# Typical Knot interval (in min.) for gamma's component (a0, a1, a2).
#  Note ;; shorter numbers recommended, but consider the computational resources.
knotint0 = {garpos_fixed.inversion_params.knotint0}
knotint1 = {garpos_fixed.inversion_params.knotint1}
knotint2 = {garpos_fixed.inversion_params.knotint2}

# Criteria for the rejection of data (+/- rsig * Sigma).
# if = 0, no data will be rejected during the process.
RejectCriteria = {garpos_fixed.inversion_params.rejectcriteria}

# Inversion type
#  0: solve only positions
#  1: solve only gammas (sound speed variation)
#  2: solve both positions and gammas
inversiontype = {garpos_fixed.inversion_params.inversiontype.value}

# Typical measurement error for travel time.
# (= 1.e-4 sec is recommended in 10 kHz carrier)
traveltimescale = {garpos_fixed.inversion_params.traveltimescale}

# Maximum loop for iteration.
maxloop = {garpos_fixed.inversion_params.maxloop}

# Convergence criteria for model parameters.
ConvCriteria = {garpos_fixed.inversion_params.convcriteria}

# Infinitesimal values to make Jacobian matrix.
deltap = {garpos_fixed.inversion_params.deltap}
deltab = {garpos_fixed.inversion_params.deltab}"""

    with open(path, "w") as f:
        f.write(fixed_str)

    logger.info(f"Garpos fixed parameters written to {path}") 


def garposfixed_from_datafile(path: Path) -> GarposFixed:
    """
    Read a GarposFixed from a datafile
    
    Args:
        path (Path): The path to the datafile
        
    Returns:
        GarposFixed: The GarposFixed object
    """

    logger.loginfo("Reading Garpos fixed parameters from datafile {}".format(path))
    config = ConfigParser()
    config.read(path)

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
        inversiontype=InversionType(int(inv_parameters.get("inversiontype", 0))),
        traveltimescale=float(inv_parameters["traveltimescale"]),
        maxloop=int(inv_parameters["maxloop"]),
        convcriteria=float(inv_parameters["ConvCriteria"]),
        deltap=float(inv_parameters["deltap"]),
        deltab=float(inv_parameters["deltab"]),
    )

    # Populate GarposFixed
    garpos_fixed = GarposFixed(
        lib_directory=inv_parameters["lib_directory"],
        lib_raytrace=inv_parameters["lib_raytrace"],
        inversion_params=inversion_params,
    )
    logger.loginfo("Garpos fixed parameters read from datafile, returning GarposFixed object")
    return garpos_fixed


def avg_transponder_position(
    transponders: List[GPTransponder],
) -> Tuple[GPPositionENU, GPPositionLLH]:
    """
    Calculate the average position of the transponders.

    Args:
        transponders (List[Transponder]): A list of transponders.

    Returns:
        Tuple[PositionENU, PositionLLH]: A tuple containing the average position in ENU and LLH coordinates.
    """
    pos_array_llh = []
    pos_array_enu = []
    for transponder in transponders:
        pos_array_llh.append(
            [
                transponder.position_llh.latitude,
                transponder.position_llh.longitude,
                transponder.position_llh.height,
            ]
        )
        pos_array_enu.append(transponder.position_enu.get_position())
    avg_pos_llh = np.mean(pos_array_llh, axis=0).tolist()
    avg_pos_enu = np.mean(pos_array_enu, axis=0).tolist()

    out_pos_llh = GPPositionLLH(
        latitude=avg_pos_llh[0], longitude=avg_pos_llh[1], height=avg_pos_llh[2]
    )
    out_pos_enu = GPPositionENU(
        east=avg_pos_enu[0], north=avg_pos_enu[1], up=avg_pos_enu[2]
    )

    return out_pos_enu, out_pos_llh


def plot_enu_llh_side_by_side(garpos_input: GarposInput):
    """
    Plot the transponder and antenna positions in ENU and LLH coordinates side by side.

    Args:
        garpos_input (GarposInput): The input data containing observations and site information.
    """

    # Create a figure with two subplots
    fig, axs = plt.subplots(1, 2, figsize=(20, 10))

    # Plot ENU plot on the first subplot
    ax_enu = axs[0]
    # Plot lines between antenna positions
    ax_enu.scatter(
        garpos_input.observation.shot_data["ant_e0"],
        garpos_input.observation.shot_data["ant_n0"],
        color="green",
        marker="o",
        linewidths=0.25,
    )

    # Plot transponder positions
    for transponder in garpos_input.site.transponders:
        ax_enu.scatter(
            transponder.position_enu.east,
            transponder.position_enu.north,
            label=transponder.id,
            marker="x",
            color="red",
            linewidths=5,
        )

    # Plot site center enu
    ax_enu.scatter(
        garpos_input.site.center_enu.east,
        garpos_input.site.center_enu.north,
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
        garpos_input.observation.shot_data["longitude"],
        garpos_input.observation.shot_data["latitude"],
        color="green",
        marker="o",
        linewidths=0.25,
    )
    # Plot transponder positions
    for transponder in garpos_input.site.transponders:
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
        garpos_input.site.center_llh.longitude,
        garpos_input.site.center_llh.latitude,
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


def rectify_shotdata_site(
    site_config: GPSiteConfig, shot_data: DataFrame[ObservationData]
) -> Tuple[GPSiteConfig, DataFrame[ShotDataFrame]]:
    """
    Rectifies shot data for a given site configuration.
    This function transforms the shot data coordinates from ECEF to ENU, renames
    certain columns, and updates the transponder positions in the site configuration.
    Args:
        site_config (SiteConfig): The site configuration containing transponder information.
        shot_data (DataFrame[ObservationData]): The shot data to be rectified.
    Returns:
        Tuple[SiteConfig, DataFrame[ShotDataFrame]]: A tuple containing the updated site configuration
        and the rectified shot data.

    The returned shot data DataFrame will have the following columns:
        - triggerTime
        - MT (transponder ID)
        - ST (ping time)
        - RT (return time)
        - TT (travel time)
        - ant_e0 (antenna east coordinate at time 0)
        - ant_n0 (antenna north coordinate at time 0)
        - ant_u0 (antenna up coordinate at time 0)
        - head0 (heading at time 0)
        - pitch0 (pitch at time 0)
        - roll0 (roll at time 0)
        - ant_e1 (antenna east coordinate at time 1)
        - ant_n1 (antenna north coordinate at time 1)
        - ant_u1 (antenna up coordinate at time 1)
        - head1 (heading at time 1)
        - pitch1 (pitch at time 1)
        - roll1 (roll at time 1)
    """

    site_config = site_config.copy()  # avoid aliasing #TODO use model_copy instead?
    coord_transformer = CoordTransformer(site_config.position_llh)
    e0, n0, u0 = coord_transformer.ECEF2ENU_vec(
        shot_data.east0.to_numpy(),
        shot_data.north0.to_numpy(),
        shot_data.up0.to_numpy(),
    )
    e1, n1, u1 = coord_transformer.ECEF2ENU_vec(
        shot_data.east1.to_numpy(),
        shot_data.north1.to_numpy(),
        shot_data.up1.to_numpy(),
    )
    shot_data["ant_e0"] = e0
    shot_data["ant_n0"] = n0
    shot_data["ant_u0"] = u0
    shot_data["ant_e1"] = e1
    shot_data["ant_n1"] = n1
    shot_data["ant_u1"] = u1
    # shot_data["SET"] = "S01" now handled with self._subset_shots
    shot_data["LN"] = "L01"
    rename_dict = {
        "trigger_time": "triggertime",
        "hae0": "height",
        "pingTime": "ST",
        "returnTime": "RT",
        "tt": "TT",
        "transponderID": "MT",
    }
    shot_data = shot_data.rename(columns=rename_dict).loc[
        :,
        [
            "triggerTime",
            "MT",
            "ST",
            "RT",
            "TT",
            "ant_e0",
            "ant_n0",
            "ant_u0",
            "head0",
            "pitch0",
            "roll0",
            "ant_e1",
            "ant_n1",
            "ant_u1",
            "head1",
            "pitch1",
            "roll1",
        ],
    ]
    for transponder in site_config.transponders:
        lat, lon, hgt = (
            transponder.position_llh.latitude,
            transponder.position_llh.longitude,
            transponder.position_llh.height,
        )

        e, n, u = coord_transformer.LLH2ENU(lat, lon, hgt)

        transponder.position_enu = GPPositionENU(east=e, north=n, up=u)
        transponder.id = (
            "M" + str(transponder.id)
            if str(transponder.id)[0].isdigit()
            else str(transponder.id)
        )

    return site_config, ObservationData.validate(shot_data, lazy=True).sort_values(
        "triggerTime"
    )


def process_garpos_results(results: GarposInput) -> Tuple[GarposInput, pd.DataFrame]:
    """
    Process garpos results to compute delta x, y, z and relevant fields.
    This function processes the garpos results to calculate the delta x, y, z
    for each transponder and other relevant fields. It also converts the
    residual travel time (ResiTT) to meters using the harmonic mean of the
    sound speed data.

    Args:
        results (GarposInput): The input data containing observations and site information.
    Returns:
        Tuple[GarposResults, pd.DataFrame]: A tuple containing the processed garpos results
        and a DataFrame with the shot data including the calculated residual ranges.
    """

    # Process garpos results to get delta x,y,z and relevant fields
    logger.loginfo("Processing GARPOS results")

    # Get the harmonic mean of the svp data, and use that to convert ResiTT to meters
    svp_df = pd.read_csv(results.sound_speed_data)
    results_df = pd.read_csv(results.shot_data,skiprows=1)
    speed_mean = harmonic_mean(svp_df.speed.values)
    range_residuals = results_df.ResiTT.values * speed_mean / 2


    results_df["ResiRange"] = range_residuals
    results_df = GarposObservationOutput.validate(results_df, lazy=True)
    
    # For each transponder, get the delta x,y,and z respectively
    for transponder in results.transponders:
        id = transponder.id
        takeoff = np.deg2rad(
            results_df[
                results_df.MT == id
            ].TakeOff.values
        )
        azimuth = np.deg2rad(
            results_df[
                results_df.MT == id
            ].head1.values
        )
        delta_x = np.mean(np.cos(takeoff) * np.cos(azimuth))
        delta_y = np.mean(np.cos(takeoff) * np.sin(azimuth))
        delta_z = np.mean(np.sin(azimuth))

        transponder.delta_center_position = GPPositionENU(
            east=delta_x, north=delta_y, up=delta_z
        )
    # save updated df
    results_df.to_csv(results.shot_data)

    logger.loginfo("GARPOS results processed, returning results tuple")
    return results, results_df
