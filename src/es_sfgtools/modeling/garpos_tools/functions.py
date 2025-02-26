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
    Site,
    Survey,
)
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposInput,
    GarposObservation,
    GarposSite,
    GarposFixed,
    InversionParams,
    InversionType,
    ObservationData,
    GarposObservationOutput,
    GarposResults,
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

    def __init__(self, lattitude:float,longitude:float,elevation:float):
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


def process_garpos_results(results: GarposInput) -> Tuple[GarposResults, pd.DataFrame]:
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
    speed_mean = harmonic_mean(results.observation.sound_speed_data.speed.values)
    range_residuals = results.observation.shot_data.ResiTT.values * speed_mean / 2

    results_df = results.observation.shot_data
    results_df["ResiRange"] = range_residuals
    results_df = GarposObservationOutput.validate(results_df, lazy=True)
    
    # For each transponder, get the delta x,y,and z respectively
    for transponder in results.site.transponders:
        id = transponder.id
        takeoff = np.deg2rad(
            results.observation.shot_data[
                results.observation.shot_data.MT == id
            ].TakeOff.values
        )
        azimuth = np.deg2rad(
            results.observation.shot_data[
                results.observation.shot_data.MT == id
            ].head1.values
        )
        delta_x = np.mean(np.cos(takeoff) * np.cos(azimuth))
        delta_y = np.mean(np.cos(takeoff) * np.sin(azimuth))
        delta_z = np.mean(np.sin(azimuth))

        transponder.delta_center_position = GPPositionENU(
            east=delta_x, north=delta_y, up=delta_z
        )

    results_out = GarposResults(
        center_llh=results.site.center_llh,
        delta_center_position=results.site.delta_center_position,
        transponders=results.site.transponders,
        shot_data=results_df,
    )
    logger.loginfo("GARPOS results processed, returning results tuple")
    return results_out, results_df

class GarposHandler:
    """
    GarposHandler is a class that handles the processing and preparation of shot data for the GARPOS model.
    It includes methods for rectifying shot data, preparing shot data files, setting inversion parameters,
    generating observation parameter files, generating data files with fixed parameters, and running the GARPOS model.

    Note:
        Respective instances of this class are intended to be for individual stations only.

    Attributes:
        LIB_DIRECTORY (str): Directory path for the RayTrace library.
        LIB_RAYTRACE (str): Path to the RayTrace library.
        shotdata (TDBShotDataArray): Array containing shot data.
        site_config (SiteConfig): Configuration for the site.
        working_dir (Path): Working directory path.
        shotdata_dir (Path): Directory path for shot data.
        results_dir (Path): Directory path for results.
        inversion_params (InversionParams): Parameters for the inversion process.
        dates (list): List of unique dates from the shot data.
        coord_transformer (CoordTransformer): Coordinate transformer for converting coordinates.

    Methods:
        __init__(self, shotdata: TDBShotDataArray, site_config: SiteConfig, working_dir: Path):
            Initializes the GarposHandler with shot data, site configuration, and working directory.
        _rectify_shotdata(self, shot_data: pd.DataFrame) -> pd.DataFrame:
        prep_shotdata(self, overwrite: bool = False):
            Prepares and saves shot data for each date in the object's date list.
        set_inversion_params(self, parameters: dict | InversionParams):
            Sets inversion parameters for the model.
        _input_to_datafile(self, shot_data: Path, path: Path, n_shot: int) -> None:
        _garposfixed_to_datafile(self, inversion_params: InversionParams, path: Path) -> None:
        _run_garpos(self, date: datetime, run_id: int | str = 0) -> GarposResults:
            Runs the GARPOS model for a given date and run ID.
        run_garpos(self, date_index: int = None, run_id: int | str = 0) -> None:
            Runs the GARPOS model for a specific date or for all dates.
    """

    def __init__(
        self, shotdata: TDBShotDataArray, working_dir: Path
    ):
        """
        Initializes the class with shot data, site configuration, and working directory.
        Args:
            shotdata (TDBShotDataArray): The shot data array.
            site_config (SiteConfig): The site configuration.
            working_dir (Path): The working directory path.
        """
        garpos_fixed = GarposFixed()
        self.LIB_DIRECTORY = garpos_fixed.lib_directory
        self.LIB_RAYTRACE = garpos_fixed.lib_raytrace
        self.shotdata = shotdata
        self.working_dir = working_dir
        self.shotdata_dir = working_dir / "shotdata"
        self.shotdata_dir.mkdir(exist_ok=True, parents=True)
        self.results_dir = working_dir / "results"
        self.results_dir.mkdir(exist_ok=True, parents=True)

        self.current_campaign = None
        self.current_survey = None
        self.coord_transformer = None

    def _rectify_shotdata(self, shot_data: pd.DataFrame) -> pd.DataFrame:
        """
        Rectifies the shot data to the site local coordinate system by transforming coordinates and renaming columns.
        This method performs the following operations on the input shot data:
        1. Transforms the ECEF coordinates to ENU coordinates for two sets of points.
        2. Adds the transformed coordinates to the DataFrame.
        3. Sets default values for the "SET" and "LN" columns.
        4. Renames specific columns according to a predefined mapping.
        5. Selects and reorders the columns in the DataFrame.
        6. Validates and sorts the DataFrame by "triggerTime".

        Args:
            shot_data (pd.DataFrame): The input DataFrame containing shot data with columns
                                      "east0", "north0", "up0", "east1", "north1", "up1",
                                      "trigger_time", "hae0", "pingTime", "returnTime",
                                      "tt", "transponderID", "head0", "pitch0", "roll0",
                                      "head1", "pitch1", and "roll1".
        Returns:
            pd.DataFrame: The rectified and validated DataFrame sorted by "triggerTime".
        """

        e0, n0, u0 = self.coord_transformer.ECEF2ENU_vec(
            shot_data.east0.to_numpy(),
            shot_data.north0.to_numpy(),
            shot_data.up0.to_numpy(),
        )
        e1, n1, u1 = self.coord_transformer.ECEF2ENU_vec(
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
        shot_data["SET"] = "S01"
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
        return ObservationData.validate(shot_data, lazy=True).sort_values("triggerTime")

    def set_site_data(self, site_path: Path|str, sound_speed_path:Path|str, atd_offset:dict):
        self.site = MetaSite.from_json(site_path)
        self.sound_speed_path = sound_speed_path
        self.atd_offset = atd_offset

    def set_campaign(self, name: str):
        for campaign in self.site.campaigns:
            if campaign.name == name:
                self.current_campaign = campaign
                self.current_survey = None
                return
        raise ValueError(f"campaign {name} not found among: {[x.name for x in self.site.campaigns]}")
    
    def set_survey(self,name:str):
        for survey in self.current_campaign.surveys:
            if survey.survey_id == name:
                self.current_survey = survey
                return
        raise ValueError(f"Survey {name} not found among: {[x.survey_id for x in self.current_campaign.surveys]}")

    def prep_shotdata(self, overwrite: bool = False):
        for survey in self.campaign.surveys.values():
            benchmarks = []
            for benchmark in self.site.benchmarks:
                if benchmark.name in survey.benchmarkIDs:
                    benchmarks.append(benchmark)
            transponders = []
            for benchmark in benchmarks:
                [
                    transponders.append(transponder.id)
                    for transponder in benchmark.transponders
                ]
            if len(transponders) == 0:
                print(f"No transponders found for survey {survey.id}")
                continue
            survey_type = survey.type.replace(" ", "")
            start_doy = survey.start.timetuple().tm_yday
            end_doy = survey.end.timetuple().tm_yday
            shot_data_path = (
                self.shotdata_dir
                / f"{survey.id}_{survey_type}_{start_doy}_{end_doy}.csv"
            )
           
        logger.loginfo("Preparing shot data")
        for date in self.dates:
            year, doy = date.year, date.timetuple().tm_yday
            shot_data_path = self.shotdata_dir / f"{str(year)}_{str(doy)}.csv"
            if not shot_data_path.exists() or overwrite:
                shot_data_queried: pd.DataFrame = self.shotdata.read_df(
                    start=survey.start, end=survey.end
                )

                if shot_data_queried.empty:
                    print(
                        f"No shot data found for survey {survey.id} {survey_type} {start_doy} {end_doy}"
                    )
                    continue
   
                shot_data_rectified = self._rectify_shotdata(shot_data_queried)
                shot_data_rectified = self.subset_shots(shot_data_rectified)
                try:
                    shot_data_rectified = ShotDataFrame.validate(
                        shot_data_rectified, lazy=True
                    )
                    # Only use shotdata for transponders in the survey
                    shot_data_rectified = shot_data_rectified[
                        shot_data_rectified.MT.isin(transponders)
                    ]

                    shot_data_rectified.MT = shot_data_rectified.MT.apply(
                        lambda x: "M" + str(x) if str(x)[0].isdigit() else str(x)
                    )
            
                    shot_data_rectified.to_csv(str(shot_data_path))
                except Exception as e:
                    logger.logerr(f"Shot data for {str(year)}_{str(doy)} failed validation. Error: {e}")
                    raise ValueError(
                        f"Shot data for {survey.id} {survey_type} {start_doy} {end_doy} failed validation."
                    ) from e
                
            self.campaign.surveys[survey.id].shot_data_path = shot_data_path
  
        logger.loginfo(f"Shot data prepared and saved under {self.shotdata_dir}")  

    def set_inversion_params(self, parameters: dict | InversionParams):
        """
        Set inversion parameters for the model.
        This method updates the inversion parameters of the model using the key-value pairs
        provided in the `args` dictionary. Each key in the dictionary corresponds to an attribute
        of the `inversion_params` object, and the associated value is assigned to that attribute.

        Args:
            parameters (dict | InversionParams): A dictionary containing key-value pairs to update the inversion parameters or an InversionParams object.

        """

        if isinstance(parameters, InversionParams):
            self.inversion_params = parameters
        else:
            for key, value in parameters.items():
                setattr(self.inversion_params, key, value)

    def _input_to_datafile(
        self,
        shot_data: Path,
        path: Path,
        n_shot: int,
    ) -> None:
        """
        Generates an observation parameter file from the provided shot data and site configuration.
        Args:
            shot_data (Path): Path to the shot data CSV file.
            path (Path): Path where the output observation parameter file will be saved.
            n_shot (int): Number of shots in the shot data.
        Returns:
            None
        Raises:
            IOError: If there is an issue writing to the specified path.
        The generated file includes sections for observation parameters, data file information,
        site parameters, and model parameters. It also includes transponder data.
        """
        shot_data_df = pd.read_csv(shot_data)
        mts = [x for x in shot_data_df.MT.unique()]

        logger.loginfo("Generating observation parameter file from shot data and site configuration")
        delta_center_position: List[float] = (
            self.inversion_params.delta_center_position.get_position()
            + self.inversion_params.delta_center_position.get_std_dev()
            + [0.0, 0.0, 0.0]
        )
        atd_offset = self.site_config.atd_offset.get_offset() + [0.0, 0.0, 0.0] * 2
        date_mjd = julian.to_jd(self.site_config.date, fmt="mjd")
        position_enu = self.avg_transponder_enu.get_position()
        obs_str = f"""
    [Obs-parameter]
        Site_name   = {self.site_config.name}
        Campaign    = {self.site_config.campaign}
        Date(UTC)   = {self.site_config.date.strftime('%Y-%m-%d')}
        Date(jday)  = {date_mjd}
        Ref.Frame   = "ITRF"
        SoundSpeed  = {str(self.site_config.sound_speed_data)}

    [Data-file]
        datacsv     = {str(shot_data)}
        N_shot      = {n_shot}
        used_shot   = {0}

    [Site-parameter]
        Latitude0   = {self.avg_transponder_llh.latitude}
        Longitude0  = {self.avg_transponder_llh.longitude}
        Height0     = {self.avg_transponder_llh.height}
        Stations    = {' '.join([transponder.id for transponder in self.site_config.transponders if transponder.id in mts])}
        Center_ENU  = {position_enu[0]} {position_enu[1]} {position_enu[2]}

    [Model-parameter]
        dCentPos    = {" ".join(map(str, delta_center_position))}
        ATDoffset   = {" ".join(map(str, atd_offset))}"""

        # Add the transponder data to the string
        for transponder in self.site_config.transponders:
            if transponder.id not in mts:
                continue
            position = (
                transponder.position_enu.get_position()
                + transponder.position_enu.get_std_dev()
                + [0.0, 0.0, 0.0]
            )
            obs_str += f"""
        {transponder.id}_dPos    = {" ".join(map(str, position))}"""

        with open(path, "w") as f:
            f.write(obs_str)

        logger.loginfo(f"Observation parameter file written to {path}")

    def _garposfixed_to_datafile(
        self, inversion_params: InversionParams, path: Path
    ) -> None:
        """
        Generates a data file with fixed parameters for the inversion process.
        This method creates a configuration file with hyperparameters and inversion parameters
        required for the inversion process. The generated file is written to the specified path.
        Args:
            inversion_params (InversionParams): An instance of InversionParams containing the
                parameters for the inversion process.
            path (Path): The file path where the generated configuration file will be saved.
        Returns:
            None
        """

        logger.loginfo("Writing fixed parameters to datafile for inversion process")
        fixed_str = f"""[HyperParameters]
    # Hyperparameters
    #  When setting multiple values, ABIC-minimum HP will be searched.
    #  The delimiter for multiple HP must be "space".

    # Smoothness parameter for background perturbation (in log10 scale)
    Log_Lambda0 = {" ".join([str(x) for x in inversion_params.log_lambda])}

    # Smoothness parameter for spatial gradient ( = Lambda0 * gradLambda )
    Log_gradLambda = {inversion_params.log_gradlambda}

    # Correlation length of data for transmit time (in min.)
    mu_t = {" ".join([str(x) for x in inversion_params.mu_t])}

    # Data correlation coefficient b/w the different transponders.
    mu_mt = {" ".join([str(x) for x in inversion_params.mu_mt])}

    [Inv-parameter]
    # The path for RayTrace lib.
    lib_directory = {self.LIB_DIRECTORY}
    lib_raytrace = {self.LIB_RAYTRACE}

    # Typical Knot interval (in min.) for gamma's component (a0, a1, a2).
    #  Note ;; shorter numbers recommended, but consider the computational resources.
    knotint0 = {inversion_params.knotint0}
    knotint1 = {inversion_params.knotint1}
    knotint2 = {inversion_params.knotint2}

    # Criteria for the rejection of data (+/- rsig * Sigma).
    # if = 0, no data will be rejected during the process.
    RejectCriteria = {inversion_params.rejectcriteria}

    # Inversion type
    #  0: solve only positions
    #  1: solve only gammas (sound speed variation)
    #  2: solve both positions and gammas
    inversiontype = {inversion_params.inversiontype.value}

    # Typical measurement error for travel time.
    # (= 1.e-4 sec is recommended in 10 kHz carrier)
    traveltimescale = {inversion_params.traveltimescale}

    # Maximum loop for iteration.
    maxloop = {inversion_params.maxloop}

    # Convergence criteria for model parameters.
    ConvCriteria = {inversion_params.convcriteria}

    # Infinitesimal values to make Jacobian matrix.
    deltap = {inversion_params.deltap}
    deltab = {inversion_params.deltab}"""

        with open(path, "w") as f:
            f.write(fixed_str)
        
        logger.loginfo(f"Fixed parameters written to {path}")

    def _run_garpos(
        self,
        results_dir: Path,
        shot_data_path: Path,
        run_id: int | str = 0,
        override: bool = False,
    ) -> GarposResults:
        """
        Run the GARPOS model for a given date and run ID.

        Args:
            date (datetime): The date for which to run the GARPOS model.
            run_id (int | str, optional): The run identifier. Defaults to 0.

        Returns:
            GarposResults: The results of the GARPOS model run.

        Raises:
            AssertionError: If the shot data file does not exist for the given date.

        This method performs the following steps:
        1. Extracts the year and day of year (DOY) from the given date.
        2. Constructs the path to the shot data file and checks its existence.
        3. Reads the shot data from the CSV file.
        4. Creates a results directory for the given year and DOY.
        5. Prepares input and settings files for the GARPOS model.
        6. Runs the GARPOS model using the prepared input and settings files.
        7. Processes the GARPOS model results.
        8. Saves the processed results to a JSON file.
        9. Saves the results DataFrame to a CSV file.
        """

        assert shot_data_path.exists(), f"Shot data not found at {shot_data_path}"
        results_path = results_dir / f"_{run_id}_results.json"
        results_df_path: Path = results_dir / f"_{run_id}_results_df.csv"

        if results_path.exists() and not override:
            print(f"Results already exist for {str(results_path)}")
            return

        year, doy = date.year, date.timetuple().tm_yday
        logger.loginfo(f"Running GARPOS model for {str(year)}_{str(doy)}. Run ID: {run_id}")
        shot_data_path = self.shotdata_dir / f"{str(year)}_{str(doy)}.csv"
        assert (
            shot_data_path.exists()
        ), f"Shot data not found at {shot_data_path} for {date}"

        shot_data = pd.read_csv(shot_data_path)
        n_shot = len(shot_data)

        results_dir.mkdir(exist_ok=True, parents=True)

        input_path = results_dir / f"_{run_id}_observation.ini"
        fixed_path = results_dir / f"_{run_id}_settings.ini"
        self._input_to_datafile(shot_data_path, input_path, n_shot)
        self._garposfixed_to_datafile(self.inversion_params, fixed_path)

        print("Running GARPOS for", shot_data_path)
        rf = drive_garpos(
            str(input_path),
            str(fixed_path),
            str(results_dir) + "/",
            self.site_config.campaign + f"_{run_id}",
            13,
        )

        results = datafile_to_garposinput(rf)
        proc_results, results_df = process_garpos_results(results)

        results_df.to_csv(results_df_path, index=False)
        with open(results_path, "w") as f:
            json.dump(proc_results.model_dump(), f, indent=4)

    def _run_garpos_survey(
        self, survey_id: str, run_id: int | str = 0, override: bool = False
    ) -> None:
        try:
            survey = self.campaign.surveys[survey_id]
        except KeyError:
            raise ValueError(f"Survey {survey_id} not found")

        results_dir = self.results_dir / survey_id
        results_dir.mkdir(exist_ok=True, parents=True)
        shot_data_path = survey.shot_data_path
        if shot_data_path is None:
            print(f"No shot data found for survey {survey_id}")
            return
        with open(results_dir / "survey_meta.json", "w") as f:
            json.dump(survey.model_dump(), f, indent=4)
        self._run_garpos(results_dir, shot_data_path, run_id, override=override)

    def run_garpos(
        self, survey_id: str = None, run_id: int | str = 0, override: bool = False
    ) -> None:

        logger.loginfo(f"GARPOS model run completed for {str(year)}_{str(doy)}. Results saved at {results_path}")

        """
        Run the GARPOS model for a specific date or for all dates.
        Args:
            date_index (int, optional): The index of the date in the self.dates list to run the model for.
                                        If None, the model will be run for all dates. Defaults to None.
            run_id (int or str, optional): An identifier for the run. Defaults to 0.
        Returns:
            None
        """


        if survey_id is None:
            for survey_id in self.campaign.surveys.keys():
                self._run_garpos_survey(survey_id, run_id, override=override)

        logger.loginfo(f"Running GARPOS model for date(s) provided. Run ID: {run_id}")
        if date_index is None:
            for date in self.dates:
                self._run_garpos(date,run_id=run_id)
        else:
            self._run_garpos_survey(survey_id, run_id, override=override)

    def plot_ts_results(
        self, survey_id: str, run_id: int | str = 0, res_filter: float = 10
    ) -> None:
        
        """
        Plots the time series results for a given survey.
        Parameters:
        -----------
        survey_id : str
            The ID of the survey to plot results for.
        run_id : int or str, optional
            The run ID of the survey results to plot. Default is 0.
        res_filter : float, optional
            The residual filter value to filter outrageous values (m). Default is 10.
        Returns:
        --------
        None

        Notes:
        ------
        - The function reads survey results from a JSON file and a CSV file.
        - It filters the results based on the residual range.
        - It generates multiple plots including scatter plots, line plots, box plots, and histograms.
        - The plots include information about the delta center position and transponder positions.
        """

        print("Plotting results for survey ", survey_id)
        try:
            survey = Survey(**dict(self.campaign.surveys[survey_id]))
        except KeyError:
            raise ValueError(f"Survey {survey_id} not found")
        
        results_dir = self.results_dir / survey.id
        results_path = results_dir / f"_{run_id}_results.json"
        with open(results_path, "r") as f:
            results = json.load(f)
        transponders = []
        arrayinfo = GPPositionENU.model_validate(results["delta_center_position"])
        for transponder in results["transponders"]:
            _transponder_ = GPTransponder.model_validate(transponder)
            transponders.append(_transponder_)
        results_df_raw = pd.read_csv(results_dir / f"_{run_id}_results_df.csv")
        results_df_raw = ShotDataFrame.validate(results_df_raw, lazy=True)
        results_df_raw["time"] = results_df_raw.ST.apply(
            lambda x: datetime.fromtimestamp(x)
        )
        df_filter = results_df_raw["ResiRange"].abs() < res_filter
        results_df = results_df_raw[df_filter]
        unique_ids = results_df["MT"].unique()

        plt.figsize = (32, 32)
        plt.suptitle(f"Survey {survey.id} Results")
        gs = gridspec.GridSpec(13, 16)
        figure_text = "Delta Center Position\n"
        dpos = arrayinfo.get_position()
        figure_text += f"Array :  East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"
        for id, transponder in enumerate(transponders):
            dpos = transponder.delta_center_position.get_position()
            figure_text += f"TSP {transponder.id} : East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"

        print(figure_text)
        ax3 = plt.subplot(gs[6:, 8:])
        ax3.set_aspect("equal", "box")
        ax3.set_xlabel("East (m)")
        ax3.set_ylabel("North (m)", labelpad=-1)
        colormap_times = results_df_raw.ST.to_numpy()
        colormap_times_scaled = (colormap_times - colormap_times.min()) / 3600
        norm = Normalize(
            vmin=0,
            vmax=(colormap_times.max() - colormap_times.min()) / 3600,
        )
        sc = ax3.scatter(
            results_df_raw["ant_e0"],
            results_df_raw["ant_n0"],
            c=colormap_times_scaled,
            cmap="viridis",
            label="Vessel",
            norm=norm,
            alpha=0.25,
        )
        ax3.scatter(0, 0, label="Origin", color="magenta", s=100)
        ax1 = plt.subplot(gs[1:5, :])
        points = np.array(
            [
                mdates.date2num(results_df["time"]),
                np.zeros(len(results_df["time"])),
            ]
        ).T.reshape(-1, 1, 2)
        segments = np.concatenate([points[:-1], points[1:]], axis=1)
        lc = LineCollection(segments, cmap="viridis", norm=norm, linewidth=5, zorder=10)
        lc.set_array(colormap_times_scaled)
        ax1.add_collection(lc)
        for i, unique_id in enumerate(unique_ids):
            df = results_df[results_df["MT"] == unique_id].sort_values("time")
            ax1.plot(
                df["time"],
                df["ResiRange"],
                label=f"{unique_id}",
                color=colors[i],
                linewidth=1,
                zorder=i,
                alpha=0.75,
            )
        ax1.set_xlabel("Time - Month / Day / Hour")
        ax1.set_ylabel("Residuals - Range (M)", labelpad=-1)
        ax1.xaxis.set_label_position("top")
        ax1.xaxis.set_ticks_position("top")
        ax1.legend()
        for transponder in transponders:
            idx = unique_ids.tolist().index(transponder.id)
            ax3.scatter(
                transponder.position_enu.east,
                transponder.position_enu.north,
                label=f"{transponder.id}",
                color=colors[idx],
                s=100,
            )
        cbar = plt.colorbar(sc, label="Time (hr)", norm=norm)
        ax3.legend()
        ax2 = plt.subplot(gs[6:9, :7])
        resiRange = results_df_raw["ResiRange"]
        resiRange_np = resiRange.to_numpy()
        resiRange_filter = np.abs(resiRange_np) < 50
        resiRange = resiRange[resiRange_filter]
        flier_props = dict(marker=".", markerfacecolor="r", markersize=5, alpha=0.25)
        ax2.boxplot(resiRange.to_numpy(), vert=False, flierprops=flier_props)
        median = resiRange.median()
        # Get the 1st and 2nd interquartile range
        q1 = resiRange.quantile(0.25)
        q3 = resiRange.quantile(0.75)
        ax2.text(
            0.5,
            1.2,
            f"Median: {median:.2f} , IQR 1: {q1:.2f}, IQR 3: {q3:.2f}",
            fontsize=10,
            verticalalignment="center",
            horizontalalignment="center",
        )
        ax2.set_xlabel("Residual Range (m)", labelpad=-1)
        # Place ax2 x ticks on top
        ax2.yaxis.set_visible(False)
        ax2.set_title("Box Plot of Residual Range Values")
        bins = np.arange(-res_filter, res_filter, 0.05)
        counts, bins = np.histogram(resiRange_np, bins=bins, density=True)
        ax4 = plt.subplot(gs[10:, :7])
        ax4.sharex(ax2)
        ax4.hist(bins[:-1], bins, weights=counts, edgecolor="black")
        ax4.axvline(median, color="blue", linestyle="-", label=f"Median: {median:.3f}")
        ax4.set_xlabel("Residual Range (m)", labelpad=-1)
        ax4.set_ylabel("Frequency")
        ax4.set_title(
            f"Histogram of Residual Range Values, within {res_filter:.1f} meters and sorted in .05m bins"
        )
        ax4.legend()
        plt.show()
