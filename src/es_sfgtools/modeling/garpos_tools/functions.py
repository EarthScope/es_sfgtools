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
import json
from matplotlib.colors import Normalize
from matplotlib.collections import LineCollection
import matplotlib.dates as mdates
import seaborn as sns
sns.set_theme()
import matplotlib.gridspec as gridspec

from sfg_metadata.metadata.src.site import Site


from es_sfgtools.processing.assets.observables import (
    ShotDataFrame,
    SoundVelocityDataFrame,
)

from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposInput,
    GarposFixed,
    InversionParams,
    InversionType,
    ObservationData,
    GarposObservationOutput,
    GPPositionENU,
    GPATDOffset,
    GPTransponder,
    GPPositionLLH,
)
from es_sfgtools.utils.loggers import GarposLogger as logger

from .load_utils import load_drive_garpos

try:
    drive_garpos = load_drive_garpos()
except Exception as e:
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

def rectify_shotdata(coord_transformer: CoordTransformer, shot_data: pd.DataFrame) -> pd.DataFrame:
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
