import pandera as pa
from pandera.typing import DataFrame
from pathlib import Path
from typing import List,Tuple,Union
import pandas as pd
from configparser import ConfigParser
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import sys
import os
import pyproj
import pymap3d as pm
import math
import julian
import logging
from scipy.stats import hmean as harmonic_mean
from sklearn.ensemble import RandomForestRegressor
from scipy.interpolate import RBFInterpolator,CubicSpline
from functools import partial

from es_sfgtools.processing.assets.observables import AcousticDataFrame,PositionDataFrame,ShotDataFrame,SoundVelocityDataFrame
from es_sfgtools.processing.assets.siteconfig import PositionENU,ATDOffset,Transponder,PositionLLH,SiteConfig
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposInput,
    GarposObservation,
    GarposSite,
    GarposFixed,
    InversionParams,
    InversionType,
    ObservationData,
    GarposObservationOutput,
    GarposResults
)


from garpos import drive_garpos

logger = logging.getLogger(__name__)

ECEF = "epsg:4978"
LLH = "epsg:4326"

_yxz2llh = pyproj.Transformer.from_crs(ECEF, LLH)
_llh2yxz = pyproj.Transformer.from_crs(LLH, ECEF)


def xyz2llh(X,Y ,Z,**kwargs):
    lon, lat, z = _yxz2llh.transform(X,Y, Z)
    return {"lat": lat, "lon": lon, "hgt": z}


def llh2xyz(lat, lon, hgt,**kwargs):
    x,y,z = _llh2yxz.transform(lat, lon, hgt)

    return {"X": x, "Y": y, "Z": z}


def LLHDEG2DEC(lat: List[float], lon: List[float]) -> List[float]:
    """
    Convert lat and lon from degrees to decimal
        >>> lat
        [40,26,15]
    """
    # process lat
    degrees, minutes, seconds = (
        float(lat[0]),
        float(lat[1]) / 60,
        float(lat[2]) / (60**2),
    )
    lat_decimal = degrees + minutes + seconds

    # process lon
    degrees, minutes, seconds = (
        float(lon[0]),
        float(lon[1]) / 60,
        float(lon[2]) / (60**2),
    )
    lon_decimal = degrees + minutes + seconds

    return [lat_decimal, lon_decimal]


def __llh2xyz(lt, ln, hgt):
    """
    Convert lat, long, height in WGS84 to ECEF (X,Y,Z).
    lat and long given in decimal degrees.
    height should be given in meters

    Parameters
    ----------
    lt : float
            Latitude in degrees
    ln : float
            Longitude in degrees
    hgt : float
            Height in meters

    Returns
    -------
    X : float
            X (m) in ECEF
    Y : float
            Y (m) in ECEF
    Z : float
            Z (m) in ECEF
    """
    lat = lt * math.pi / 180.0
    lon = ln * math.pi / 180.0
    a = 6378137.0  # earth semimajor axis in meters
    f = 1.0 / 298.257223563  # reciprocal flattening
    e2 = 2.0 * f - f**2  # eccentricity squared

    chi = (1.0 - e2 * (math.sin(lat)) ** 2) ** 0.5
    b = a * (1.0 - e2)

    X = (a / chi + hgt) * math.cos(lat) * math.cos(lon)
    Y = (a / chi + hgt) * math.cos(lat) * math.sin(lon)
    Z = (b / chi + hgt) * math.sin(lat)

    return X, Y, Z


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
    def __init__(self,pos_llh):
        if isinstance(pos_llh,list):
            self.lat0 = pos_llh[0]
            self.lon0 = pos_llh[1]
            self.hgt0 = pos_llh[2]
        else:
            self.lat0 = pos_llh.latitude
            self.lon0 = pos_llh.longitude
            self.hgt0 = pos_llh.height

        self.X0,self.Y0,self.Z0 = pm.geodetic2ecef(self.lat0,self.lon0,self.hgt0)
    def XYZ2ENU(self,X,Y,Z,**kwargs):
        dX,dY,dZ = X-self.X0,Y-self.Y0,Z-self.Z0
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
    def LLH2ENU(self,lat,lon,hgt,**kwargs):
        X,Y,Z = pm.geodetic2ecef(lat,lon,hgt)
        dX,dY,dZ = X-self.X0,Y-self.Y0,Z-self.Z0
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
    
    def LLH2ENU_vec(self,lat:np.ndarray,lon:np.ndarray,hgt:np.ndarray) -> Tuple[np.ndarray,np.ndarray,np.ndarray]:
        X,Y,Z = pm.geodetic2ecef(lat,lon,hgt)
        dX,dY,dZ = X-self.X0,Y-self.Y0,Z-self.Z0
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
    def ECEF2ENU_vec(self,X:np.ndarray,Y:np.ndarray,Z:np.ndarray) -> Tuple[np.ndarray,np.ndarray,np.ndarray]:
        dX,dY,dZ = X-self.X0,Y-self.Y0,Z-self.Z0
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

def garposinput_to_datafile(garpos_input:GarposInput,path:Path):
    """
    Write a GarposInput to a datafile
    """
    
    # Write the data file
    center_enu: List[float] = garpos_input.site.center_enu.get_position()
    delta_center_position: List[float] = garpos_input.site.delta_center_position.get_position() + garpos_input.site.delta_center_position.get_std_dev() + [0.0, 0.0, 0.0]
    atd_offset = garpos_input.site.atd_offset.get_offset() + [0.0, 0.0, 0.0]*2

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


def datafile_to_garposinput(path:Path) -> GarposInput:
    """
    Read a GarposInput from a datafile
    """
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
        position = PositionENU(
            east=east_value,
            east_sigma =east_sigma,
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
            transponder = Transponder(id=transponder_id, position_enu=position)
            transponder_list.append(transponder)
        if "dcentpos" in key:
            delta_center_position = position
        if "atdoffset" in key:
            atd_offset = ATDOffset(
                forward=position.east,
                rightward=position.north,
                downward=position.up,
            )
        
    site = GarposSite(
        name=observation_section["Site_name"],
        center_llh=PositionLLH(
            latitude=float(site_section["Latitude0"]),
            longitude=float(site_section["Longitude0"]),
            height=float(site_section["Height0"]),
        ),
        center_enu=PositionENU(
            east=float(site_section["Center_ENU"].split()[0]),
            north=float(site_section["Center_ENU"].split()[1]),
            up=float(site_section["Center_ENU"].split()[2]),
        ),
        atd_offset=atd_offset,
        transponders=transponder_list,
        delta_center_position=delta_center_position,
    )

    # Now handle shot_data_file and sound_speed_file
    shot_data_file = data_section["datacsv"]
    sound_speed_file = observation_section["soundspeed"]

    try:
        shot_data_results = ObservationData.validate(pd.read_csv(shot_data_file))
    except:
        shot_data_results = ObservationData(pd.read_csv(shot_data_file,skiprows=1))
    sound_speed_results = SoundVelocityDataFrame(pd.read_csv(sound_speed_file))

    # Populate GarposObservation
   
    observation = GarposObservation(
        campaign=observation_section["campaign"],
        date_utc=(date_utc := datetime.strptime(observation_section["date(UTC)"], "%Y-%m-%d")),
        date_mjd=julian.to_jd(date_utc, fmt="jd"),
        ref_frame=observation_section["ref.frame"],
        shot_data=shot_data_results,
        sound_speed_data=sound_speed_results,
    )

    return GarposInput(site=site, observation=observation, shot_data_file=shot_data_file, sound_speed_file=sound_speed_file)


def garposfixed_to_datafile(garpos_fixed,path:Path) -> None:
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


def garposfixed_from_datafile(path:Path) -> GarposFixed:
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
        inversiontype=InversionType(int(inv_parameters.get("inversiontype",0))),
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
    return garpos_fixed

def avg_transponder_position(
    transponders: List[Transponder],
) -> Tuple[PositionENU, PositionLLH]:
    """
    Calculate the average position of the transponders
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

    min_pos_llh = np.min(pos_array_llh, axis=0).tolist()

    out_pos_llh = PositionLLH(
        latitude=avg_pos_llh[0], longitude=avg_pos_llh[1], height=avg_pos_llh[2]
    )
    out_pos_enu = PositionENU(east=avg_pos_enu[0], north=avg_pos_enu[1], up=avg_pos_enu[2])

    return out_pos_enu, out_pos_llh

def plot_enu_llh_side_by_side(garpos_input:GarposInput):
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

def sitedata_to_garpossite(site_config:SiteConfig,atd_offset:ATDOffset) -> GarposSite:
    site_center_llh = site_config.position_llh
    coord_transformer = CoordTransformer(site_center_llh)

    for transponder in site_config.transponders:
        lat, lon, hgt = (
            transponder.position_llh.latitude,
            transponder.position_llh.longitude,
            transponder.position_llh.height,
        )

        e, n, u = coord_transformer.LLH2ENU(lat, lon, hgt)

        transponder.position_enu = PositionENU(east=e, north=n, up=u)
        transponder.id = "M" + str(transponder.id) if str(transponder.id)[0].isdigit() else str(transponder.id)


    transponder_avg_enu, _ = avg_transponder_position(site_config.transponders)
    #

    delta_center_position = PositionENU()
    delta_center_position.east_sigma = 1.0
    delta_center_position.north_sigma = 1.0
    delta_center_position.up_sigma = 0.0

    site_config.name = "NCB1"
    garpos_site = GarposSite(
        name=site_config.name,
        center_llh=site_center_llh,
        center_enu=transponder_avg_enu,
        atd_offset=atd_offset,
        transponders=site_config.transponders,
        delta_center_position=delta_center_position,
    )

    return garpos_site

def rectify_shotdata_site(
        site_config:SiteConfig,
        shot_data:DataFrame[ObservationData]) -> Tuple[SiteConfig,DataFrame[ShotDataFrame]]:
    
    site_config = site_config.copy() # avoid aliasing
    coord_transformer = CoordTransformer(site_config.position_llh)
    e0,n0,u0 = coord_transformer.ECEF2ENU_vec(shot_data.east0.to_numpy(),shot_data.north0.to_numpy(),shot_data.up0.to_numpy())
    e1,n1,u1 = coord_transformer.ECEF2ENU_vec(shot_data.east1.to_numpy(),shot_data.north1.to_numpy(),shot_data.up1.to_numpy())
    shot_data["ant_e0"] = e0
    shot_data["ant_n0"] = n0
    shot_data["ant_u0"] = u0
    shot_data["ant_e1"] = e1
    shot_data["ant_n1"] = n1
    shot_data["ant_u1"] = u1
    shot_data["SET"] = "S01"
    shot_data["LN"] = "L01"
    rename_dict = {
        "trigger_time":"triggertime",
        "hae0":"height",
        "pingTime":"ST",
        "returnTime":"RT",
        "tt":"TT",
        "transponderID":"MT",
    }
    shot_data = (
        shot_data.rename(columns=rename_dict)
        .loc[
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
    )
    for transponder in site_config.transponders:
        lat, lon, hgt = (
            transponder.position_llh.latitude,
            transponder.position_llh.longitude,
            transponder.position_llh.height,
        )

        e, n, u = coord_transformer.LLH2ENU(lat, lon, hgt)

        transponder.position_enu = PositionENU(east=e, north=n, up=u)
        transponder.id = "M" + str(transponder.id) if str(transponder.id)[0].isdigit() else str(transponder.id)

    return site_config,ObservationData.validate(shot_data,lazy=True).sort_values("triggerTime")

def dev_garpos_input_from_site_obs(
    site_config:SiteConfig,
    shot_data:pd.DataFrame,
):
    garpos_site: GarposSite = sitedata_to_garpossite(site_config,atd_offset)
    coord_transformer = CoordTransformer(site_config.position_llh)
    e0,n0,u0 = coord_transformer.ECEF2ENU_vec(shot_data.east0.to_numpy(),shot_data.north0.to_numpy(),shot_data.up0.to_numpy())
    e1,n1,u1 = coord_transformer.ECEF2ENU_vec(shot_data.east1.to_numpy(),shot_data.north1.to_numpy(),shot_data.up1.to_numpy())
    date_utc = shot_data.triggerTime.min()
    date_mjd = julian.to_jd(date_utc, fmt="mjd")
    shot_data["ant_e0"] = e0
    shot_data["ant_n0"] = n0
    shot_data["ant_u0"] = u0
    shot_data["ant_e1"] = e1
    shot_data["ant_n1"] = n1
    shot_data["ant_u1"] = u1
    shot_data["SET"] = "S01"
    shot_data["LN"] = "L01"
    rename_dict = {
        "trigger_time":"triggertime",
        "hae0":"height",
        "pingTime":"ST",
        "returnTime":"RT",
        "tt":"TT",
        "transponderID":"MT",
    }
    shot_data = (
        shot_data.rename(columns=rename_dict)
        .loc[
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
    )

    shot_data = ObservationData.validate(shot_data,lazy=True).sort_values("triggerTime")
    garpos_observation = GarposObservation(
        campaign=site_config.name,
        date_utc=date_utc,
        date_mjd=date_mjd,
        ref_frame="ITRF",
        shot_data=shot_data,
        sound_speed_data=sound_velocity,
    )
    garpos_input = GarposInput(observation=garpos_observation, site=garpos_site)
    return garpos_input

# def garpos_input_from_site_obs(
#         site_config:SiteConfig,
#         atd_offset:ATDOffset,
#         gnss_data:DataFrame[PositionDataFrame],
#         imu_data:DataFrame[IMUDataFrame],
#         acoustic_data:DataFrame[AcousticDataFrame],
#         sound_velocity:DataFrame[SoundVelocityDataFrame]
#     ) -> GarposInput:

#     gnss_data.longitude = gnss_data.longitude.apply(lambda x: x if x < 180 else x - 360)
#     gnss_data.time = pd.to_datetime(gnss_data.time,format='mixed')
#     imu_data.Time = pd.to_datetime(imu_data.Time, format="mixed")
#     acoustic_data.TriggerTime = pd.to_datetime(
#         acoustic_data.TriggerTime, format="mixed"
#     )

#     gnss_data = PositionDataFrame.validate(gnss_data,lazy=True)
#     imu_data = IMUDataFrame.validate(imu_data,lazy=True)
#     acoustic_data = AcousticDataFrame.validate(acoustic_data,lazy=True)
#     sound_velocity = SoundVelocityDataFrame.validate(sound_velocity,lazy=True)

#     gnss_data.sort_values("time",inplace=True)
#     imu_data.sort_values("Time",inplace=True)
#     acoustic_data.sort_values("TriggerTime",inplace=True)

#     garpos_site: GarposSite = sitedata_to_garpossite(site_config,atd_offset)
#     coord_transformer = CoordTransformer(site_config.position_llh)

#     e,n,u = coord_transformer.LLH2ENU_vec(gnss_data.latitude.to_numpy(),gnss_data.longitude.to_numpy(),gnss_data.height.to_numpy())
#     gnss_data["x"] = e
#     gnss_data["y"] = n
#     gnss_data["z"] = u

#     date_utc = gnss_data.time.min()
#     date_mjd = julian.to_jd(date_utc, fmt="mjd")
#     shot_data: DataFrame[ObservationData] = merge_to_shotdata(
#         acoustic=acoustic_data, imu=imu_data, gnss=gnss_data
#     )
#     print(f"Shot data: {shot_data.shape[0]} Merged From {acoustic_data.shape[0]} Acoustic, {imu_data.shape[0]} IMU and {gnss_data.shape[0]} GNSS")
#     garpos_observation = GarposObservation(
#         campaign=site_config.name,
#         date_utc=date_utc,
#         date_mjd=date_mjd,
#         ref_frame="ITRF",
#         shot_data=shot_data,
#         sound_speed_data=sound_velocity,
#     )
#     garpos_input = GarposInput(
#         observation=garpos_observation, site=garpos_site)
#     return garpos_input

def process_garpos_results(results:GarposInput) -> GarposResults:
    # Process garpos results to get delta x,y,z and relevant fields

    # Get the harmonic mean of the svp data, and use that to convert ResiTT to meters
    speed_mean = harmonic_mean(results.observation.sound_speed_data.speed.values)
    range_residuals = results.observation.shot_data.ResiTT.values * speed_mean /2

    results_df = results.observation.shot_data
    results_df["ResiRange"] = range_residuals
    results_df = GarposObservationOutput.validate(results_df,lazy=True)
    # For each transponder, get the delta x,y,and z respectively
   
    for transponder in results.site.transponders:
        id = transponder.id
        takeoff = np.deg2rad(results.observation.shot_data[results.observation.shot_data.MT == id].TakeOff.values)
        azimuth = np.deg2rad(results.observation.shot_data[results.observation.shot_data.MT == id].head1.values)
        delta_x = np.mean(np.cos(takeoff)*np.cos(azimuth))
        delta_y = np.mean(np.cos(takeoff)*np.sin(azimuth))
        delta_z = np.mean(np.sin(azimuth))

        transponder.delta_center_position = PositionENU(east=delta_x,north=delta_y,up=delta_z)
    
    results_out = GarposResults(
        center_llh=results.site.center_llh,
        delta_center_position=results.site.delta_center_position,
        transponders=results.site.transponders,
        shot_data=results_df
    )

    return results_out

def dev_main(
    site_config:SiteConfig,
    hyper_params:InversionParams,
    shot_data:Union[str,Path],
    working_dir:Path=Path("/tmp/garpos/")
) -> GarposResults:

    working_dir.mkdir(exist_ok=True)
    try:
        shot_data = ShotDataFrame.validate(pd.read_csv(shot_data),lazy=True)
    except Exception as e:
        logger.error(f"ShotDataFrame - Error reading shot data: {e}")
        return None
    # Add "M" to transponder ids if they are numbers

    # process the shot data
    site_config_rectified,shot_data_rectified = rectify_shotdata_site(site_config,shot_data)
    shot_data_rectified_path = working_dir / "rectified_shot_data.csv"
    shot_data_rectified.to_csv(shot_data_rectified_path,index=False)

    avg_enu,avg_llh = avg_transponder_position(site_config_rectified.transponders)

    garpos_site = GarposSite(
        name=site_config.name,
        center_llh=site_config.position_llh,
        center_enu=avg_enu,
        atd_offset=site_config.atd_offset,
        transponders=site_config.transponders,
        delta_center_position=hyper_params.delta_center_position,
    )
    garpos_observation = GarposObservation(
        campaign=site_config.campaign,
        date_utc=site_config.date,
        date_mjd=julian.to_jd(site_config.date, fmt="jd"),
        ref_frame="ITRF",
        shot_data=shot_data_rectified,
        sound_speed_data=pd.read_csv(site_config.sound_speed_data)
    )
    garpos_input = GarposInput(
        observation=garpos_observation,
        site=garpos_site
    )
    garpos_fixed = GarposFixed()
    garpos_fixed.inversion_params = hyper_params

    working_dir.mkdir(exist_ok=True)

    results_dir = working_dir / "results"
    results_dir.mkdir(exist_ok=True)

    input_path = working_dir / "observation.ini"
    fixed_path = working_dir / "settings.ini"

    garposinput_to_datafile(garpos_input, input_path)
    garposfixed_to_datafile(garpos_fixed, fixed_path)

    rf = drive_garpos(str(input_path), str(fixed_path), str(results_dir), "test", 13)

    results = datafile_to_garposinput(rf)
    proc_results = process_garpos_results(results)

    return proc_results


def main(
    input: GarposInput, fixed: GarposFixed,working_dir:Path=Path("/tmp/garpos/")
) -> GarposObservationOutput:
   
    working_dir.mkdir(exist_ok=True)

    results_dir = working_dir / "results"
    results_dir.mkdir(exist_ok=True)

    input_path = working_dir / "observation.ini"
    fixed_path = working_dir / "settings.ini"

    garposinput_to_datafile(input, input_path)
    garposfixed_to_datafile(fixed, fixed_path)

    rf = drive_garpos(str(input_path), str(fixed_path), str(results_dir), "test", 13)

    results = datafile_to_garposinput(rf)
    proc_results = process_garpos_results(results)

    return proc_results
