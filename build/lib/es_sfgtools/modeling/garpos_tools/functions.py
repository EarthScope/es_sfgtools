from pandera.typing import DataFrame
from pathlib import Path
from typing import List,Tuple
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
from scipy.stats import hmean as harmonic_mean

from es_sfgtools.processing.schemas.observables import AcousticDataFrame, IMUDataFrame, PositionDataFrame, SoundVelocityDataFrame
from es_sfgtools.processing.schemas.site_config import PositionENU,ATDOffset,Transponder,PositionLLH,SiteConfig
from src.es_sfgtools.modeling.garpos_tools.schemas import GarposInput,GarposObservation,GarposSite,GarposFixed,InversionParams,InversionType,ObservationData


from garpos import drive_garpos

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

def merge_to_shotdata(
    acoustic: DataFrame[AcousticDataFrame],
    imu: DataFrame[IMUDataFrame],
    gnss: DataFrame[PositionDataFrame],
) -> pd.DataFrame:

    acoustic = acoustic.reset_index()
    acoustic.columns = acoustic.columns.str.lower()
    imu.columns = imu.columns.str.lower()
    gnss.columns = gnss.columns.str.lower()

    # create a column for returntime in acoustic thats in datetime format
    # modified julian date -> datetime
    acoustic["time"] = acoustic["triggertime"]

    gnss["time"] = pd.to_datetime(gnss["time"])
    imu["time"] = pd.to_datetime(imu["time"])
    acoustic["time"] = pd.to_datetime(acoustic["time"])

    acoustic_day_start = acoustic["time"].apply(lambda x: x.replace(hour=0, minute=0, second=0,microsecond=0))
    acoustic["returntime_dt"] = acoustic["returntime"].apply(
        lambda x: pd.Timedelta(seconds=x)
    ) + acoustic_day_start
    acoustic["pingtime_dt"] = acoustic["pingtime"].apply(
        lambda x: pd.Timedelta(seconds=x)
    ) + acoustic_day_start

    """
            IMU:
            ,Time,Latitude,Longitude,Height,NorthVelocity,EastVelocity,UpVelocity,Roll,Pitch,Azimuth
            0,2018-06-05 00:00:00.050,54.33236627535,-158.46946390983,12.6195,-0.0078,0.2226,-0.2324,-0.537771604,7.053161625,60.43386426
            1,2018-06-05 00:00:00.100,54.33236624129,-158.46946374432,12.6089,-0.0972,0.1926,-0.1779,-0.174584708,7.267122425,60.364148634
            2,2018-06-05 00:00:00.150,54.33236617274,-158.46946360741,12.6013,-0.188,0.1696,-0.1188,0.411828413,7.418472712,60.36250395
            3,2018-06-05 00:00:00.200,54.33236606896,-158.46946349718,12.5963,-0.2655,0.1306,-0.0671,1.123918267,7.526855965,60.388641956
            4,2018-06-05 00:00:00.250,54.33236593335,-158.46946342109,12.5938,-0.3355,0.0833,-0.0197,1.869285234,7.608214088,60.424503402
            5,2018-06-05 00:00:00.300,54.33236577218,-158.46946338987,12.5935,-0.391,0.0248,0.0265,2.563764239,7.672790742,60.466341231
            6,2018-06-05 00:00:00.350,54.33236558986,-158.46946340787,12.5957,-0.4335,-0.0411,0.0764,3.122817073,7.71598298,60.49379062
            7,2018-06-05 00:00:00.400,54.33236539136,-158.46946347742,12.6007,-0.4673,-0.1119,0.1269,3.490824818,7.730072669,60.49884816
            8,2018-06-05 00:00:00.450,54.33236518435,-158.46946360571,12.6088,-0.4808,-0.1876,0.1932,3.576496104,7.689833437,60.467685238
            9,2018-06-05 00:00:00.500,54.3323649729,-158.46946378839,12.6208,-0.4871,-0.2585,0.2708,3.396767019,7.563953759,60.409343007
            10,2018-06-05 00:00:00.550,54.33236476027,-158.46946402185,12.6369,-0.4891,-0.3229,0.3485,2.973482584,7.32876294,60.332332277

            Acoustic:
            TriggerTime,TransponderID,PingTime,ReturnTime,TwoWayTravelTime,DecibalVoltage,CorrelationScore
            2018-06-05 00:00:14,5209,58274.000174768735,58274.00022122925,4.014189,-12,55
            2018-06-05 00:00:14,5210,58274.000174768735,58274.00022204121,4.084342,-12,50
            2018-06-05 00:00:14,5211,58274.000174768735,58274.000174768735,0.0,0,25
            2018-06-05 00:00:14,5212,58274.000174768735,58274.000174768735,0.0,0,25
            2018-06-05 00:00:29,5209,58274.000348379835,58274.00039485291,4.015274,-9,60
            2018-06-05 00:00:29,5210,58274.000348379835,58274.00039566915,4.085797,-9,40
            2018-06-05 00:00:29,5211,58274.000348379835,58274.00039847627,4.328332,-12,65
            2018-06-05 00:00:29,5212,58274.000348379835,58274.000348379835,0.0,0,25
            2018-06-05 00:00:44,5209,58274.000521990936,58274.000568462034,4.015103,-9,55

            GNSS:
            ,x,y,z,latitude,longitude,height,number_of_satellites,pdop,time
            0,-3467151.069,-1367881.2187,5158410.5605,54.33236416081,201.53052300364,10.6455,8,1.69,2018-06-05 00:00:00.199999
            1,-3467151.1217,-1367881.249,5158410.5177,54.33236349803,201.53052313896,10.6457,8,1.69,2018-06-05 00:00:00.399998
            2,-3467151.2402,-1367881.2424,5158410.5137,54.33236269053,201.53052237697,10.7054,8,1.69,2018-06-05 00:00:00.599997
            3,-3467151.4095,-1367881.211,5158410.5603,54.33236186872,201.53052097264,10.8283,8,1.69,2018-06-05 00:00:00.799996
            4,-3467151.6101,-1367881.1993,5158410.6043,54.33236076895,201.53051967334,10.9704,8,1.69,2018-06-05 00:00:00.999995
            5,-3467151.8074,-1367881.1811,5158410.6112,54.33235951384,201.53051830044,11.0791,8,1.69,2018-06-05 00:00:01.199993

            --->
            OUTPUT:
            ,SET,LN,MT,TT,ResiTT,TakeOff,gamma,flag,ST,ant_e0,ant_n0,ant_u0,head0,pitch0,roll0,RT,ant_e1,ant_n1,ant_u1,head1,pitch1,roll1
            0,S01,L01,M11,2.289306,0.0,0.0,0.0,False,30072.395125,-27.85291,1473.14423,14.73469,176.47,0.59,-1.39,30075.74594,-26.70998,1462.01803,14.32703,177.07,-0.5,-1.1
            1,S01,L01,M13,3.12669,0.0,0.0,0.0,False,30092.395725,-22.08296,1412.88729,14.59827,188.24,0.41,-2.13,30096.58392,-22.3514,1401.77938,14.65401,190.61,-0.1,-2.14
            2,S01,L01,M14,2.702555,0.0,0.0,0.0,False,30093.48579,-22.25377,1409.87685,14.67772,188.93,0.15,-1.7,30097.24985,-22.38458,1399.96509,14.55534,190.82,-0.39,-2.21
            3,S01,L01,M14,2.68107,0.0,0.0,0.0,False,30102.396135,-23.25514,1387.38992,14.75355,192.39,0.1,-1.79,30106.13871,-23.96613,1378.4627,14.58135,192.92,0.21,-1.7

    """

    # Merge GNSS and IMU data
    merged_gnss_imu = pd.merge_asof(
        left=gnss, right=imu.loc[:, ["time", "roll", "pitch", "azimuth"]], on="time"
    ).loc[
        :,
        [
            "time",
            "x",
            "y",
            "z",
            "latitude",
            "longitude",
            "height",
            "roll",
            "pitch",
            "azimuth",
        ],
    ]

    shot_trigger_merged = pd.merge_asof(
        left=acoustic.sort_values(by="pingtime_dt").drop(columns=["time"]),
        right=merged_gnss_imu,
        left_on="pingtime_dt",
        right_on="time",
    )
    shot_return_merged = pd.merge_asof(
        left=acoustic.sort_values(by="returntime_dt").drop(columns=["time"]),
        right=merged_gnss_imu,
        left_on="returntime_dt",
        right_on="time",
    )

    # rename shot_trigger columns
    shot_trigger_merged.rename(
        columns={
            "transponderid": "MT",
            "twowaytraveltime": "TT",
            "x": "ant_e0",
            "y": "ant_n0",
            "z": "ant_u0",
            "azimuth": "head0",
            "pitch": "pitch0",
            "roll": "roll0",
            "pingtime": "ST",
            "returntime": "RT",
        },
        inplace=True,
    )
    shot_return_merged.rename(
        columns={
            "transponderid": "MT",
            "twowaytraveltime": "TT",
            "x": "ant_e1",
            "y": "ant_n1",
            "z": "ant_u1",
            "azimuth": "head1",
            "pitch": "pitch1",
            "roll": "roll1",
            "returntime": "RT",
            "pingtime": "ST",
        },
        inplace=True,
    )

    # shot_return_merged["time"] = shot_return_merged[
    #     "returntime_dt"
    # ] - shot_return_merged.RT.apply(lambda x: pd.Timedelta(seconds=x))
    # shot_trigger_merged["time"] = shot_trigger_merged["pingtime_dt"]

    output_df = pd.merge(
        left=shot_trigger_merged.loc[
            :,
            [
                "latitude",
                "longitude",
                "height",
                "ant_e0",
                "ant_n0",
                "ant_u0",
                "head0",
                "pitch0",
                "roll0",
                "triggertime",
                "MT",
                "TT",
            ],
        ],
        right=shot_return_merged,
        how="left",
        on=["triggertime", "MT", "TT"],
    ).dropna()

    # remove _x from lattitude and longitude
    output_df = output_df.rename(
        columns={
            "latitude_x": "latitude",
            "longitude_x": "longitude",
            "height_x": "height",
        }
    )

    output_df = output_df.loc[
        :,
        [   "triggertime",
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
            "latitude",
            "longitude",
            "height",
        ],
    ]
    output_df = output_df.reset_index(drop=True)
    output_df["SET"] = "S01"
    output_df["LN"] = "L01"
    return output_df


def garposinput_to_datafile(garpos_input:GarposInput,path:Path):
    """
    Write a GarposInput to a datafile
    """
    path.parent.mkdir(exist_ok=True)
    if garpos_input.shot_data_file is None:
        garpos_input.shot_data_file = path.parent / f"{garpos_input.site.name}_shot_data.csv"
        
    if garpos_input.sound_speed_file is None:
        garpos_input.sound_speed_file = path.parent / f"{garpos_input.site.name}_sound_speed.csv"

    #Write the shot data and sound speed data to csv
    garpos_input.observation.shot_data.MT = garpos_input.observation.shot_data.MT.apply(
            lambda x: "M" + str(x) if x[0].isdigit() else x
        )
    garpos_input.observation.shot_data.to_csv(garpos_input.shot_data_file, index=False)
    garpos_input.observation.sound_speed_data.to_csv(garpos_input.sound_speed_file, index=False)

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

    df = pd.read_csv(shot_data_file, skiprows=1, index_col=0)
    shot_data_results = ObservationData.validate(df, lazy=True)
    sound_speed_results = SoundVelocityDataFrame(pd.read_csv(sound_speed_file))

    # Populate GarposObservation
    observation = GarposObservation(
        campaign=observation_section["campaign"],
        date_utc=datetime.strptime(observation_section["date(utc)"], "%Y-%m-%d"),
        date_mjd=observation_section["date(jday)"],
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
        inversiontype=InversionType(int(inv_parameters["inversiontype"])),
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
    _, transponder_avg_llh = avg_transponder_position(site_config.transponders)
    #
    transponder_center_enu: List[float] = coord_transformer.LLH2ENU(
        transponder_avg_llh.latitude,
        transponder_avg_llh.longitude,
        transponder_avg_llh.height,
    )
    transponder_avg_enu = PositionENU(east=transponder_center_enu[0], north=transponder_center_enu[1], up=transponder_center_enu[2])
    delta_center_position = PositionENU()
    delta_center_position.east_sigma = 1.0
    delta_center_position.north_sigma = 1.0
    delta_center_position.up_sigma = 0.0

    garpos_site = GarposSite(
        name=site_config.name,
        center_llh=site_center_llh,
        center_enu=transponder_avg_enu,
        atd_offset=atd_offset,
        transponders=site_config.transponders,
        delta_center_position=delta_center_position,
    )

    return garpos_site

def garpos_input_from_site_obs(
        site_config:SiteConfig,
        atd_offset:ATDOffset,
        gnss_data:DataFrame[PositionDataFrame],
        imu_data:DataFrame[IMUDataFrame],
        acoustic_data:DataFrame[AcousticDataFrame],
        sound_velocity:DataFrame[SoundVelocityDataFrame]
    ) -> GarposInput:

    gnss_data.longitude = gnss_data.longitude.apply(lambda x: x if x < 180 else x - 360)
    gnss_data.time = pd.to_datetime(gnss_data.time,format='mixed')
    imu_data.Time = pd.to_datetime(imu_data.Time, format="mixed")
    acoustic_data.TriggerTime = pd.to_datetime(
        acoustic_data.TriggerTime, format="mixed"
    )

    gnss_data = PositionDataFrame.validate(gnss_data,lazy=True)
    imu_data = IMUDataFrame.validate(imu_data,lazy=True)
    acoustic_data = AcousticDataFrame.validate(acoustic_data,lazy=True)
    sound_velocity = SoundVelocityDataFrame.validate(sound_velocity,    lazy=True)

    garpos_site: GarposSite = sitedata_to_garpossite(site_config,atd_offset)
    coord_transformer = CoordTransformer(site_config.position_llh)
    for row in gnss_data.itertuples(index=True):
        x, y, z = coord_transformer.LLH2ENU(
            getattr(row, "latitude"), getattr(row, "longitude"), getattr(row, "height")
        )
        gnss_data.at[row.Index, "x"] = x
        gnss_data.at[row.Index, "y"] = y
        gnss_data.at[row.Index, "z"] = z
    date_utc = gnss_data.time.min()
    date_mjd = julian.to_jd(date_utc, fmt="mjd")
    shot_data: DataFrame[ObservationData] = merge_to_shotdata(
        acoustic=acoustic_data, imu=imu_data, gnss=gnss_data
    )
    print(f"Shot data: {shot_data.shape[0]} Merged From {acoustic_data.shape[0]} Acoustic, {imu_data.shape[0]} IMU and {gnss_data.shape[0]} GNSS")
    garpos_observation = GarposObservation(
        campaign=site_config.name,
        date_utc=date_utc,
        date_mjd=date_mjd,
        ref_frame="ITRF",
        shot_data=shot_data,
        sound_speed_data=sound_velocity,
    )
    garpos_input = GarposInput(
        observation=garpos_observation, site=garpos_site)
    return garpos_input

def process_garpos_results(results:GarposInput) -> GarposResults:
    # Process garpos results to get delta x,y,z and relevant fields

    # Get the harmonic mean of the svp data, and use that to convert ResiTT to meters
    speed_mean = harmonic_mean(results.observation.sound_speed_data.speed.values)
    range_residuals = results.observation.shot_data.ResiTT.values * speed_mean

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

def main(
    input: GarposInput, fixed: GarposFixed
) -> Tuple:
    # Convert the observation and site data to the desired format
    #garpos_path = os.environ["GARPOS_DIR"]

    tmp_dir = Path("/tmp/garpos/")
    tmp_dir.mkdir(exist_ok=True)


    results_dir = tmp_dir / "results"
    results_dir.mkdir(exist_ok=True)

    input_path = tmp_dir/"observation.ini" 
    fixed_path = tmp_dir/"settings.ini"

    
    garposinput_to_datafile(input, input_path)
    garposfixed_to_datafile(fixed, fixed_path)


    rf = drive_garpos(str(input_path), str(fixed_path), str(results_dir), "test", 13)

    results = datafile_to_garposinput(rf)

    print(rf)
    proc_results = process_garpos_results(results)

    return proc_results
