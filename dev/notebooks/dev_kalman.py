import os
from pathlib import Path
import pandas as pd

from es_sfgtools.data_mgmt.data_handler import DataHandler
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)
import matplotlib.pyplot as plt

from nptyping import NDArray
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import NavigationToolbar2Tk

import sys
sys.path.append("/Users/franklyndunbar/Project/SeaFloorGeodesy/gnatss/src")
import gnatss
from gnatss.ops.kalman import run_filter_simulation
import gnatss.constants as constants
import datetime
import pymap3d


GPS_EPOCH = datetime.datetime(1980, 1, 6, 0, 0, 0)
J200_EPOCH = datetime.datetime(2000, 1, 1, 12, 0, 0)

COLUMN_ORDER = ['time', 'east', 'north', 'up','ant_x', 'ant_y', 'ant_z','ant_sigx', 'ant_sigy', 'ant_sigz','rho_xy', 'rho_xz', 'rho_yz','east_sig', 'north_sig', 'up_sig','v_sden', 'v_sdeu', 'v_sdnu']

def time_to_gpsweek_seconds(time: datetime.datetime) -> tuple[int, float]:
    """
    Convert a datetime object to GPS week and seconds of week.
    """

    # Calculate the difference in time
    delta = time - GPS_EPOCH

    # Total seconds since GPS epoch
    total_seconds = delta.total_seconds()

    # Calculate GPS week and seconds of week
    gps_week = int(total_seconds // (7 * 24 * 3600))
    seconds_of_week = total_seconds % (7 * 24 * 3600)

    return gps_week, seconds_of_week


def time_to_j200(time: datetime.datetime) -> float:
    """
    Convert a datetime object to Julian Date (JD) and then to J2000.
    """
    # Calculate the difference in time
    delta = time - J200_EPOCH

    # Total seconds since J2000 epoch
    total_seconds = delta.total_seconds()

    return total_seconds

def shotData_to_positiondf(shotdata:pd.DataFrame) -> pd.DataFrame:
    position_df_0 = shotdata[["pingTime", "east0", "north0", "up0","head0","pitch0","roll0","east_std0","north_std0","up_std0"]]
    position_df_1 = shotdata[["returnTime", "east1", "north1", "up1","head1","pitch1","roll1","east_std1","north_std1","up_std1"]]

    east_velocity_0 = (position_df_0["east0"].diff() / position_df_0["pingTime"].diff()).abs()
    north_velocity_0 = (position_df_0["north0"].diff() / position_df_0["pingTime"].diff()).abs()
    up_velocity_0 = (position_df_0["up0"].diff() / position_df_0["pingTime"].diff()).abs()
    east_velocity_1 = (position_df_1["east1"].diff() / position_df_1["returnTime"].diff()).abs()
    north_velocity_1 = (position_df_1["north1"].diff() / position_df_1["returnTime"].diff()).abs()
    up_velocity_1 = (position_df_1["up1"].diff() / position_df_1["returnTime"].diff()).abs()


    position_df_0 = position_df_0.rename(columns={"east0": "ant_x", "north0": "ant_y", "up0": "ant_z"})
    position_df_1 = position_df_1.rename(columns={"east1": "ant_x", "north1": "ant_y", "up1": "ant_z"})
    position_df_0 = position_df_0.rename(columns={"east_std0": "ant_sigx", "north_std0": "ant_sigy", "up_std0": "ant_sigz"})
    position_df_1 = position_df_1.rename(columns={"east_std1": "ant_sigx", "north_std1": "ant_sigy", "up_std1": "ant_sigz"})
    position_df_0 = position_df_0.rename(columns={"head0": "heading", "pitch0": "pitch", "roll0": "roll"})
    position_df_1 = position_df_1.rename(columns={"head1": "heading", "pitch1": "pitch", "roll1": "roll"})

    position_df_0["east"] = east_velocity_0
    position_df_0["north"] = north_velocity_0
    position_df_0["up"] = up_velocity_0

    position_df_0["east_sig"] = position_df_0["ant_sigx"]
    position_df_0["north_sig"] = position_df_0["ant_sigy"]
    position_df_0["up_sig"] = position_df_0["ant_sigz"]

    position_df_1["east"] = east_velocity_1
    position_df_1["north"] = north_velocity_1
    position_df_1["up"] = up_velocity_1

    position_df_1["east_sig"] = position_df_1["ant_sigx"]
    position_df_1["north_sig"] = position_df_1["ant_sigy"]
    position_df_1["up_sig"] = position_df_1["ant_sigz"]

    position_df_0["rho_xy"] = position_df_0["rho_xz"] = position_df_0["rho_yz"] = 1
    position_df_1["rho_xy"] = position_df_1["rho_xz"] = position_df_1["rho_yz"] = 1
    position_df_0["v_sden"] =  position_df_0["v_sdeu"] = position_df_0["v_sdnu"] = 1
    position_df_1["v_sden"] =  position_df_1["v_sdeu"] = position_df_1["v_sdnu"] = 1

    position_df_0 = position_df_0.rename(columns={"pingTime": "time"})
    position_df_1 = position_df_1.rename(columns={"returnTime": "time"})
    position_df = pd.concat([position_df_0, position_df_1])

    return position_df

def gpsData_to_positiondf(gpsdata:pd.DataFrame) -> pd.DataFrame:
    gps_df = gpsdata.copy()
    gps_df.time = gps_df.time.apply(lambda x: x.timestamp())
    east_velocity = (gps_df.east.diff() / gps_df.time.diff()).abs()
    north_velocity = (gps_df.north.diff() / gps_df.time.diff()).abs()
    up_velocity = (gps_df.up.diff() / gps_df.time.diff()).abs()

    gps_df["ant_x"] = gps_df["east"]
    gps_df["ant_y"] = gps_df["north"]
    gps_df["ant_z"] = gps_df["up"]

    gps_df["east"] = east_velocity
    gps_df["north"] = north_velocity
    gps_df["up"] = up_velocity

    gps_df["ant_sigx"] = .5
    gps_df["ant_sigy"] = .5
    gps_df["ant_sigz"] = .5

    gps_df["rho_xy"] = 0
    gps_df["rho_xz"] = 0
    gps_df["rho_yz"] = 0

    gps_df["east_sig"] = 1
    gps_df["north_sig"] = 1
    gps_df["up_sig"] = 1

    gps_df["v_sden"] = 1
    gps_df["v_sdeu"] = 1
    gps_df["v_sdnu"] = 1
    gps_df["source"] = 'KINEMATIC'

    return gps_df

def runFilter(df_all:pd.DataFrame) -> pd.DataFrame:

    df_numpy = df_all[COLUMN_ORDER].to_numpy()
    x, P, _, _ = run_filter_simulation(
        df_numpy,
        constants.start_dt,
        constants.gnss_pos_psd,
        constants.vel_psd,
        constants.cov_err,
    )

    # Positions covariance
    ant_cov = P[:, :3, :3]
    ant_cov_df = pd.DataFrame(ant_cov.reshape(ant_cov.shape[0], -1), columns=constants.ANT_GPS_COV)
    ant_cov_df[[*constants.ANT_GPS_GEOCENTRIC_STD]] = ant_cov_df[
        [*constants.ANT_GPS_COV_DIAG]
    ].apply(np.sqrt)


    # Smoothed positions
    smoothed_results = pd.DataFrame(
        x.reshape(x.shape[0], -1)[:, :3],
        columns=constants.ANT_GPS_GEOCENTRIC,
    )
    # Add a unique index for merging
    smoothed_results["merge_idx"] = smoothed_results.index
    ant_cov_df["merge_idx"] = ant_cov_df.index

    smoothed_results[constants.GPS_TIME] = df_all[constants.GPS_TIME].reset_index(drop=True)
    ant_cov_df[constants.GPS_TIME] = df_all[constants.GPS_TIME].reset_index(drop=True)

    smoothed_results = smoothed_results.merge(ant_cov_df, on="merge_idx", how="left", suffixes=('', '_cov'))
    return smoothed_results


def kalman_update(
    positions_data: pd.DataFrame, kin_positions: pd.DataFrame
) -> pd.DataFrame:
    positions_data_copy = positions_data.copy()
    e, n, u = pymap3d.geodetic2ecef(
        lat=positions_data_copy.latitude,
        lon=positions_data_copy.longitude,
        alt=positions_data_copy.height,
    )
    (
        positions_data_copy["ant_x"],
        positions_data_copy["ant_y"],
        positions_data_copy["ant_z"],
    ) = (e, n, u)
    positions_data_copy["east"] = positions_data_copy.eastVelocity
    positions_data_copy["north"] = positions_data_copy.northVelocity
    positions_data_copy["up"] = positions_data_copy.upVelocity

    positions_data_copy["ant_sigx"] = positions_data_copy["latitude_std"]
    positions_data_copy["ant_sigy"] = positions_data_copy["longitude_std"]
    positions_data_copy["ant_sigz"] = positions_data_copy["height_std"]
    positions_data_copy["rho_xy"] = 0
    positions_data_copy["rho_xz"] = 0
    positions_data_copy["rho_yz"] = 0

    positions_data_copy["east_sig"] = positions_data_copy["eastVelocity_std"]
    positions_data_copy["north_sig"] = positions_data_copy["northVelocity_std"]
    positions_data_copy["up_sig"] = positions_data_copy["upVelocity_std"]

    positions_data_copy["v_sden"] = 1
    positions_data_copy["v_sdeu"] = 1
    positions_data_copy["v_sdnu"] = 1

    gps_df = kin_positions.copy()
    east_velocity = gps_df.east.diff() / gps_df.time.diff()
    north_velocity = gps_df.north.diff() / gps_df.time.diff()
    up_velocity = gps_df.up.diff() / gps_df.time.diff()

    gps_df["ant_x"] = gps_df["east"]
    gps_df["ant_y"] = gps_df["north"]
    gps_df["ant_z"] = gps_df["up"]

    gps_df["east"] = east_velocity
    gps_df["north"] = north_velocity
    gps_df["up"] = up_velocity

    gps_df["ant_sigx"] = 0.1
    gps_df["ant_sigy"] = 0.1
    gps_df["ant_sigz"] = 0.1

    gps_df["rho_xy"] = 0
    gps_df["rho_xz"] = 0
    gps_df["rho_yz"] = 0

    gps_df["east_sig"] = 2
    gps_df["north_sig"] = 2
    gps_df["up_sig"] = 2

    gps_df["v_sden"] = 2
    gps_df["v_sdeu"] = 2
    gps_df["v_sdnu"] = 2

    df_all = pd.concat([positions_data_copy, gps_df])
    column_order = [
        "time",
        "east",
        "north",
        "up",
        "ant_x",
        "ant_y",
        "ant_z",
        "ant_sigx",
        "ant_sigy",
        "ant_sigz",
        "rho_xy",
        "rho_xz",
        "rho_yz",
        "east_sig",
        "north_sig",
        "up_sig",
        "v_sden",
        "v_sdeu",
        "v_sdnu",
    ]
    df_all = df_all[column_order]
    df_all = df_all.sort_values(by="time")
    df_all = df_all.dropna()

    smoothed_results = runFilter(df_all)

    merged_positions = pd.merge_asof(
        positions_data_copy.sort_values("time"),
        smoothed_results.sort_values("time"),
        on="time",
        direction="nearest",
        suffixes=("", "_smoothed"),
    )
    merged_positions["east_offset"] = (
        merged_positions["ant_x_smoothed"] - merged_positions["ant_x"]
    ).abs()
    merged_positions["north_offset"] = (
        merged_positions["ant_y_smoothed"] - merged_positions["ant_y"]
    ).abs()
    merged_positions["up_offset"] = (
        merged_positions["ant_z_smoothed"] - merged_positions["ant_z"]
    ).abs()
    merged_positions = merged_positions[
        ["time", "east_offset", "north_offset", "up_offset"]
    ]

    return smoothed_results, merged_positions


main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")
dh = DataHandler(main_dir)

network = "cascadia-gorda"
station = "NCC1"
survey = "2024_A_1126"

dh.change_working_station(network=network, station=station, campaign=survey)
print(dates:=dh.kin_position_tdb.get_unique_dates())
kin_positions = dh.kin_position_tdb.read_df(dates[8], dates[9])
shotdata = dh.shotdata_tdb_pre.read_df(dates[8], dates[9])
positions_data = dh.imu_position_tdb.read_df(dates[8], dates[9])
positions_data.time = positions_data.time.apply(lambda x: x.timestamp())
kin_positions.time = kin_positions.time.apply(lambda x: x.timestamp())

smoothed_results, merged_positions = kalman_update(positions_data, kin_positions)

print(f"East Offset (m): {merged_positions['east_offset'].describe()}")
print(f"North Offset (m): {merged_positions['north_offset'].describe()}")
print(f"Up Offset (m): {merged_positions['up_offset'].describe()}")