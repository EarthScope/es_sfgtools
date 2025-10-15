from typing import List

import gnatss.constants as constants
import numpy as np
import pandas as pd
import pymap3d
from gnatss.ops.kalman import run_filter_simulation
from numpy import datetime64
from scipy.stats import zscore
from sklearn.neighbors import RadiusNeighborsRegressor

from ..logging import ProcessLogger as logger

# Local imports
from ..tiledb_tools.tiledb_schemas import (
    TDBIMUPositionArray,
    TDBKinPositionArray,
    TDBShotDataArray,
)

MEDIAN_EAST_POSITION = 0
MEDIAN_NORTH_POSITION = 0
MEDIAN_UP_POSITION = 0

def prepare_positions_data(positions_data:pd.DataFrame) -> pd.DataFrame:
    """
    Prepares IMU positions data for Kalman filtering by converting geodetic coordinates to ECEF,
    computing median positions, and adding velocity and uncertainty columns.
    Parameters
    ----------
    positions_data : pandas.DataFrame
        DataFrame containing IMU position and velocity data with columns:
        'latitude', 'longitude', 'height', 'eastVelocity', 'northVelocity', 'upVelocity',
        and their respective standard deviations.
    Returns
    -------
    positions_data_copy : pandas.DataFrame
        A copy of the input DataFrame with additional columns:
        - 'ant_x', 'ant_y', 'ant_z': ECEF coordinates
        - 'east', 'north', 'up': velocity components
        - 'ant_sigx', 'ant_sigy', 'ant_sigz': uncertainties in position
        - 'rho_xy', 'rho_xz', 'rho_yz': correlation coefficients (set to 0)
        - 'east_sig', 'north_sig', 'up_sig': uncertainties in velocity
        - 'v_sden', 'v_sdeu', 'v_sdnu': additional velocity uncertainty columns (set to 0)
    Notes
    -----
    Also sets global variables MEDIAN_EAST_POSITION, MEDIAN_NORTH_POSITION, and MEDIAN_UP_POSITION
    to the median ECEF coordinates.
    """

    positions_data_copy = positions_data.copy()
    positions_data_copy.time = positions_data_copy.time.apply(lambda x: x.timestamp())

    global MEDIAN_EAST_POSITION, MEDIAN_NORTH_POSITION, MEDIAN_UP_POSITION
    e, n, u = pymap3d.geodetic2ecef(lat=positions_data_copy.latitude, lon=positions_data_copy.longitude, alt=positions_data_copy.height)

    MEDIAN_EAST_POSITION = np.median(e)
    MEDIAN_NORTH_POSITION = np.median(n)
    MEDIAN_UP_POSITION = np.median(u)

    positions_data_copy["ant_x"], positions_data_copy["ant_y"], positions_data_copy["ant_z"] = e, n, u
    positions_data_copy["east"] = positions_data_copy.eastVelocity
    positions_data_copy["north"] = positions_data_copy.northVelocity
    positions_data_copy["up"] = positions_data_copy.upVelocity


    positions_data_copy["ant_sigx"] = positions_data_copy["latitude_std"].bfill().ffill()
    positions_data_copy["ant_sigy"] = positions_data_copy["longitude_std"].bfill().ffill()
    positions_data_copy["ant_sigz"] = positions_data_copy["height_std"].bfill().ffill()
    positions_data_copy["rho_xy"] = 0
    positions_data_copy["rho_xz"] = 0
    positions_data_copy["rho_yz"] = 0
    positions_data_copy["east_sig"] = positions_data_copy["eastVelocity_std"].bfill().ffill()
    positions_data_copy["north_sig"] = positions_data_copy["northVelocity_std"].bfill().ffill()
    positions_data_copy["up_sig"] = positions_data_copy["upVelocity_std"].bfill().ffill()

    positions_data_copy["v_sden"] = 0
    positions_data_copy["v_sdeu"] = 0
    positions_data_copy["v_sdnu"] = 0

    return positions_data_copy

def prepare_kinematic_data(kin_positions: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares kinematic GPS data for Kalman filtering by computing velocities and filtering outliers.

    This function takes a DataFrame containing kinematic GPS positions and processes it as follows:
    - Copies the input DataFrame to avoid modifying the original.
    - Renames position columns ('east', 'north', 'up') to antenna coordinates ('ant_x', 'ant_y', 'ant_z').
    - Initializes velocity columns ('east', 'north', 'up') with NaN values.
    - Adds uncertainty and correlation columns with default values.
    - Calculates velocity components by differentiating position over time.
    - Filters out rows with velocity spikes using a z-score threshold.
    - Prints the reduction in data size after filtering.

    Parameters
    ----------
    kin_positions : pd.DataFrame
        DataFrame containing kinematic GPS positions with columns 'east', 'north', 'up', and 'time'.

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with velocity columns and outlier rows removed.
    """

    gps_df = kin_positions.copy()
    gps_df.time = gps_df.time.apply(lambda x: x.timestamp())

    gps_df["ant_x"] = gps_df["east"]
    gps_df["ant_y"] = gps_df["north"]
    gps_df["ant_z"] = gps_df["up"]

    # Placeholder for velocity columns
    gps_df["east"] = np.nan
    gps_df["north"] = np.nan
    gps_df["up"] = np.nan

    gps_df["ant_sigx"] = .1
    gps_df["ant_sigy"] = .1
    gps_df["ant_sigz"] = .1
    gps_df["rho_xy"] = 0
    gps_df["rho_xz"] = 0
    gps_df["rho_yz"] = 0
    gps_df["east_sig"] = .1
    gps_df["north_sig"] = .1
    gps_df["up_sig"] = .1
    gps_df["v_sden"] = 0
    gps_df["v_sdeu"] = 0
    gps_df["v_sdnu"] = 0

    time_diff = gps_df.time.diff().bfill()

    ant_x_diff = gps_df.ant_x.diff()
    ant_y_diff = gps_df.ant_y.diff()
    ant_z_diff = gps_df.ant_z.diff()


    east_velocity = (ant_x_diff / time_diff).bfill()
    north_velocity = (ant_y_diff / time_diff).bfill()
    up_velocity = (ant_z_diff / time_diff).bfill()

    gps_df.east = east_velocity
    gps_df.north = north_velocity
    gps_df.up = up_velocity

    original_len = len(gps_df)

    # filter by z-score to remove spikes
    z_thresh = 4
    east_z = zscore(east_velocity.abs())
    north_z = zscore(north_velocity.abs())
    up_z = zscore(up_velocity.abs())

    # create logical and filter
    east_z_filter = east_z < z_thresh
    north_z_filter = north_z < z_thresh
    up_z_filter = up_z < z_thresh

    gps_df = gps_df[east_z_filter & north_z_filter & up_z_filter]

    filtered_len = len(gps_df)
    logger.loginfo(f"Kinematic data filtered from {original_len} to {filtered_len} rows for a {((original_len - filtered_len) / original_len * 100):.2f} % reduction using z-score threshold of {z_thresh}.")
    return gps_df


def combine_data(imu_position_data: pd.DataFrame, ppp_position_data: pd.DataFrame) -> pd.DataFrame:
    """
    Combines IMU position and PPP position data into a single DataFrame with a specified column order.
    Parameters
    ----------
    imu_position_data : pd.DataFrame
        DataFrame containing IMU position data with columns matching the expected column order.
    ppp_position_data : pd.DataFrame
        DataFrame containing PPP position data with columns matching the expected column order.
    Returns
    -------
    pd.DataFrame
        Combined DataFrame containing both position and GPS data, ordered by time and columns.
        Note: Rows with NaN values are retained to preserve kinematic velocity information.
    """

    column_order = [
        'time', 'east', 'north', 'up', 'ant_x', 'ant_y', 'ant_z',
        'ant_sigx', 'ant_sigy', 'ant_sigz', 'rho_xy', 'rho_xz', 'rho_yz',
        'east_sig', 'north_sig', 'up_sig', 'v_sden', 'v_sdeu', 'v_sdnu'
    ]
    df_all = pd.concat([imu_position_data, ppp_position_data])
    df_all = df_all[column_order]
    df_all = df_all.sort_values(by="time")
    # Don't dropna here, as kinematic velocities are NaN
    
    logger.loginfo(f"Combined data shape: {df_all.shape}")
    return df_all

def run_kalman_filter_and_smooth(df_all: pd.DataFrame, start_dt: float, gnss_pos_psd: float, vel_psd: float, cov_err: float) -> pd.DataFrame:
    """
    Runs a Kalman filter simulation on GNSS shot data and processes the results.
    Parameters
    ----------
    df_all : pd.DataFrame
        Input DataFrame containing GNSS shot data. Rows with NaN values are dropped before processing.
    start_dt : float
        Initial time delta for the Kalman filter simulation.
    gnss_pos_psd : float
        Position process spectral density for GNSS measurements.
    vel_psd : float
        Velocity process spectral density for the filter.
    cov_err : float
        Initial covariance error for the filter.
    Returns
    -------
    pd.DataFrame
        DataFrame containing smoothed GNSS positions and associated covariance statistics.
        If the input DataFrame is empty after dropping NaNs, returns an empty DataFrame.
    """

    # Drop rows with NaN values which are from the first row of kinematic velocity calculation
    df_all = df_all.dropna()
    if df_all.empty:
        return pd.DataFrame()

    x, P, _, _ = run_filter_simulation(
        df_all.to_numpy(),
        start_dt, gnss_pos_psd, vel_psd, cov_err
    )
    logger.loginfo(f"Filter Parameters - Start DT: {start_dt}, GNSS_POS_PSD: {gnss_pos_psd}, VEL_PSD: {vel_psd}, COV_ERR: {cov_err}")

    # Process positions covariance
    ant_cov = P[:, :3, :3]
    ant_cov_df = pd.DataFrame(ant_cov.reshape(ant_cov.shape[0], -1), columns=constants.ANT_GPS_COV)
    ant_cov_df[[*constants.ANT_GPS_GEOCENTRIC_STD]] = ant_cov_df[[*constants.ANT_GPS_COV_DIAG]].apply(np.sqrt)

    # Process smoothed positions
    smoothed_results = pd.DataFrame(
        x.reshape(x.shape[0], -1)[:, :3],
        columns=constants.ANT_GPS_GEOCENTRIC,
    )
    
    # Merge results with covariance data
    smoothed_results["merge_idx"] = smoothed_results.index
    ant_cov_df["merge_idx"] = ant_cov_df.index
    
    time_reset = df_all[constants.GPS_TIME].reset_index(drop=True)
    smoothed_results[constants.GPS_TIME] = time_reset
    ant_cov_df[constants.GPS_TIME] = time_reset

    smoothed_results = smoothed_results.merge(ant_cov_df, on="merge_idx", how="left", suffixes=('', '_cov'))
    
    return smoothed_results

def analyze_offsets(merged_positions: pd.DataFrame):
    """
    Analyzes the offsets between smoothed and original antenna positions in the provided DataFrame.
    Calculates the absolute differences for the X, Y, and Z coordinates between the columns
    'ant_x_smoothed', 'ant_y_smoothed', 'ant_z_smoothed' and their respective original columns
    'ant_x', 'ant_y', 'ant_z'. Computes summary statistics (count, mean, std, min, 25%, 50%, 75%, max)
    for each offset and prints the results in a formatted table.
    Parameters
    ----------
    merged_positions : pd.DataFrame
        DataFrame containing the columns 'ant_x', 'ant_y', 'ant_z', 'ant_x_smoothed',
        'ant_y_smoothed', and 'ant_z_smoothed'.
    Returns
    -------
    None
        Prints the summary statistics to the console. If the DataFrame is empty, prints a message and returns.
    """
  
    if merged_positions.empty:
        logger.loginfo("No merged positions to analyze.")
        return

    offset_x = (merged_positions["ant_x_smoothed"] - merged_positions["ant_x"]).abs()
    offset_y = (merged_positions["ant_y_smoothed"] - merged_positions["ant_y"]).abs()
    offset_z = (merged_positions["ant_z_smoothed"] - merged_positions["ant_z"]).abs()

    summary_df = pd.DataFrame({
        'Offset X (m)': offset_x.describe(),
        'Offset Y (m)': offset_y.describe(),
        'Offset Z (m)': offset_z.describe()
    })
    
    logger.loginfo(summary_df.round(6).to_string())

def update_shotdata_with_smoothed_positions(shotdata: pd.DataFrame, smoothed_results: pd.DataFrame):
    """
    Interpolates smoothed positions onto shotdata ping and return times.
    """
    if smoothed_results.empty:
        logger.loginfo("No smoothed results to interpolate from.")
        return shotdata

    X_train = smoothed_results.time.to_numpy().reshape(-1, 1)
    Y_train = smoothed_results[[ "ant_x", "ant_y", "ant_z"]].to_numpy()

    position_interpolator = RadiusNeighborsRegressor(radius=0.2, weights='distance')
    position_interpolator.fit(X_train, Y_train)

    train_score = position_interpolator.score(X_train, Y_train)
    logger.loginfo(f"Position Interpolator Train Score: {train_score:.4f}")

    ping_times = shotdata.pingTime.to_numpy().reshape(-1, 1)
    return_times = shotdata.returnTime.to_numpy().reshape(-1, 1)

    predicted_ping_pos = position_interpolator.predict(ping_times)
    predicted_return_pos = position_interpolator.predict(return_times)

    shotdata.loc[:, ["east0", "north0", "up0"]][~np.isnan(predicted_ping_pos[:,0])] = (
        predicted_ping_pos[~np.isnan(predicted_ping_pos[:, 0]), :]
    )
    shotdata.loc[:, ["isUpdated"]][~np.isnan(predicted_ping_pos[:, 0])] = True

    shotdata.loc[:, ["east1", "north1", "up1"]][~np.isnan(predicted_return_pos[:, 0])] = (
        predicted_return_pos[~np.isnan(predicted_return_pos[:, 0]), :]
    )
    shotdata.loc[:, ["isUpdated"]][~np.isnan(predicted_return_pos[:, 0])] = True

    nan_pings = np.isnan(predicted_ping_pos).any(axis=1).sum()
    nan_returns = np.isnan(predicted_return_pos).any(axis=1).sum()
    if nan_pings > 0:
        logger.loginfo(f"Warning: {nan_pings} ping times could not be interpolated (no smoothed data within radius).")
    if nan_returns > 0:
        logger.loginfo(f"Warning: {nan_returns} return times could not be interpolated (no smoothed data within radius).")

    return shotdata

def filter_spatial_outliers(df: pd.DataFrame, radius: float = 5000) -> pd.DataFrame:
    """
    Filters out rows in the DataFrame that are outside a specified radius from the median ECEF position.
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing ECEF position columns 'ant_x', 'ant_y', 'ant_z'.
    radius : float
        Radius in meters to define the acceptable range from the median position.
    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with rows outside the specified radius removed.
    """
    original_len = len(df)
    position_filters = (
        (df.ant_x.between(MEDIAN_EAST_POSITION - radius, MEDIAN_EAST_POSITION + radius)) &
        (df.ant_y.between(MEDIAN_NORTH_POSITION - radius, MEDIAN_NORTH_POSITION + radius)) &
        (df.ant_z.between(MEDIAN_UP_POSITION - radius, MEDIAN_UP_POSITION + radius))
    )
    df_filtered = df[position_filters]
    filtered_len = len(df_filtered)
    logger.loginfo(f"Data filtered from {original_len} to {filtered_len} rows for a {((original_len - filtered_len) / original_len * 100):.2f} % reduction using {radius}m position threshold.")
    return df_filtered


def main(
    shotdata: pd.DataFrame,
    kin_positions: pd.DataFrame,
    positions_data: pd.DataFrame,
    gnss_pos_psd=constants.gnss_pos_psd,
    vel_psd=constants.vel_psd,
    cov_err=constants.cov_err,
    start_dt=constants.start_dt,
    filter_radius=5000,
 ) -> pd.DataFrame:
    """
        Refines shotdata using GNSS and IMU data through Kalman filtering and smoothing.

        Parameters
        ----------
        shotdata : pd.DataFrame
            DataFrame containing shot event data to be refined.
        kin_positions : pd.DataFrame
            DataFrame containing kinematic GNSS positions.
        positions_data : pd.DataFrame
            DataFrame containing original positions data.
        gnss_pos_psd : float or array-like, optional
            GNSS position process noise spectral density (default: constants.GNSS_POS_PSD).
        vel_psd : float or array-like, optional
            Velocity process noise spectral density (default: constants.VEL_PSD).
        cov_err : float or array-like, optional
            Initial covariance error (default: constants.COV_ERR).
        start_dt : float or pd.Timestamp, optional
            Start datetime for filtering (default: constants.START_DT).
        filter_radius : float, optional
            Radius for spatial outlier filtering in meters (default: 5000).

        Returns
        -------
        pd.DataFrame
            Updated shotdata DataFrame with refined positions and antenna offsets.

        Notes
        -----
        - Combines positions and kinematic GNSS data, filters spatial outliers, and applies Kalman filter smoothing.
        - Merges smoothed results with original and kinematic positions for offset analysis.
        - Updates shotdata with refined positions and prints summary statistics of antenna offsets.
    """

    if positions_data.empty:
        logger.loginfo("No positions data provided.")
        return shotdata

    positions_data_copy = prepare_positions_data(positions_data)

    if kin_positions.empty:
        logger.loginfo("No kinematic positions data provided.")
        gps_data = pd.DataFrame(columns=positions_data_copy.columns)
    else:
        gps_data = prepare_kinematic_data(kin_positions)

    if filter_radius > 0:
        positions_data_copy = filter_spatial_outliers(positions_data_copy, radius=filter_radius)
        gps_data = filter_spatial_outliers(gps_data, radius=filter_radius)

    df_all = combine_data(positions_data_copy, gps_data)

    smoothed_results = run_kalman_filter_and_smooth(df_all, start_dt, gnss_pos_psd, vel_psd, cov_err)

    if smoothed_results.empty:
        logger.loginfo("Kalman filter returned no results.")
        return shotdata

    merged_positions = pd.merge_asof(
        positions_data_copy.sort_values("time"),
        smoothed_results.sort_values("time"),
        on="time",
        tolerance=pd.Timedelta("10ms").total_seconds(),
        direction="nearest",
        suffixes=("", "_smoothed"),
    )
    logger.loginfo("\n--- Offset Analysis ---")
    logger.loginfo("----Results vs Original Positions----")
    analyze_offsets(merged_positions)

    shotdata_updated = update_shotdata_with_smoothed_positions(shotdata, smoothed_results)


    return shotdata_updated


def merge_shotdata_kinposition(
    shotdata_pre: TDBShotDataArray,
    shotdata: TDBShotDataArray,
    kin_position: TDBKinPositionArray,
    position_data:TDBIMUPositionArray,
    dates: List[datetime64],
    filter_radius: float = 5000,
) -> TDBShotDataArray:
    """
    Merge the shotdata and kin_position data

    Args:
        shotdata_pre (TDBShotDataArray): the DFOP00 data
        shotdata (TDBShotDataArray): The shotdata array to write to
        kin_position (TDBKinPositionArray): The TileDB KinPosition array
        dates (List[datetime64]): The dates to merge
        filter_radius (float, optional): Radius for spatial outlier filtering in meters (default: 5000).
    """

    logger.loginfo("Merging shotdata and kin_position data")
    for date in dates:

        shotdata_df = shotdata_pre.read_df(start=date)
        kin_position_df = kin_position.read_df(start=date)
        position_df = position_data.read_df(start=date)
        if position_data is None or position_df is None or kin_position_df is None:
            continue
        if shotdata_df.empty or kin_position_df.empty or position_df.empty:
            continue

        logger.loginfo(f"Interpolating shotdata for date {str(date)}")

        # interpolate the enu values
        shotdata_df_updated = main(
            shotdata=shotdata_df,
            kin_positions=kin_position_df,
            positions_data=position_df,
            gnss_pos_psd=constants.gnss_pos_psd,
            vel_psd=constants.vel_psd,
            cov_err=constants.cov_err,
            start_dt=constants.start_dt,
            filter_radius=filter_radius,
        )

        shotdata.write_df(shotdata_df_updated, validate=False)
