# %%
import os
from pathlib import Path
import pandas as pd
import numpy as np
import pymap3d
import datetime
import sys
from scipy.stats import zscore
from sklearn.neighbors import RadiusNeighborsRegressor
import matplotlib.pyplot as plt

# Assuming 'gnatss' and 'es_sfgtools' are installed or in the path
from es_sfgtools.data_mgmt.data_handler import DataHandler
# It's better to handle path modifications carefully.
# If gnatss is a package, it should be installed.
# For development, this is okay.
sys.path.append("/Users/franklyndunbar/Project/SeaFloorGeodesy/gnatss/src")
import gnatss
from gnatss.ops.kalman import run_filter_simulation
import gnatss.constants as constants

# Matplotlib is commented out as requested
# import matplotlib.pyplot as plt
# %matplotlib ipympl

# %% 
# Constants
GPS_EPOCH = datetime.datetime(1980, 1, 6, 0, 0, 0)
J200_EPOCH = datetime.datetime(2000, 1, 1, 12, 0, 0)

# %% 
# Helper Functions

MEDIAN_EAST_POSITION = 0
MEDIAN_NORTH_POSITION = 0
MEDIAN_UP_POSITION = 0

def time_to_gpsweek_seconds(time: datetime.datetime) -> tuple[int, float]:
    """
    Convert a datetime object to GPS week and seconds of week.
    """
    delta = time - GPS_EPOCH
    total_seconds = delta.total_seconds()
    gps_week = int(total_seconds // (7 * 24 * 3600))
    seconds_of_week = total_seconds % (7 * 24 * 3600)
    return gps_week, seconds_of_week

def time_to_j200(time: datetime.datetime) -> float:
    """
    Convert a datetime object to seconds since J2000 epoch.
    """
    delta = time - J200_EPOCH
    return delta.total_seconds()

def load_data(dh: DataHandler, start_date, end_date):
    """
    Loads kinematic positions, shot data, and IMU positions.
    """
    kin_positions = dh.kin_position_tdb.read_df(start_date, end_date)
    shotdata = dh.shotdata_tdb_pre.read_df(start_date, end_date)
    positions_data = dh.imu_position_tdb.read_df(start_date, end_date)
    
    # Convert time to unix timestamp
    positions_data.time = positions_data.time.apply(lambda x: x.timestamp())
    kin_positions.time = kin_positions.time.apply(lambda x: x.timestamp())
    # shotdata pingTime and returnTime are already float timestamps
    
    return kin_positions, shotdata, positions_data

def split_data_into_chunks(df, columns = {}, index_column='time', max_gap_seconds=300):
    """
    Splits a DataFrame into chunks based on time gaps.
    """
    # first ensure the DataFrame is sorted by time
    df = df.sort_values(by=index_column).reset_index(drop=True)
    time_diffs = df[index_column].diff()
    split_indices = time_diffs[time_diffs > max_gap_seconds].index.tolist()
    
    # now check for jumps in specified columns
    for column,val in columns.items():
        if column in df.columns:
            col_diffs = df[column].diff().abs()
            col_split_indices = col_diffs[col_diffs > val].index.tolist()
            split_indices.extend(col_split_indices)
    
    split_indices = sorted(set(split_indices))
    chunks = []
    start_idx = 0
    for split_idx in split_indices:
        chunks.append(df.iloc[start_idx:split_idx])
        start_idx = split_idx
    chunks.append(df.iloc[start_idx:])
    
    print(f"Data split into {len(chunks)} chunks based on a {max_gap_seconds}s gap threshold.")
    return [chunk for chunk in chunks if not chunk.empty]

def prepare_positions_data(positions_data):
    """
    Prepares the IMU positions data for Kalman filtering.
    """
    global MEDIAN_EAST_POSITION, MEDIAN_NORTH_POSITION, MEDIAN_UP_POSITION
    e, n, u = pymap3d.geodetic2ecef(lat=positions_data.latitude, lon=positions_data.longitude, alt=positions_data.height)

    MEDIAN_EAST_POSITION = np.median(e)
    MEDIAN_NORTH_POSITION = np.median(n)
    MEDIAN_UP_POSITION = np.median(u)

    positions_data["ant_x"], positions_data["ant_y"], positions_data["ant_z"] = e, n, u
    positions_data["east"] = positions_data.eastVelocity
    positions_data["north"] = positions_data.northVelocity
    positions_data["up"] = positions_data.upVelocity
    
    positions_data_copy = positions_data.copy()
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
    
    return positions_data_copy

def prepare_kinematic_data(kin_positions,max_speed=2):
    """
    Prepares the kinematic GPS data for Kalman filtering, without velocity.
    """
    gps_df = kin_positions.copy()

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
    gps_df["v_sden"] = .1
    gps_df["v_sdeu"] = .1
    gps_df["v_sdnu"] = .1

    time_diff = gps_df.time.diff().fillna(method='bfill')
    time_diff.iloc[-1] = time_diff.iloc[-2]  # Handle last value

    ant_x_diff = gps_df.ant_x.diff()
    ant_y_diff = gps_df.ant_y.diff()
    ant_z_diff = gps_df.ant_z.diff()


    east_velocity = (ant_x_diff / time_diff).fillna(method='bfill')
    north_velocity = (ant_y_diff / time_diff).fillna(method='bfill')
    up_velocity = (ant_z_diff / time_diff).fillna(method='bfill')

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
    print(f"Kinematic data filtered from {original_len} to {filtered_len} rows for a {((original_len - filtered_len) / original_len * 100):.2f} % reduction using z-score threshold of {z_thresh}.")
    return gps_df

# def calculate_and_clamp_kinematic_velocity(chunk, max_speed=2):
#     """
#     Calculates and clamps kinematic velocity for a single data chunk.
#     """
    
#     time_diff = chunk.loc[kin_rows, 'time'].diff()
    
#     # Calculate velocities
#     chunk.loc[kin_rows, 'east'] = (chunk.loc[kin_rows, 'ant_x'].diff() / time_diff)
#     chunk.loc[kin_rows, 'north'] = (chunk.loc[kin_rows, 'ant_y'].diff() / time_diff)
#     chunk.loc[kin_rows, 'up'] = (chunk.loc[kin_rows, 'ant_z'].diff() / time_diff)

#     chunk.loc[kin_rows, 'east'].fillna(method='bfill', inplace=True)
#     chunk.loc[kin_rows, 'north'].fillna(method='bfill', inplace=True)
#     chunk.loc[kin_rows, 'up'].fillna(method='bfill', inplace=True)

#     # Clamp the velocities to a reasonable range
#     vel_cols = ['east', 'north', 'up']
#     chunk[vel_cols] = chunk[vel_cols].clip(-max_speed, max_speed)
    
#     return chunk

def combine_data(positions_data, gps_data):
    """
    Combines IMU and kinematic data.
    """
    column_order = [
        'time', 'east', 'north', 'up', 'ant_x', 'ant_y', 'ant_z',
        'ant_sigx', 'ant_sigy', 'ant_sigz', 'rho_xy', 'rho_xz', 'rho_yz',
        'east_sig', 'north_sig', 'up_sig', 'v_sden', 'v_sdeu', 'v_sdnu'
    ]
    df_all = pd.concat([positions_data, gps_data])
    df_all = df_all[column_order]
    df_all = df_all.sort_values(by="time")
    # Don't dropna here, as kinematic velocities are NaN
    
    print(f"Combined data shape: {df_all.shape}")
    return df_all

def run_kalman_filter_and_smooth(df_all, start_dt, gnss_pos_psd, vel_psd, cov_err):
    """
    Runs the Kalman filter simulation and processes the results.
    """
    # Drop rows with NaN values which are from the first row of kinematic velocity calculation
    df_all = df_all.dropna()
    if df_all.empty:
        return pd.DataFrame()

    x, P, _, _ = run_filter_simulation(
        df_all.to_numpy(),
        start_dt, gnss_pos_psd, vel_psd, cov_err
    )
    print(f"Filter Parameters - Start DT: {start_dt}, GNSS_POS_PSD: {gnss_pos_psd}, VEL_PSD: {vel_psd}, COV_ERR: {cov_err}")

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

def analyze_offsets(merged_positions):
    """
    Analyzes the offsets between smoothed and original positions and prints a summary.
    """
    if merged_positions.empty:
        print("No merged positions to analyze.")
        return

    offset_x = (merged_positions["ant_x_smoothed"] - merged_positions["ant_x"]).abs()
    offset_y = (merged_positions["ant_y_smoothed"] - merged_positions["ant_y"]).abs()
    offset_z = (merged_positions["ant_z_smoothed"] - merged_positions["ant_z"]).abs()

    summary_df = pd.DataFrame({
        'Offset X (m)': offset_x.describe(),
        'Offset Y (m)': offset_y.describe(),
        'Offset Z (m)': offset_z.describe()
    })
    
    print(summary_df.round(4).to_string())

def update_shotdata_with_smoothed_positions(shotdata, smoothed_results):
    """
    Interpolates smoothed positions onto shotdata ping and return times.
    """
    if smoothed_results.empty:
        print("No smoothed results to interpolate from.")
        return shotdata

    X_train = smoothed_results.time.to_numpy().reshape(-1, 1)
    Y_train = smoothed_results[[ "ant_x", "ant_y", "ant_z"]].to_numpy()
    
    position_interpolator = RadiusNeighborsRegressor(radius=0.1, weights='distance')
    position_interpolator.fit(X_train, Y_train)
    
    train_score = position_interpolator.score(X_train, Y_train)
    print(f"Position Interpolator Train Score: {train_score:.4f}")
    
    ping_times = shotdata.pingTime.to_numpy().reshape(-1, 1)
    return_times = shotdata.returnTime.to_numpy().reshape(-1, 1)
    
    predicted_ping_pos = position_interpolator.predict(ping_times)
    predicted_return_pos = position_interpolator.predict(return_times)
    
    shotdata.loc[:,["ant_e0","ant_n0","ant_u0"]]= predicted_ping_pos
    shotdata.loc[:,["ant_e1","ant_n1","ant_u1"]]= predicted_return_pos
    
    nan_pings = np.isnan(predicted_ping_pos).any(axis=1).sum()
    nan_returns = np.isnan(predicted_return_pos).any(axis=1).sum()
    if nan_pings > 0:
        print(f"Warning: {nan_pings} ping times could not be interpolated (no smoothed data within radius).")
    if nan_returns > 0:
        print(f"Warning: {nan_returns} return times could not be interpolated (no smoothed data within radius).")

    return shotdata

def main():
    """
    Main function to run the Kalman filter processing pipeline.
    """
    # Configurable Parameters
    
    START_DT = constants.start_dt
    GNSS_POS_PSD = constants.gnss_pos_psd 
    VEL_PSD = constants.vel_psd 
    COV_ERR = constants.cov_err 

    # Setup DataHandler
    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")
    dh = DataHandler(main_dir)
    
    network = "cascadia-gorda"
    station = "NCC1"
    survey = "2023_A_1063"
    dh.change_working_station(network=network, station=station, campaign=survey)
    
    # Load Data
    dates = dh.kin_position_tdb.get_unique_dates()
    kin_positions, shotdata, positions_data = load_data(dh, dates[8], dates[9])
    
    # Prepare data (without trimming or velocity calculation)

    positions_data_prepared = prepare_positions_data(positions_data)
    kin_data_prepared = prepare_kinematic_data(kin_positions)

    # Filter out rows where the positions are greater than 5km from median
    pos_filter_positions = (
        (positions_data_prepared.ant_x.between(MEDIAN_EAST_POSITION - 5000, MEDIAN_EAST_POSITION + 5000)) &
        (positions_data_prepared.ant_y.between(MEDIAN_NORTH_POSITION - 5000, MEDIAN_NORTH_POSITION + 5000)) &
        (positions_data_prepared.ant_z.between(MEDIAN_UP_POSITION - 5000, MEDIAN_UP_POSITION + 5000))
    )
    pos_filter_kinematic = (
        (kin_data_prepared.ant_x.between(MEDIAN_EAST_POSITION - 5000, MEDIAN_EAST_POSITION + 5000)) &
        (kin_data_prepared.ant_y.between(MEDIAN_NORTH_POSITION - 5000, MEDIAN_NORTH_POSITION + 5000)) &
        (kin_data_prepared.ant_z.between(MEDIAN_UP_POSITION - 5000, MEDIAN_UP_POSITION + 5000))
    )
    pos_filter_original_count = len(positions_data_prepared)
    kin_filter_original_count = len(kin_data_prepared)

    positions_data_prepared = positions_data_prepared[pos_filter_positions]
    kin_data_prepared = kin_data_prepared[pos_filter_kinematic]

    print(f"Positions data filtered from {pos_filter_original_count} to {len(positions_data_prepared)} rows for a {((pos_filter_original_count - len(positions_data_prepared)) / pos_filter_original_count * 100):.2f} % reduction using 5km position threshold.")
    print(f"Kinematic data filtered from {kin_filter_original_count} to {len(kin_data_prepared)} rows for a {((kin_filter_original_count - len(kin_data_prepared)) / kin_filter_original_count * 100):.2f} % reduction using 5km position threshold.")

    df_all = combine_data(positions_data_prepared, kin_data_prepared)

    smoothed_results = run_kalman_filter_and_smooth(df_all, START_DT, GNSS_POS_PSD, VEL_PSD, COV_ERR)

    # plot the results and the positions data, plot velocities with their values on the right y-axis
    if smoothed_results.empty:
        print("No smoothed results to plot or analyze.")
        return
    # fig,axes = plt.subplots(3,1,figsize=(10,8),sharex=True)
    # axes[0].scatter(smoothed_results.time, smoothed_results.ant_x, label='Smoothed East',s=1,color='blue')
    # axes[0].scatter(positions_data_prepared.time, positions_data_prepared.ant_x, label='Original East',s=1,color='orange',alpha=0.5)
    # axes[0].scatter(kin_data_prepared.time, kin_data_prepared.ant_x, label='Kinematic East',s=1,color='green',alpha=0.5)
    # # axes_0_2 = axes[0].twinx()
    # # axes_0_2.scatter(positions_data_prepared.time, positions_data_prepared.east, label='Original East Vel',s=1,color='red',alpha=0.5)
    # # axes_0_2.scatter(kin_data_prepared.time, kin_data_prepared.east, label='Kinematic East Vel',s=1,color='purple',alpha=0.5)
    # # axes_0_2.set_ylabel('East Velocity (m/s)')

    # axes[0].set_ylabel('East (m)')
    # axes[0].legend()
    # axes[1].scatter(smoothed_results.time, smoothed_results.ant_y, label='Smoothed North',s=1,color='blue')
    # axes[1].scatter(positions_data_prepared.time, positions_data_prepared.ant_y, label='Original North',s=1,color='orange',alpha=0.5)
    # axes[1].scatter(kin_data_prepared.time, kin_data_prepared.ant_y, label='Kinematic North',s=1,color='green',alpha=0.5)
    # # axes_1_2 = axes[1].twinx()
    # # axes_1_2.scatter(positions_data_prepared.time, positions_data_prepared.north, label='Original North Vel',s=1,color='red',alpha=0.5)
    # # axes_1_2.scatter(kin_data_prepared.time, kin_data_prepared.north, label='Kinematic North Vel',s=1,color='purple',alpha=0.5)
    # # axes_1_2.set_ylabel('North Velocity (m/s)')
    # axes[1].set_ylabel('North (m)')
    # axes[1].legend()

    # axes[2].scatter(smoothed_results.time, smoothed_results.ant_z, label='Smoothed Up',s=1,color='blue')
    # axes[2].scatter(positions_data_prepared.time, positions_data_prepared.ant_z, label='Original Up',s=1,color='orange',alpha=0.5)
    # axes[2].scatter(kin_data_prepared.time, kin_data_prepared.ant_z, label='Kinematic Up',s=1,color='green',alpha=0.5)
    # axes[2].set_ylabel('Up (m)')
    # # axes_2_2 = axes[2].twinx()
    # # axes_2_2.scatter(positions_data_prepared.time, positions_data_prepared.up, label='Original Up Vel',s=1,color='red',alpha=0.5)
    # # axes_2_2.scatter(kin_data_prepared.time, kin_data_prepared.up, label='Kinematic Up Vel',s=1,color='purple',alpha=0.5)
    # # axes_2_2.set_ylabel('Up Velocity (m/s)')
    # axes[2].set_xlabel('Time (s since epoch)')
    # axes[2].legend()
    # plt.tight_layout()
    # plt.show()

    merged_positions = pd.merge_asof(
        positions_data_prepared.sort_values("time"),
        smoothed_results.sort_values("time"),
        on="time",
        tolerance=pd.Timedelta('10ms').total_seconds(),
        direction="nearest",
        suffixes=('', '_smoothed')
    )
    print("\n--- Offset Analysis ---")
    print("----Results vs Original Positions----")
    analyze_offsets(merged_positions)

    merged_positions_kinematic = pd.merge_asof(
        kin_data_prepared.sort_values("time"),
        smoothed_results.sort_values("time"),
        on="time",
        tolerance=pd.Timedelta('10ms').total_seconds(),
        direction="nearest",
        suffixes=('', '_smoothed')
    )
    print("----Results vs Kinematic Positions----")
    analyze_offsets(merged_positions_kinematic)

    

    # # Split data into chunks based on time gaps
    # data_chunks = split_data_into_chunks(df_all, columns={"ant_x": 0.5, "ant_y": 0.5, "ant_z": 0.5}, max_gap_seconds=MAX_GAP_SECONDS)

    # all_merged_positions = []
    # all_shotdata_updated = []


    # for i, chunk in enumerate(data_chunks):
    #     print(f"\n--- Processing Chunk {i+1}/{len(data_chunks)} ({len(chunk)} rows) ---")
    #     if len(chunk) < 10: # Need a few points to initialize the filter
    #         print("Chunk too small to process, skipping.")
    #         continue

    #     # Calculate and clamp kinematic velocity for the chunk
    #     chunk_with_vel = calculate_and_clamp_kinematic_velocity(chunk, max_speed=MAX_SPEED)

    #     # Run Kalman Filter on the chunk
    #     smoothed_results = run_kalman_filter_and_smooth(chunk_with_vel, START_DT, GNSS_POS_PSD, VEL_PSD, COV_ERR)
        
    #     if smoothed_results.empty:
    #         print("No smoothed results for this chunk, skipping analysis.")
    #         continue

    #     # Analyze offsets for the current chunk
    #     chunk_positions_prepared = positions_data_prepared[
    #         positions_data_prepared.time.between(chunk.time.min(), chunk.time.max())
    #     ]
    #     merged_positions = pd.merge_asof(
    #         chunk_positions_prepared.sort_values("time"),
    #         smoothed_results.sort_values("time"),
    #         on="time",
    #         direction="nearest",
    #         suffixes=('', '_smoothed')
    #     )
    #     analyze_offsets(merged_positions)
    #     all_merged_positions.append(merged_positions)

    #     # Update shotdata for the time range of the current chunk
    #     chunk_shotdata = shotdata[
    #         shotdata.pingTime.between(chunk.time.min(), chunk.time.max())
    #     ]
    #     if not chunk_shotdata.empty:
    #         shotdata_updated = update_shotdata_with_smoothed_positions(chunk_shotdata.copy(), smoothed_results)
    #         all_shotdata_updated.append(shotdata_updated)

    # # --- Aggregate and Finalize Results ---
    # if all_merged_positions:
    #     final_merged_positions = pd.concat(all_merged_positions, ignore_index=True)
    #     print("\n--- Final Combined Offset Analysis ---")
    #     analyze_offsets(final_merged_positions)
    
    # if all_shotdata_updated:
    #     final_shotdata_updated = pd.concat(all_shotdata_updated, ignore_index=True)
    #     print("\n--- Final Combined Newly Computed Ping Positions (ECEF m) ---")
    #     print(final_shotdata_updated[[ "ant_e0", "ant_n0", "ant_u0"]].describe().round(4).to_string())
    #     print("\n--- Final Combined Newly Computed Reply Positions (ECEF m) ---")
    #     print(final_shotdata_updated[[ "ant_e1", "ant_n1", "ant_u1"]].describe().round(4).to_string())

if __name__ == "__main__":
    main()
