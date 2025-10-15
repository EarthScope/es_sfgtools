import datetime
from collections import defaultdict
from typing import List

import matplotlib.pyplot as plt
import numpy as np

from ..data_mgmt.assetcatalog.file_schemas import AssetEntry
from ..processing.assets.tiledb import TDBKinPositionArray

UNIX_EPOCH = np.datetime64("1970-01-01T00:00:00Z")
def to_timestamp(time:np.datetime64 | datetime.datetime) -> float:
    """Converts a numpy.datetime64 or datetime.datetime object to a UNIX timestamp.

    Parameters
    ----------
    time : np.datetime64 | datetime.datetime
        The time to convert.

    Returns
    -------
    float
        The UNIX timestamp.
    """
    if isinstance(time,int):
        time = datetime.datetime.fromtimestamp(time/1e9)
    if isinstance(time,datetime.datetime) or isinstance(time,datetime.date):
        time = np.datetime64(time)
    unix_epoch = np.datetime64("1970-01-01T00:00:00Z")
    return (time - UNIX_EPOCH) / np.timedelta64(1, 's')

def get_rinex_timelast(rinex_asset:AssetEntry) -> datetime.datetime:
    """Gets the last timestamp from a RINEX file.

    Parameters
    ----------
    rinex_asset : AssetEntry
        The RINEX asset entry.

    Returns
    -------
    datetime.datetime
        The last timestamp in the RINEX file.
    """
    year = str(rinex_asset.timestamp_data_start.year)[2:]
    ref_date = datetime.datetime(1970, 1, 1, 0, 0, 0)
    with open(rinex_asset.local_path) as f:
        for line in f:
            # line sample: 23  6 24 23 59 59.5000000  0  9G21G27G32G08G10G23G24G02G18
            if line.strip().startswith(year):
                date_line = line.strip().split()
                try:
                    current_date = datetime.datetime(
                        year=2000 + int(date_line[0]), 
                        month=int(date_line[1]), 
                        day=int(date_line[2]), 
                        hour=int(date_line[3]), 
                        minute=int(date_line[4]),
                        second=int(float(date_line[5]))
                    )
                    if current_date > ref_date:
                        ref_date = current_date
                except Exception:
                    pass
    return ref_date

def plot_kin_position_data(kin_position_data:TDBKinPositionArray,rinex_entries:List[AssetEntry] = []) -> None:
    """Plots KinPosition data over time.

    This function plots KinPosition data over time, with each subplot
    representing a unique month of data.

    The function performs the following steps:
    1. Extracts unique dates from the KinPosition data.
    2. Organizes the dates by year and month.
    3. Creates a subplot for each unique month.
    4. Reads the KinPosition data for the date range of each month.
    5. Plots the KinPosition data points as scatter plots.
    6. Adds vertical lines to indicate daily and hourly markers.
    7. Formats the x-axis with hourly ticks and rotates the labels for better
       readability.
    8. Sets the title for each subplot to indicate the date range of the data.
    9. Adjusts the layout and displays the plot.

    Parameters
    ----------
    kin_position_data : TDBKinPositionArray
        An object containing KinPosition data with methods to retrieve unique
        dates and read data frames.
    rinex_entries : List[AssetEntry], optional
        A list of RINEX asset entries, by default [].
    """

    kin_position_dates = kin_position_data.get_unique_dates().tolist()
    year_month_map = defaultdict(dict)
    rinex_ym_map = defaultdict(dict)
    
    if rinex_entries:
    
        for entry in rinex_entries:
            if entry.timestamp_data_end is None:
                entry.timestamp_data_end = get_rinex_timelast(entry)
            kin_position_dates.extend(
                [entry.timestamp_data_start.date(), entry.timestamp_data_end.date()])
            rinex_ym_map[entry.timestamp_data_start.year].setdefault(
                entry.timestamp_data_start.month, []).append(entry)  

    unique_months = []
    for date in kin_position_dates:
        year_month_map[date.year].setdefault(date.month, []).append(date)
        y_m = f"{date.year}-{date.month}"
        if y_m not in unique_months:
            unique_months.append(y_m)

    unique_months = [[int(x) for x in ym.split("-")] for ym in unique_months]
    fig, axes = plt.subplots(ncols=1, nrows=len(unique_months), figsize=(12, 4))
    for i, ym in enumerate(unique_months):
        if len(unique_months) == 1:
            current_ax = axes
        else:
            current_ax = axes[i]

        year, month = ym
        dates = sorted(year_month_map[year][month])
        df = kin_position_data.read_df(dates[0], dates[-1])
        df_dates = df["time"].values

      
        date_min = df_dates.min()
        date_max = df_dates.max()

        if (rinex_entries_ym := rinex_ym_map[year].get(month, [])):
            rinex_dates_start = [np.datetime64(x.timestamp_data_start).astype("datetime64[D]") for x in rinex_entries_ym]
            rinex_dates_end = [np.datetime64(x.timestamp_data_end).astype("datetime64[D]") for x in rinex_entries_ym]
            date_min = min(date_min, min(rinex_dates_start))
            date_max = max(date_max, max(rinex_dates_end))

        ref = to_timestamp(date_min)
        current = date_min.copy()
        markers_hourly = [current.astype("datetime64[D]").astype("datetime64[h]")]
        while current <= date_max:
            current = markers_hourly[-1] + np.timedelta64(6, "h")
            markers_hourly.append(current.astype("datetime64[h]"))

        markers_daily = sorted(set([x.astype("datetime64[D]") for x in markers_hourly]))

        df_timestamps = []
        df_timestamp_ticks = []
        for i in range(df_dates.shape[0]):
            df_timestamps.append(to_timestamp(df_dates[i]) - ref)
            df_timestamp_ticks.append(df_dates[i].astype("datetime64[h]"))
        rinex_timestamps = []
        rinex_timestamp_ticks = []

        for entry in rinex_ym_map[year].get(month, []):
            rinex_day_start = to_timestamp(entry.timestamp_data_start) - ref
            rinex_day_end = to_timestamp(entry.timestamp_data_end) - ref
            rinex_day = np.arange(rinex_day_start, rinex_day_end, 3600/5)
            rinex_timestamps.extend(rinex_day)
            

        hour_tick_dates = markers_hourly
        hour_tick_points = [to_timestamp(x) - ref for x in hour_tick_dates]
        day_tick_points = [to_timestamp(x) - ref for x in markers_daily]

        y_df = np.ones_like(df_dates)

        current_ax.scatter(df_timestamps, y_df, marker="_", label="Kin Position Data", color="b",s=300,linewidth=20)
        [current_ax.axvline(d, color="r", linestyle="-") for d in day_tick_points]
        current = date_min.copy().astype("datetime64[D]").astype("datetime64[h]")
        while current < date_max:
            current_ax.axvline(
                to_timestamp(current) - ref, color="k", alpha=0.1, linestyle="-"
            )
            current += np.timedelta64(1, "h")
        
        if rinex_timestamps:
            y_rnx = np.ones_like(rinex_timestamps)*2
            current_ax.scatter(rinex_timestamps, y_rnx*2, marker="_", label="RINEX Data", color="g",s=300,linewidth=20)

        current_ax.yaxis.set_ticks([])
        current_ax.xaxis.set_ticks(hour_tick_points[::2], hour_tick_dates[::2])
        # rotate x ticks
        current_ax.set_xticklabels(
            current_ax.get_xticklabels(), rotation=25, horizontalalignment="right"
        )
        current_ax.tick_params(direction="in", length=5, pad=5)
        current_ax.set_title(
            f"Kin Position Data spanning {date_min.astype('datetime64[D]')} to {date_max.astype('datetime64[D]')}"
        )
    fig.subplots_adjust(bottom=0.2)
    plt.legend()
    plt.show()