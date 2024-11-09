from es_sfgtools.processing.assets.tiledb_temp import TDBGNSSArray, TDBAcousticArray, TDBPositionArray, TDBShotDataArray
from es_sfgtools.processing.pipeline import DataHandler
from es_sfgtools.processing.assets.file_schemas import AssetType, AssetEntry
from pathlib import Path
import pandas as pd
import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

tildb_path = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools/Cascadia/NCL1/TileDB"
)

gnss_array = TDBGNSSArray(tildb_path / "gnss_db.tdb")

acoustic_array = TDBAcousticArray(tildb_path / "acoustic.tdb")

position_array = TDBPositionArray(tildb_path / "position.tdb")

shot_data_array = TDBShotDataArray(tildb_path / "shot_data.tdb")


# Dev testing
UNIX_EPOCH = np.datetime64("1970-01-01T00:00:00Z")
def to_timestamp(time:np.datetime64) -> float:
    if isinstance(time,int):
        time = datetime.datetime.fromtimestamp(time/1e9)
    unix_epoch = np.datetime64("1970-01-01T00:00:00Z")
    return (time - UNIX_EPOCH) / np.timedelta64(1, 's')

gnss_dates = gnss_array.get_unique_dates().tolist()

year_month_map = defaultdict(dict)
unique_months = []
for date in gnss_dates:
    year_month_map[date.year].setdefault(date.month, []).append(date)
    y_m = f"{date.year}-{date.month}"
    if y_m not in unique_months:
        unique_months.append(y_m)


unique_months = [[int(x) for x in ym.split("-")] for ym in unique_months]
print(unique_months)

fig, axes = plt.subplots(ncols=1,nrows=len(unique_months),figsize=(16,4))
for i,ym in enumerate(unique_months):
    if len(unique_months) == 1:
        current_ax = axes
    else:
        current_ax = axes[i]

    year,month = ym
    dates = sorted(year_month_map[year][month])
    df = gnss_array.read_df(dates[0],dates[-1])
    df_dates = df["time"].values
    ref = to_timestamp(df_dates[0])
    date_min = df_dates.min()
    date_max = df_dates.max()

    current = date_min.copy()
    markers = [current.astype('datetime64[h]')]
    while current < date_max:
        current += np.timedelta64(6, "h")
        markers.append(current.astype('datetime64[h]'))

    df_timestamps = []
    timestamp_ticks = []
    for i in range(df_dates.shape[0]):
        df_timestamps.append(to_timestamp(df_dates[i]) - ref)
        timestamp_ticks.append(df_dates[i].astype('datetime64[h]'))

    timestamp_tick_indices = timestamp_ticks.copy()

    # date_difs = np.diff(df_timestamps)
    # for i in range(date_difs.shape[0]):
    #     if date_difs[i] > 3600*12:
    #         df_timestamps[i+1:] -= date_difs[i]
    #         timestamp_tick_indices[i+1:] -= np.timedelta64(int(date_difs[i]*1000),'ms')

    # hour_tick_map = {}
    # for idx,ts in zip(timestamp_tick_indices,timestamp_ticks):
    #     hour_tick_map[ts] = idx

    # hour_tick_dates = list(hour_tick_map.keys())
    # hour_tick_indices = list(hour_tick_map.values())
    # hour_tick_points = [to_timestamp(x) - ref for x in hour_tick_indices]
    hour_tick_dates = markers
    hour_tick_points = [to_timestamp(x) - ref for x in hour_tick_dates]
    y = np.ones_like(df_dates)

    current_ax.scatter(df_timestamps,y,marker='|')
    current_ax.yaxis.set_ticks([])
    current_ax.xaxis.set_ticks(hour_tick_points,hour_tick_dates)
    # rotate x ticks
    current_ax.set_xticklabels(current_ax.get_xticklabels(), rotation=25, horizontalalignment='right')
    current_ax.tick_params(direction='in',length=5,pad=5)
    current_ax.set_title(
        f"GNSS Data spanning {date_min.astype('datetime64[D]')} to {date_max.astype('datetime64[D]')}"
    )
fig.subplots_adjust(bottom=0.2)
plt.show()

print("Done")
