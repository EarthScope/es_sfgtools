from es_sfgtools.processing.assets.tiledb_temp import TDBGNSSArray, TDBAcousticArray, TDBPositionArray, TDBShotDataArray
from es_sfgtools.processing.pipeline import DataHandler
from es_sfgtools.processing.assets.file_schemas import AssetType, AssetEntry
from pathlib import Path
import pandas as pd
import datetime
dh_dir_sv3 = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NCL1/"
    )
tildb_path = dh_dir_sv3 / "test.tdb"

gnss_array = TDBGNSSArray(tildb_path)

acoustic_array = TDBAcousticArray(dh_dir_sv3 / "acoustic.tdb")

position_array = TDBPositionArray(dh_dir_sv3 / "position.tdb")

shot_data_array = TDBShotDataArray(dh_dir_sv3 / "shot_data.tdb")



network = "NCB"
station = "NCB1"
survey = "2023"

dh = DataHandler(network=network, station=station, survey=survey, data_dir=dh_dir_sv3)

gnss_entries = dh.catalog.get_assets(
    network=network, station=station, survey=survey, type=AssetType.GNSS
)

time_min = datetime.datetime(1900,1,1,0,0,0)
time_max = datetime.datetime(2100,1,1,0,0,0)
total_rows = 0
for entry in gnss_entries:
    df = pd.read_csv(entry.local_path).drop(labels=["modified_julian_date","second_of_day"],axis=1)
    gnss_array.write_df(df)
    total_rows += df.shape[0]

# query the data
df = gnss_array.read_df(start=time_min,end=time_max)
print(df)


acoustic_entries = dh.catalog.get_assets(
    network=network, station=station, survey=survey, type=AssetType.ACOUSTIC
)

for entry in acoustic_entries:
    df = pd.read_csv(entry.local_path)
    acoustic_array.write_df(df)
  
acoustic_df = acoustic_array.read_df(start=time_min,end=time_max)
print(acoustic_df)

position_entries = dh.catalog.get_assets(
    network=network, station=station, survey=survey, type=AssetType.POSITION
)

# for entry in position_entries:
#     df = pd.read_csv(entry.local_path)
#     position_array.write_df(df)

# position_df = position_array.read_df(start=time_min,end=time_max)
# print(position_df)

shot_data_entries = dh.catalog.get_assets(
    network=network, station=station, survey=survey, type=AssetType.SHOTDATA
)

for entry in shot_data_entries:
    df = pd.read_csv(entry.local_path)
    shot_data_array.write_df(df)

shot_data_df = shot_data_array.read_df(start=time_min,end=time_max)
print(shot_data_df)
