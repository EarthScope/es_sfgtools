from es_sfgtools.processing.assets.tiledb_temp import TDBGNSSArray
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