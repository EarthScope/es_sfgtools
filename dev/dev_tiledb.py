from es_sfgtools.processing.assets.tiledb_temp import TDBGNSSArray, TDBAcousticArray, TDBPositionArray, TDBShotDataArray
from es_sfgtools.processing.pipeline import DataHandler
from es_sfgtools.processing.assets.file_schemas import AssetType, AssetEntry
from pathlib import Path
import pandas as pd
import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

from es_sfgtools.processing.pipeline.plotting import plot_gnss_data


network = "Cascadia"
survey = "2023"
main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools")
dh = DataHandler(main_dir)

dh.change_working_station(network=network,station="NCL1")
dh.change_working_survey("2023")

tildb_path = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools/Cascadia/NCL1/TileDB"
)

gnss_array = TDBGNSSArray(tildb_path / "gnss_db.tdb")

acoustic_array = TDBAcousticArray(tildb_path / "acoustic.tdb")

position_array = TDBPositionArray(tildb_path / "position.tdb")

shot_data_array = TDBShotDataArray(tildb_path / "shot_data.tdb")


# Dev testing
rinex_assets = dh.get_asset_by_type("RINEX")
plot_gnss_data(gnss_array,rinex_assets)

print("Done")