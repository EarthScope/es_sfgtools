from pathlib import Path
import logging

logging.basicConfig(level=logging.WARNING, filename="dev.log", filemode="w")
from es_sfgtools.processing.pipeline.temp import DataHandler
from es_sfgtools.processing.assets.tiledb_temp import TDBGNSSArray
from es_sfgtools.processing.assets.file_schemas import AssetType, AssetEntry
import os

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"

os.environ["PATH"] += os.pathsep + str(pride_path)

data_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools/Cascadia/NCL1/Data/intermediate")

rinex_path = data_dir / "NCL11800.23o"
pride_dir = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools/Pride"
)
write_dir = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools"
)
gnss_uri = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools/Cascadia/NCL1/TileDB/gnss_db.tdb"
)
from es_sfgtools.processing.operations.gnss_ops import rinex_to_kin,kin_to_gnssdf,novatel_to_rinex_batch
from es_sfgtools.processing.pipeline.datadiscovery import scrape_directory_local
# kinfile,resfile = rinex_to_kin(source=rinex_path,pridedir=pride_dir,writedir=write_dir)

# gnssdf = kin_to_gnssdf(kinfile)

# gnss_array = TDBGNSSArray(gnss_uri)

# gnss_array.write_df(gnssdf)

novatel_dir = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NCL1/HR"
)

novatel_assets = [x for x in scrape_directory_local(novatel_dir) if x.type == AssetType.NOVATEL770]

rinex_assets = novatel_to_rinex_batch(novatel_assets, writedir=write_dir,show_details=True)
print(rinex_assets)