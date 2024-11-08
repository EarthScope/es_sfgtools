from pathlib import Path
import logging

logging.basicConfig(level=logging.WARNING, filename="dev.log", filemode="w")
from es_sfgtools.processing.pipeline.temp import DataHandler
from es_sfgtools.processing.assets.tiledb_temp import TDBGNSSArray
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
from es_sfgtools.processing.operations.gnss_ops import rinex_to_kin,kin_to_gnssdf

kinfile,resfile = rinex_to_kin(source=rinex_path,pridedir=pride_dir,writedir=write_dir)

gnssdf = kin_to_gnssdf(kinfile)

gnss_array = TDBGNSSArray(gnss_uri)

gnss_array.write_df(gnssdf)
