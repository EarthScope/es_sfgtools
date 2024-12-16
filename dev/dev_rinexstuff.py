from pathlib import Path
import logging

logging.basicConfig(level=logging.WARNING, filename="dev.log", filemode="w")

from es_sfgtools.processing.assets.tiledb_temp import TDBGNSSArray
from es_sfgtools.processing.assets.file_schemas import AssetType, AssetEntry
import os

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"

os.environ["PATH"] += os.pathsep + str(pride_path)

data_dir = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/"
)

assert data_dir.exists(), "Data directory does not exist"

alaska_shumagins_dir = data_dir / "alaska-shumagins"
site_dir = alaska_shumagins_dir / "IVB1"
campaign_dir = site_dir / "2018_A_SFG1"

assert campaign_dir.exists(), "Campaign directory does not exist"
rinex_path = campaign_dir / "IVB11560.18o"

pride_dir = data_dir / "Pride"
write_dir = campaign_dir / "intermediate"


from es_sfgtools.processing.operations.gnss_ops import rinex_to_kin,kin_to_gnssdf,novatel_to_rinex_batch
from es_sfgtools.processing.pipeline.datadiscovery import scrape_directory_local
# kinfile,resfile = rinex_to_kin(source=rinex_path,pridedir=pride_dir,writedir=write_dir)

# gnssdf = kin_to_gnssdf(kinfile)

# gnss_array = TDBGNSSArray(gnss_uri)

# gnss_array.write_df(gnssdf)

rinex_to_kin(source=rinex_path,pridedir=pride_dir,writedir=write_dir,site="IVB1")
