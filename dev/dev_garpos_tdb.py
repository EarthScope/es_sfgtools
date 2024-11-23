from pathlib import Path
import logging

logging.basicConfig(level=logging.WARNING, filename="dev.log", filemode="w")
from es_sfgtools.processing.pipeline.data_handler import DataHandler
from es_sfgtools.modeling.garpos_tools.functions import DevGarposInput
from es_sfgtools.processing.assets.siteconfig import SiteConfig
from es_sfgtools.processing.operations.site_ops import CTDfile_to_svp
from es_sfgtools.processing.assets import AssetEntry,AssetType
import os

data_dir = Path().home() / "Project/SeaFloorGeodesy/Data/Cascadia2023/NFL1"
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
catalog_path = data_dir / "catalog.sqlite"
# add to path
os.environ["PATH"] += os.pathsep + str(pride_path)
if __name__ == "__main__":

    config_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools/Cascadia/NCL1"
    ) / "NCL1_2023_config.yaml"
    network = "Cascadia"
    station = "NFL1"
    survey = "2023"
    main_dir = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools"
    )

    dh = DataHandler(main_dir)

    NCL1 = main_dir.parent / "NCL1"
    NDP1 = main_dir.parent / "NDP1"
    NFL1 = main_dir.parent / "NFL1"

    dh.change_working_station(network=network, station="NCL1", survey="2023")
    config_path = dh.station_dir / "NCL1_2023_config.yaml"
    svp_path = dh.station_dir / "CTD_NCL1_Ch_Mi"
    svp_path_processed = dh.station_dir / "svp.csv"
    if not svp_path_processed.exists():
        svp_df = CTDfile_to_svp(svp_path)
        svp_df.to_csv(svp_path_processed)
        svp_asset = AssetEntry(
            type=AssetType.SVP,
            local_path=svp_path_processed
        )
        dh.catalog.add_entry(svp_asset)

    config = SiteConfig.from_config(config_path)
    config.sound_speed_data = svp_path_processed
    garposInput = DevGarposInput(shotdata=dh.shotdata_tdb,site_config=config,working_dir=dh.station_dir)
    garposInput.prep_shotdata(overwrite=True)
    garposInput.run_garpos()
