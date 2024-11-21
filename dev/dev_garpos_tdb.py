from pathlib import Path
import logging

logging.basicConfig(level=logging.WARNING, filename="dev.log", filemode="w")
from es_sfgtools.processing.pipeline.data_handler import DataHandler
from es_sfgtools.modeling.garpos_tools.functions import DevGarposInput
import os

data_dir = Path().home() / "Project/SeaFloorGeodesy/Data/Cascadia2023/NFL1"
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
catalog_path = data_dir / "catalog.sqlite"
# add to path
os.environ["PATH"] += os.pathsep + str(pride_path)
if __name__ == "__main__":

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
    
    garposInput = DevGarposInput(shotdata=dh.shotdata_tdb,)
