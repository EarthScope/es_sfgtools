from pathlib import Path
import logging
logging.basicConfig(level=logging.WARNING,filename="dev.log",filemode="w")
from es_sfgtools.processing.pipeline.temp import DataHandler
import os

data_dir = (
    Path().home()
    / "Project/SeaFloorGeodesy/Data/Cascadia2023/NFL1"
)
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
catalog_path = data_dir/"catalog.sqlite"
# add to path
os.environ["PATH"] += os.pathsep + str(pride_path)
if __name__ == "__main__":

    network = "Cascadia"
    station = "NFL1"
    survey = "2023"
    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools")
    
    dh = DataHandler(main_dir)

    NCL1 = main_dir.parent/"NCL1"
    NDP1 = main_dir.parent/"NDP1"
    NFL1 = main_dir.parent/"NFL1"

    dh.discover_data_directory(
        network=network,
        station="NCL1",
        survey="2023",
        dir_path=NCL1,
    )
    dh.change_working_station(network=network,station="NCL1")
    dh.change_working_survey("2023")
    dh.pipeline_sv3()
    
    # dh.discover_data_directory(
    #     network=network,
    #     station="NDP1",
    #     survey="2023",
    #     dir_path=NDP1,
    # )
    # dh.change_working_station(network=network,station="NDP1")
    # dh.change_working_survey("2023")
    # dh.pipeline_sv3()

    # dh.discover_data_directory(
    #     network=network,
    #     station="NFL1",
    #     survey="2023",
    #     dir_path=NFL1,
    # )
    # dh.change_working_station(network=network,station="NFL1")
    # dh.change_working_survey("2023")
    # dh.pipeline_sv3()
