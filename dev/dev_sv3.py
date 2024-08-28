import sys
from pathlib import Path
import json
import es_sfgtools
import logging
logging.basicConfig(level=logging.INFO,filename="dev.log",filemode="w")
from es_sfgtools.pipeline import DataHandler
import os

data_dir = Path().home() / "Project/SeaFloorGeodesy/Data/NCB1/HR/"
data_files = [str(x) for x in data_dir.glob("*")]
catalog_path = Path().home() / "Project/SeaFloorGeodesy/Data/TestSV3"
catalog_path.mkdir(exist_ok=True)
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
# add to path
os.environ["PATH"] += os.pathsep + str(pride_path)
if __name__ == "__main__":
    dh = DataHandler(catalog_path)

    network = "NCB"
    station = "NCB1"
    survey = "TestSV3"


    dh.add_data_local(
        network=network,
        station=station,
        survey=survey,
        local_filepaths=data_files,
        discover_file_type=True
    )

    dh.process_campaign_data(
        network=network,
        station=station,
        survey=survey,
        override=False,
        show_details=False
    )

    # dh.get_observation_session_data(
    #     network=network,
    #     station=station,
    #     survey=survey,
    #     plot=True
    # )
