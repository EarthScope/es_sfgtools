import os
from pathlib import Path
from es_sfgtools.data_mgmt.data_handler import DataHandler
from es_sfgtools.logging import BaseLogger
from es_sfgtools.utils.archive_pull import list_campaign_files  


pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

if __name__ == "__main__":

    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")
    BaseLogger.route_to_console()
    dh = DataHandler(main_dir)

    network = "cascadia-gorda"
    station = "NCC1"
    campaign = "2024_A_1126"
    dh.change_working_station(network, station, campaign)

    pipeline,config = dh.get_pipeline_sv3()
    config.novatel_config.override = True
    pipeline.pre_process_novatel()
