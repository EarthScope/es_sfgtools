# download, preprocess, and postprocess SV3 data
import os
from pathlib import Path

os.environ["DYLD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib:" + os.environ.get("DYLD_LIBRARY_PATH", "")
PRIDE_DIR = Path.home()/".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(PRIDE_DIR)
from es_sfgtools.data_mgmt.data_handler import DataHandler
from es_sfgtools.data_mgmt.post_processing import IntermediateDataProcessor
from es_sfgtools.modeling.garpos_tools.garpos_handler import GarposHandler


def main():
    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain2")
    network = "cascadia-gorda"
    station = "NBR1"
    campaign = "2025_A_1126"
    raw_dir_ncc1 = main_dir / network / station / campaign / "raw"
    dh = DataHandler(main_dir)
    dh.change_working_station(network=network, station=station, campaign=campaign)
    dh.discover_data_and_add_files(raw_dir_ncc1)
    ncc1_pipeline,ncc1_config = dh.get_pipeline_sv3()

    ncc1_pipeline.run_pipeline()
    
    ncc1MidProcess:IntermediateDataProcessor = dh.getIntermediateDataProcessor()
    ncc1MidProcess.parse_surveys()
    ncc1MidProcess.prepare_shotdata_garpos()

    gp_handler_ncc1: GarposHandler = dh.getGARPOSHandler()
    gp_handler_ncc1.run_garpos(run_id="Test",iterations=2,override=True)

if __name__ == "__main__":
    main()