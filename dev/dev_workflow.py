# download, preprocess, and postprocess SV3 data
from pathlib import Path
import os
os.environ["DYLD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib:" + os.environ.get("DYLD_LIBRARY_PATH", "")
PRIDE_DIR = Path.home()/".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(PRIDE_DIR)
from es_sfgtools.data_mgmt.data_handler import DataHandler

def main():
    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain2")
    network = "cascadia-gorda"
    station = "NCC1"
    campaign = "2025_A_1126"
    raw_dir_ncc1 = main_dir / network / station / campaign / "raw"
    dh = DataHandler(main_dir)
    dh.change_working_station(network=network, station=station, campaign=campaign)
    dh.discover_data_and_add_files(raw_dir_ncc1)
    ncc1_pipeline,ncc1_config = dh.get_pipeline_sv3()

    ncc1_pipeline.run_pipeline()

if __name__ == "__main__":
    main()