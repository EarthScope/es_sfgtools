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
    campaign = "2025_A_1126"
    run_id = "Test"
    dh = DataHandler(main_dir)

    station = "NBR1"
    raw_dir_ncc1 = main_dir / network / station / campaign / "raw"
    dh.change_working_station(network=network, station=station, campaign=campaign)
    dh.discover_data_and_add_files(raw_dir_ncc1)
    nbr1_pipeline,nbr1_config = dh.get_pipeline_sv3()
    nbr1_pipeline.run_pipeline()
    nbr1MidProcess:IntermediateDataProcessor = dh.getIntermediateDataProcessor()
    nbr1MidProcess.parse_surveys(override=False)
    nbr1MidProcess.prepare_shotdata_garpos()
    gp_handler_nbr1: GarposHandler = dh.getGARPOSHandler()
    gp_handler_nbr1.run_garpos(run_id=run_id,iterations=2,override=True)
    gp_handler_nbr1.plot_ts_results(run_id=run_id,res_filter=10,savefig=True,showfig=False)
    
    station = "NCC1"
    raw_dir_ncc1 = main_dir / network / station / campaign / "raw"
    dh.change_working_station(network=network, station=station, campaign=campaign)
    dh.discover_data_and_add_files(raw_dir_ncc1)
    ncc1_pipeline,ncc1_config = dh.get_pipeline_sv3()
    ncc1_pipeline.run_pipeline()
    ncc1MidProcess:IntermediateDataProcessor = dh.getIntermediateDataProcessor()
    ncc1MidProcess.parse_surveys()
    ncc1MidProcess.prepare_shotdata_garpos()
    gp_handler_ncc1: GarposHandler = dh.getGARPOSHandler()
    gp_handler_ncc1.run_garpos(run_id=run_id, iterations=2, override=True)
    gp_handler_ncc1.plot_ts_results(run_id=run_id, res_filter=10, savefig=True,showfig=False)

    station = 'NTH1'
    raw_dir_nth1 = main_dir / network / station / campaign / "raw"
    dh.change_working_station(network=network, station=station, campaign=campaign)
    dh.discover_data_and_add_files(raw_dir_nth1)
    nth1_pipeline,nth1_config = dh.get_pipeline_sv3()
    nth1_pipeline.run_pipeline()
    nth1MidProcess:IntermediateDataProcessor = dh.getIntermediateDataProcessor()
    nth1MidProcess.parse_surveys()
    nth1MidProcess.prepare_shotdata_garpos(overwrite=True)
    gp_handler_nth1: GarposHandler = dh.getGARPOSHandler()
    gp_handler_nth1.run_garpos(run_id=run_id, iterations=2, override=True)
    gp_handler_nth1.plot_ts_results(run_id=run_id, res_filter=10, savefig=True,showfig=False)


if __name__ == "__main__":
    main()
