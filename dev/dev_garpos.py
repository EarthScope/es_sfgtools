import os
from pathlib import Path
os.environ["DYLD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib:" + os.environ.get("DYLD_LIBRARY_PATH", "")
os.environ["GARPOS_PATH"] = str(
    Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/garpos").resolve()
)

from es_sfgtools.data_mgmt.data_handler import DataHandler
from es_sfgtools.modeling.garpos_tools.load_utils import load_drive_garpos
from es_sfgtools.utils.archive_pull import load_site_metadata
from es_sfgtools.logging.loggers import BaseLogger

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

try:
    drive_garpos = load_drive_garpos()
except Exception as e:
    from garpos import drive_garpos

from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposFixed,
    InversionParams,
    GarposInput,
    GPTransponder,
    GPATDOffset,
    GPPositionENU,
    GPPositionLLH,
    ObservationData,
)
from es_sfgtools.modeling.garpos_tools.functions import (
    CoordTransformer,
    process_garpos_results,
    rectify_shotdata,
)

if __name__ == "__main__":
    from es_sfgtools.logging import BaseLogger
    from es_sfgtools.modeling.garpos_tools.schemas import GPPositionENU
    BaseLogger.route_to_console()

    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")

    network = "cascadia-gorda"
    station = "NCC1"
    campaign = "2025_A_1126"
    dh = DataHandler(main_dir)
    dh.change_working_station(network=network, station=station, campaign=campaign)

    site = load_site_metadata(network=network, station=station)
    gp_handler_ncc1 = dh.get_garpos_handler(site_data=site)
    gp_handler_ncc1.load_sound_speed_data(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/GARPOS/2024_A_1126/svp.csv"
    )
    #gp_handler_ncc1.set_survey("2025_A_1126_1")
    gp_handler_ncc1.prep_shotdata(custom_filters={"ping_replies":{"min_replies":1}})

    update_dict = {"rejectcriteria": 2,"log_lambda":[-1,0],"delta_center_position":GPPositionENU(east_sigma=10,north_sigma=10,up_sigma=10),"convcriteria":0.1,"maxloop":15}

    gp_handler_ncc1.set_inversion_params(update_dict)


    # input_path = Path(
    #     "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/GARPOS/2024_A_1126/2024_A_1126_1/results/_3_observation.ini"
    # )
    # fixed_path = Path(
    #     "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/GARPOS/2024_A_1126/2024_A_1126_1/results/_3_settings.ini"
    # )
    # results_dir = Path(
    #     "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/GARPOS/2024_A_1126/2024_A_1126_1/results"
    # )
    # rf = drive_garpos(
    #     str(input_path),
    #     str(fixed_path),
    #     str(results_dir) + "/",
    #     "test_with_john",
    #     13,
    # )

    # results = GarposInput.from_datafile(rf)
    # proc_results, results_df = process_garpos_results(results)

    # print(proc_results)
    gp_handler_ncc1.run_garpos(run_id=0,override=True,iterations=10)

    # gp_handler_ncc1.plot_ts_results(campaign_name='2024_A_1126',survey_id="2024_A_1126_1",res_filter=20)
