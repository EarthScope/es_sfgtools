import os
from pathlib import Path

os.environ["GARPOS_PATH"] = str(Path("/Users/franklyndunbar/Project/garpos").resolve())

from es_sfgtools.processing.pipeline.data_handler import DataHandler

from es_sfgtools.utils.archive_pull import load_site_metadata
from es_sfgtools.utils.loggers import BaseLogger

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)


if __name__ == "__main__":
    from es_sfgtools.utils.loggers import BaseLogger
    BaseLogger.route_to_console()

    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")

    network = "cascadia-gorda"
    station = "NCC1"
    campaign = "2024_A_1126"
    dh = DataHandler(main_dir)
    dh.change_working_station(network=network, station=station, campaign=campaign)


    site = load_site_metadata(network=network, station=station, profile="dev")
    gp_handler_ncc1 = dh.get_garpos_handler(site_data=site)
    gp_handler_ncc1.set_campaign(campaign)
    gp_handler_ncc1.prep_shotdata(True)
    gp_handler_ncc1.load_sound_speed_data()
    

    update_dict = {"rejectcriteria": 2.5,"log_lambda":[0]}

    gp_handler_ncc1.set_inversion_params(update_dict)

    gp_handler_ncc1.run_garpos(campaign_id='2024_A_1126',run_id=0,override=True)

    gp_handler_ncc1.plot_ts_results(campaign_name='2024_A_1126',survey_id="2024_A_1126_1",res_filter=20)
