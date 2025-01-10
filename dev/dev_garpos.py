import os
from pathlib import Path

from es_sfgtools.processing.pipeline.data_handler import DataHandler
from es_sfgtools.processing.assets.siteconfig import SiteConfig
from es_sfgtools.processing.operations.site_ops import (
    CTDfile_to_svp,
    masterfile_to_siteconfig,
    leverarmfile_to_atdoffset,
)
from es_sfgtools.utils.loggers import BaseLogger

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

if __name__ == "__main__":

    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")
    BaseLogger.route_to_console()
    dh = DataHandler(main_dir)

    network = "cascadia-gorda"
    station = "NCC1"
    survey = "2024_A_1126"

    dh.change_working_station(network=network, station=station, survey=survey)

    ncc1_2024_config = dh.station_dir / "NCC1_2024_config.yaml"
    svp_path = dh.station_dir / "NCC1_CTD_2021_fit"
    svp_path_processed = dh.station_dir / "svp.csv"
    if not svp_path_processed.exists():
        svp_df = CTDfile_to_svp(svp_path)
        svp_df.to_csv(svp_path_processed)

    config = SiteConfig.from_config(ncc1_2024_config)
    config.sound_speed_data = svp_path_processed
    gp_handler_ncc1= dh.get_garpos_handler(site_config=config)

    gp_handler_ncc1.prep_shotdata()
    update_dict = {"rejectcriteria": 2.5}
    gp_handler_ncc1.set_inversion_params(update_dict)
    gp_handler_ncc1.run_garpos(-1)
