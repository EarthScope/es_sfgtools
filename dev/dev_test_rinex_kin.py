import os
from pathlib import Path

from es_sfgtools.processing.operations.gnss_ops import (
    PrideCLIConfig,
    get_gnss_products,
    get_nav_file,
    rinex_to_kin,
)
from es_sfgtools.utils.loggers import BaseLogger

from es_sfgtools.data_mgmt.data_handler import DataHandler

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

if __name__ == "__main__":

    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")
    BaseLogger.route_to_console()
    dh = DataHandler(main_dir)

    network = "cascadia-gorda"
    station = "NCC1"
    campaign = "2024_A_1126"

    dh.change_working_station(network=network, station=station, campaign=campaign)

    pride_config = PrideCLIConfig(
        sample_frequency=0.1,
        override_products_download=True,
        override=True,
    )
    failed_rinex_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/2024_A_1126/intermediate/NCC12870.24o"
    )

    navfile = get_nav_file(failed_rinex_path)
    get_gnss_products(rinex_path=failed_rinex_path, pride_dir=dh.pride_dir, override=True)
    kin, res = rinex_to_kin(failed_rinex_path,
                           writedir=dh.inter_dir,
                           pridedir=dh.pride_dir,
                           site=dh.current_station,
                           pride_config=pride_config
                           )
