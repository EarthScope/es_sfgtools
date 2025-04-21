from pathlib import Path
import logging

logging.basicConfig(level=logging.WARNING, filename="dev.log", filemode="w")
from es_sfgtools.processing.pipeline.data_handler import DataHandler
from es_sfgtools.processing.pipeline.pipelines import SV3Pipeline, SV3PipelineConfig
from es_sfgtools.processing.assets.siteconfig import GPSiteConfig
from es_sfgtools.processing.operations.site_ops import (
    CTDfile_to_svp,
    masterfile_to_siteconfig,
    leverarmfile_to_atdoffset,
)
import os
from es_sfgtools.processing.assets import AssetEntry, AssetType
from es_sfgtools.utils.archive_pull import list_campaign_files
from es_sfgtools.utils.loggers import BaseLogger

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

    survey_files = list_campaign_files(
        network=network, station=station, campaign=campaign, show_details=True
    )
    dh.add_data_remote(survey_files)
    dh.download_data()

    pipeline, config = dh.get_pipeline_sv3()
    config.novatel_config.override = False
    config.dfop00_config.override = True
    config.rinex_config.override = False
    config.rinex_config.override_products_download = False
    config.rinex_config.pride_config.sample_frequency = 0.25
    pipeline.config = config
    pipeline.run_pipeline()

    # ncc1_2024_config = dh.station_dir / "NCC1_2024_config.yaml"
    # svp_path = dh.station_dir / "NCC1_CTD_2021_fit"
    # svp_path_processed = dh.station_dir / "svp.csv"
    # if not svp_path_processed.exists():
    #     svp_df = CTDfile_to_svp(svp_path)
    #     svp_df.to_csv(svp_path_processed)

    # config = SiteConfig.from_config(ncc1_2024_config)
    # config.sound_speed_data = svp_path_processed
    # gp_handler_ncc1= dh.get_garpos_handler(site_config=config)

    # gp_handler_ncc1.prep_shotdata()
    # update_dict = {"rejectcriteria": 2.5}
    # gp_handler_ncc1.set_inversion_params(update_dict)
    # gp_handler_ncc1.run_garpos(-1)

    # station = "NFL1"
    # campaign = "2023"
    # main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/SFGTools")

    # dh = DataHandler(main_dir)

    # NCL1 = main_dir.parent/"NCL1"
    # NDP1 = main_dir.parent/"NDP1"
    # NFL1 = main_dir.parent/"NFL1"

    # dh.change_working_station(network=network,station="NCL1",campaign="2023")
    # dh.discover_data_and_add_files(
    #     NCL1
    # )

    # pipeline,config = dh.get_pipeline_sv3()

    # print(config)
    # pipeline.process_novatel()
    # pipeline.process_rinex()
    # pipeline.process_kin()
    # pipeline.process_dfop00()
    # pipeline.update_shotdata()

    # svp_path = dh.station_dir / "CTD_NCL1_Ch_Mi"
    # svp_path_processed = dh.station_dir / "svp.csv"
    # config_path = dh.station_dir / "NCL1_2023_config.yaml"
    # if not svp_path_processed.exists():
    #     svp_df = CTDfile_to_svp(svp_path)
    #     svp_df.to_csv(svp_path_processed)
    #     svp_asset = AssetEntry(type=AssetType.SVP, local_path=svp_path_processed)
    #     dh.catalog.add_entry(svp_asset)

    # config = SiteConfig.from_config(config_path)
    # config.sound_speed_data = svp_path_processed
    # gp_handler = dh.get_garpos_handler(site_config=config)
    # gp_handler.prep_shotdata()
    # gp_handler.run_garpos()
    # network='alaska-shumagins'
    # site='IVB1'
    # campaigns = ['2018_A_SFG1','2022_A_1049']

    # dh = DataHandler(Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain"))
    # for survey in surveys:
    #     dh.change_working_station(network=network,station=site,survey=survey)
    #     remote_filepaths = list_survey_files(network=network, station=site, survey=survey, show_details=True)

    #     dh.add_data_remote(remote_filepaths)
    #     dh.download_data()
    # print(dh.get_dtype_counts())
    # pipeline,config = dh.get_pipeline_sv3()
    # pipeline.process_novatel()
    # pipeline.process_rinex()
    # pipeline.process_kin()
    # pipeline.process_dfop00()
    # pipeline.update_shotdata()

    # config_path = dh.station_dir / "NCC1_2024_config.yaml"
    # svp_path = dh.station_dir / "NCC1_CTD_2021_fit"

    # config = SiteConfig.from_config(config_path)

    # svp_path_processed = dh.station_dir / "svp.csv"
    # if not svp_path_processed.exists():
    #     svp_df = CTDfile_to_svp(svp_path)
    #     svp_df.to_csv(svp_path_processed)

    # config.sound_speed_data = svp_path_processed

    # gp_handler = dh.get_garpos_handler(site_config=config)
    # gp_handler.prep_shotdata()
    # gp_handler.run_garpos()
