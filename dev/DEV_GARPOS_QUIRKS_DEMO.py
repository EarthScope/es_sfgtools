# download, preprocess, and postprocess SV3 data
import os
from pathlib import Path

os.environ["GARPOS_PATH"] = str(Path.home() / "Project/SeaFloorGeodesy" / "garpos")

os.environ["DYLD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib:" + os.environ.get("DYLD_LIBRARY_PATH", "")
PRIDE_DIR = Path.home()/".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(PRIDE_DIR)

from es_sfgtools.workflows.workflow_handler import WorkflowHandler


def main():
    main_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain")
    wfh = WorkflowHandler(main_dir)

    network = "cascadia-gorda"
    campaign = "2025_A_1126"

    filter_config = {
        "pride_residuals": {"enabled": False, "max_residual_mm": 8},
        "max_distance_from_center": {"enabled": False, "max_distance_m": 500.0},
        "ping_replies": {"enabled": False, "min_replies": 1},
        "acoustic_filters": {"enabled": True, "level": "OK"}
    }
    global_config = {
        "dfop00_config": {
        "override": True
        },
        "novatel_config": {
        "n_processes": 14,
        "override": False
        },
        "position_update_config": {
        "override": True,
        "lengthscale": 0.1,
        "plot": False
        },
        "pride_config": {
        "cutoff_elevation": 7,
        "end": None,
        "frequency": ["G12", "R12", "E15", "C26", "J12"],
        "high_ion": None,
        "interval": None,
        "local_pdp3_path": None,
        "loose_edit": True,
        "sample_frequency": 1,
        "start": None,
        "system": "GREC23J",
        "tides": "SOP",
        "override_products_download": False,
        "override": False
        },
        "rinex_config": {
        "n_processes": 14,
        "time_interval": 24,
        "override": False
        }
    }

    ncc1_config = { "pride_config": {
            "cutoff_elevation": 7,}
    }

    override_survey_parsing = False
    override_garpos_parsing = False

    # station = "NCC1"
    # run_id = "NCC1_Test1"
    # raw_dir_ncc1 = main_dir / network / station / campaign / "raw"
    # wfh.set_network_station_campaign(network_id=network, station_id=station, campaign_id=campaign)
    # wfh.ingest_add_local_data(directory_path=raw_dir_ncc1)
    # wfh.preprocess_run_pipeline_sv3(primary_config=global_config,secondary_config=ncc1_config)
    os.environ["S3_SYNC_BUCKET"] = (
        "seafloor-public-bucket-bucket83908e77-gprctmuztrim"
    )
    # wfh.midprocess_get_sitemeta()
    # mid_processer = wfh.midprocess_get_processor()
    # mid_processer.midprocess_sync_s3()

    # station = 'NBR1'
    # wfh.set_network_station_campaign(network_id=network, station_id=station, campaign_id=campaign)
    # mid_processer = wfh.midprocess_get_processor()
    # mid_processer.midprocess_sync_s3()

    # station = 'GCC1'
    # wfh.set_network_station_campaign(network_id=network, station_id=station, campaign_id=campaign)
    # mid_processer = wfh.midprocess_get_processor()
    # mid_processer.midprocess_sync_s3()

    station = 'NTH1'
    wfh.set_network_station_campaign(network_id=network, station_id=station, campaign_id=campaign)
    mid_processer = wfh.midprocess_get_processor()
    mid_processer.midprocess_sync_s3()

    # wfh.midprocess_parse_surveys(override=override_survey_parsing,write_intermediate=True)
    # wfh.midprocess_prep_garpos(override=override_garpos_parsing,custom_filters=filter_config,survey_id="2025_A_1126_1")
    # wfh.midprocess_prep_garpos()
    # wfh.modeling_run_garpos(run_id=run_id,iterations=2,override=True,custom_settings={"maxloop":50})
    # wfh.modeling_plot_garpos_results(run_id=run_id,residuals_filter=10)

if __name__ == "__main__":
    main()
