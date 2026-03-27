import os
from pathlib import Path

from es_sfgtools.workflows.workflow_handler import WorkflowHandler

global_config = {
    "dfop00_config": {"override": False},
    "novatel_config": {"n_processes": 5, "override": False},
    "position_update_config": {"override": False, "lengthscale": 0.1, "plot": False},
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
        "override": True,
    },
    "rinex_config": {
        "n_processes": 5, 
        "time_interval": 24, 
        "override": False},
}

ncc1_config = {
    "pride_config": {
        "cutoff_elevation": 7,
    }
}

# Input survey parameters
network='cascadia-gorda'
site='NCC1'
campaign='2025_A_1126'

# Set data directory path for local environment
data_dir = Path(f"{os.path.expanduser('~/data/sfg_march15')}")
raw_data_dir = data_dir / network / site / campaign / "raw"

def main():
    #### USE THE FOLLOWING DEFAULTS UNLESS DESIRED ####
    os.makedirs(data_dir, exist_ok=True)
    workflow = WorkflowHandler(directory=data_dir)
    workflow.set_network_station_campaign(network_id=network, 
                                        station_id=site, 
                                        campaign_id=campaign)
                                        
    print(f"Workflow directory: {workflow.directory}")
    print(f"Raw data directory for campaign: {raw_data_dir}")

    # 1. Ingest catalog data
    workflow.ingest_catalog_archive_data()
    workflow.ingest_download_intermediate_archive_data(rinex_1Hz=False)
    workflow.preprocess_run_pipeline_sv3(job="intermediate", 
                                        primary_config=global_config)



if __name__ == "__main__":
    main()