import os
from pathlib import Path

os.environ["GARPOS_PATH"] = str(Path.home() / "path"/"to"/"your"/"garpos")

os.environ["DYLD_LIBRARY_PATH"] = (
    os.environ.get("CONDA_PREFIX", "")
    + "/lib:"
    + os.environ.get("DYLD_LIBRARY_PATH", "")
)
PRIDE_DIR = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(PRIDE_DIR)


from es_sfgtools.workflows.workflow_handler import WorkflowHandler


def main():
    main_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain")

    workflow = WorkflowHandler(main_dir)

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
            "override": True
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

    NETWORK = "cascadia-gorda"
    CAMPAIGN = "2025_A_1126"
    STATIONS = ["NTH1"]#, "NCC1", "NBR1", "GCC1"]

    for station in STATIONS:
        workflow.set_network_station_campaign(
            network_id=NETWORK,
            station_id=station,
            campaign_id=CAMPAIGN,
        )
        if station == "NCC1":
            workflow.preprocess_run_pipeline_sv3(
                job='all',
                primary_config=global_config,
                secondary_config=ncc1_config,
            )
        else:
            workflow.preprocess_run_pipeline_sv3(
                job='run_pride',
                primary_config=global_config,
            )

if __name__ == "__main__":
    main()
