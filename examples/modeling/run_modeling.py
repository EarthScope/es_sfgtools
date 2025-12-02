import os
from pathlib import Path

os.environ["GARPOS_PATH"] = str(Path.home() / "Project/SeaFloorGeodesy" / "garpos")

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

    custom_config = {
        "maxloop":20,
    }
    custom_filters = {
        "pride_residuals": {"enabled": False, "max_residual_mm": 8},
        "max_distance_from_center": {"enabled": False, "max_distance_m": 500.0},
        "ping_replies": {"enabled": False, "min_replies": 1},
        "acoustic_filters": {"enabled": True, "level": "OK"},
    }
    NETWORK = "cascadia-gorda"
    CAMPAIGN = "2025_A_1126"
    STATIONS = ["NTH1", "NCC1", "NBR1", "GCC1"]

    for station in STATIONS:
        workflow.set_network_station_campaign(
            network_id=NETWORK,
            station_id=station,
            campaign_id=CAMPAIGN,
        )



        workflow.modeling_run_sfgdstf(
            run_id="sfgdstf_modeling_run",
            iterations=3,
            override=True,
            custom_settings=custom_config,
        )


if __name__ == "__main__":
    main()
