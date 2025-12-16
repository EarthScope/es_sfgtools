# download, preprocess, and postprocess SV3 data
import os
from pathlib import Path

os.environ["GARPOS_PATH"] = str(Path.home() / "Project/SeaFloorGeodesy" / "garpos")
os.environ["WORKING_ENVIRONMENT"] = "ECS"
os.environ["S3_SYNC_BUCKET"] = "seafloor-public-bucket-bucket83908e77-gprctmuztrim"
os.environ["MAIN_DIRECTORY_ECS"] = "/Volumes/DunbarSSD/Project/SeafloorGeodesy/ECSDEMO"
os.environ["DYLD_LIBRARY_PATH"] = (
    os.environ.get("CONDA_PREFIX", "")
    + "/lib:"
    + os.environ.get("DYLD_LIBRARY_PATH", "")
)
PRIDE_DIR = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(PRIDE_DIR)

from es_sfgtools.workflows.workflow_handler import WorkflowHandler

def main():
    wfh = WorkflowHandler()
    network = "cascadia-community"
    station = "NSS1"
    campaign = "2024_A"
    wfh.set_network_station_campaign(
        network_id=network, station_id=station, campaign_id=campaign
    )
    #wfh.ingest_catalog_archive_data()
    #wfh.ingest_download_archive_data()

if __name__ == "__main__":
    main()