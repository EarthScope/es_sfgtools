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
from es_sfgtools.config.env_config import Environment, WorkingEnvironment
Environment.load_working_environment()
from es_sfgtools.workflows.workflow_handler import WorkflowHandler
from es_sfgtools.workflows.midprocess import IntermediateDataProcessor
from es_sfgtools.workflows.pipelines.sv3_pipeline import SV3PipelineECS
from es_sfgtools.data_mgmt.assetcatalog.schemas import AssetEntry

def main():
    wfh:WorkflowHandler = WorkflowHandler()
    network = "cascadia-community"
    station = "NSS1"
    campaign = "2024_A"
    wfh.set_network_station_campaign(
        network_id=network, station_id=station, campaign_id=campaign
    )
    pl = SV3PipelineECS(
        directory_handler=wfh.directory_handler,
        asset_catalog=wfh.asset_catalog,
    )
    pl.set_network_station_campaign(
        network_id=network, station_id=station, campaign_id=campaign
    
    )

    #pl.config.rinex_config.override = True
    #pl.get_rinex_files()
    # wfh.ingest_catalog_archive_data()
    # wfh.ingest_download_archive_data()
    #pl.process_rinex()
    #pl.update_shotdata()
    dataPostProcessor: IntermediateDataProcessor = wfh.midprocess_get_processor(override_metadata_require=True)
    dataPostProcessor.parse_surveys()

if __name__ == "__main__":
    main()
