# download, preprocess, and postprocess SV3 data
import os
from pathlib import Path

os.environ["CONDA_PREFIX"] = (
    "/Users/franklyndunbar/micromamba/envs/seafloor_geodesy_mac"
)
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
    COMBINATIONS = [
    # {"network": "cascadia-gorda", "station": "NCC1", "campaign": "2022_A_1065"},
    # {"network": "cascadia-gorda", "station": "NCC1", "campaign": "2023_A_1063"},
    # {"network": "cascadia-gorda", "station": "NCC1", "campaign": "2024_A_1126"},
    # {"network": "cascadia-gorda", "station": "NCC1", "campaign": "2025_A_1126"},
    # {"network": "cascadia-gorda", "station": "NBR1", "campaign": "2023_A_1063"},
    # {"network": "cascadia-gorda", "station": "NBR1", "campaign": "2025_A_1126"},
    # {"network": "cascadia-gorda", "station": "GCC1", "campaign": "2022_A_1065"},
    # {"network": "cascadia-gorda", "station": "GCC1", "campaign": "2023_A_1063"},
    # {"network": "cascadia-gorda", "station": "GCC1", "campaign": "2024_A_1126"},
    {"network": "cascadia-gorda", "station": "GCC1", "campaign": "2025_A_1126"},
    # {"network": "cascadia-gorda", "station": "NCL1", "campaign": "2023_A_1063"},
    # {"network": "cascadia-gorda", "station": "NDP1", "campaign": "2023_A_1063"},
    # {"network": "cascadia-gorda", "station": "NTH1", "campaign": "2025_A_1126"},
]
    wfh:WorkflowHandler = WorkflowHandler()
    for combo in COMBINATIONS:
        print(f"Processing {combo['network']}/{combo['station']}/{combo['campaign']}")
        network = combo["network"]
        station = combo["station"]
        campaign = combo["campaign"]
        wfh.set_network_station_campaign(
            network_id=network, station_id=station, campaign_id=campaign
        )

        #wfh.ingest_catalog_archive_data()
        #wfh.ingest_download_archive_data()
        
        pl = SV3PipelineECS(
            directory_handler=wfh.directory_handler,
            asset_catalog=wfh.asset_catalog,
        )
        try:
            pl.set_network_station_campaign(
                network_id=network, station_id=station, campaign_id=campaign
            
            )
        except Exception as e:
            print(f"Error setting network/station/campaign: {e}")
            continue

        #pl.config.rinex_config.override = True
        #pl.get_rinex_files()
        pl.config.dfop00_config.override = False
        pl.config.position_update_config.override = True
        #pl.process_rinex()
        pl.process_dfop00()
        pl.update_shotdata()
        dataPostProcessor: IntermediateDataProcessor = wfh.midprocess_get_processor(override_metadata_require=True)
        dataPostProcessor.parse_surveys()

if __name__ == "__main__":
    main()
