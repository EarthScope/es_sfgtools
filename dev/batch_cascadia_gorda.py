import sys
from pathlib import Path
sys.path.append(Path(__file__).parent.parent.as_posix())
from app.src import PipelineManifest, run_manifest

from pathlib import Path
import os
from es_sfgtools.workflows.workflow_handler import WorkflowHandler

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)
manifest_path = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/NCC1-preproc-manifest.json"
)
DEFAULT_CONFIG = {
    # The destination S3 bucket for syncing TileDB arrays
    "S3_SYNC_BUCKET": "seafloor-public-bucket-bucket83908e77-gprctmuztrim",
}
for key, value in DEFAULT_CONFIG.items():
    os.environ[key] = value
from es_sfgtools.config.env_config import Environment

# This will read the environment variables set above
Environment.load_working_environment()

if __name__ == "__main__":
    manifest_object:PipelineManifest = PipelineManifest.load(manifest_path)
    # run_manifest(
    #     manifest_object=manifest_object
    # )
    COMBINATIONS = [
        {"network": "cascadia-gorda", "station": "NCC1", "campaign": "2022_A_1065"},
        {"network": "cascadia-gorda", "station": "NCC1", "campaign": "2023_A_1063"},
        {"network": "cascadia-gorda", "station": "NCC1", "campaign": "2024_A_1126"},
        {"network": "cascadia-gorda", "station": "NCC1", "campaign": "2025_A_1126"},
        {"network": "cascadia-gorda", "station": "NBR1", "campaign": "2023_A_1063"},
        {"network": "cascadia-gorda", "station": "NBR1", "campaign": "2025_A_1126"},
        {"network": "cascadia-gorda", "station": "GCC1", "campaign": "2022_A_1065"},
        {"network": "cascadia-gorda", "station": "GCC1", "campaign": "2023_A_1063"},
        {"network": "cascadia-gorda", "station": "GCC1", "campaign": "2024_A_1126"},
        {"network": "cascadia-gorda", "station": "GCC1", "campaign": "2025_A_1126"},
        {"network": "cascadia-gorda", "station": "NCL1", "campaign": "2023_A_1063"},
        {"network": "cascadia-gorda", "station": "NDP1", "campaign": "2023_A_1063"},
        {"network": "cascadia-gorda", "station": "NTH1", "campaign": "2025_A_1126"},
    ]
    workflow = WorkflowHandler(directory=manifest_object.main_directory)
    for combo in COMBINATIONS:
        print(f"Processing {combo['network']}/{combo['station']}/{combo['campaign']}")
        workflow.set_network_station_campaign(
            network_id=combo["network"],
            station_id=combo["station"],
            campaign_id=combo["campaign"],
        )
        # Sync TileDB arrays to S3
        workflow.midprocess_upload_s3(overwrite=False, override_metadata_require=True)
