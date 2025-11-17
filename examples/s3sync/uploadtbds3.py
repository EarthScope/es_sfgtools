"""
Seafloor Geodesy Data Processing Demo - Upload TileDB Arrays to S3

This demo shows the steps to sync TileDB arrays with preprocessed data to an S3 bucket.

"""
# =============================================================================
# CONFIGURATION
# NOTE: Ensure you have AWS credentials set via environment variables:
# "AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY","AWS_SESSION_TOKEN"
# or via 'aws sso login'
# =============================================================================

DEFAULT_CONFIG = {
    # The destination S3 bucket for syncing TileDB arrays
    "S3_SYNC_BUCKET": "seafloor-public-bucket-bucket83908e77-gprctmuztrim",
}
import os
for key, value in DEFAULT_CONFIG.items():
    os.environ[key] = value

from es_sfgtools.config.env_config import Environment
# This will read the environment variables set above
Environment.load_working_environment()

from es_sfgtools.workflows.workflow_handler import WorkflowHandler


NETWORK = "cascadia-gorda"
CAMPAIGN = "2025_A_1126" # Note: The specific campaign does not matter as it's station centric.
STATIONS = ["NTH1", "NCC1", "NBR1", "GCC1","NCL1","NDP1"]
HOME_DIR = "/path/to/SeafloorGeodesy/SFGMain"
workflow = WorkflowHandler(directory=HOME_DIR)

for station in STATIONS:
    workflow.set_network_station_campaign(
        network_id=NETWORK,
        station_id=station, # The specific station to process
        campaign_id=CAMPAIGN,
    )
    # Sync TileDB arrays to S3
    workflow.midprocess_upload_s3(overwrite=False,override_metadata_require=True)
