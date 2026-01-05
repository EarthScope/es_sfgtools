"""
Seafloor Geodesy Data Processing Demo - GeoLab Environment

This demo shows the steps to get pre-processed data for modeling in the geolab environment.

"""
# =============================================================================
# CONFIGURATION
# NOTE: Ensure that the correct environment variables are set for GeoLab processing (via dockerfile or manually)
# =============================================================================

DEFAULT_CONFIG = {
    # GeoLab environment settings
    "WORKING_ENVIRONMENT": "GEOLAB",
    "MAIN_DIRECTORY_GEOLAB": "/Volumes/DunbarSSD/Project/SeafloorGeodesy/GEOLABDemo",
    "S3_SYNC_BUCKET": "seafloor-public-bucket-bucket83908e77-gprctmuztrim",
}
import os
for key, value in DEFAULT_CONFIG.items():
    os.environ[key] = value
    
from es_sfgtools.config.env_config import Environment
# This will read the environment variables set above
Environment.load_working_environment()

from es_sfgtools.workflows.workflow_handler import WorkflowHandler

# GARPOS filtering configuration
FILTER_CONFIG = {
    "pride_residuals": {
        "enabled": False,
        "max_residual_mm": 8,
        "description": "Filter based on GNSS positioning residuals",
    },
    "max_distance_from_center": {
        "enabled": True,
        "max_distance_m": 150.0,
        "description": "Filter shots beyond maximum distance from array center",
    },
    "ping_replies": {
        "enabled": False,
        "min_replies": 1,
        "description": "Filter based on minimum acoustic ping replies",
    },
    "acoustic_filters": {
        "enabled": True,
        "level": "OK",
        "description": "Apply standard acoustic data quality filters",
    },
}

NETWORK = "cascadia-gorda"
CAMPAIGN = "2025_A_1126"
STATIONS = ["NTH1", "NCC1", "NBR1", "GCC1"]

workflow = WorkflowHandler()

for station in STATIONS:
    workflow.set_network_station_campaign(
        network_id=NETWORK,
        station_id=station,
        campaign_id=CAMPAIGN,
    )
    workflow.midprocess_prep_garpos(custom_filters=FILTER_CONFIG,override=False,write_intermediate=False)
