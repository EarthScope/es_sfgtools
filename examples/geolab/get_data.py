"""
Seafloor Geodesy Data Processing Demo - GeoLab Environment

This demo shows the steps to get pre-processed data for modeling in the geolab environment.

"""

import os
from pathlib import Path
from typing import  Dict, Any

# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_CONFIG = {
    # GeoLab environment settings
    'WORKING_ENVIRONMENT': 'GEOLAB',
    'MAIN_DIRECTORY_GEOLAB': '/Volumes/DunbarSSD/Project/SeafloorGeodesy/GEOLABDemo',
    'S3_SYNC_BUCKET': 'seafloor-public-bucket-bucket83908e77-gprctmuztrim',
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def setup_geolab_environment(config: Dict[str, Any]) -> None:
    """
    Configure environment variables for GeoLab processing.
    
    This function sets up all required environment variables for seafloor
    geodesy processing in the GeoLab environment.
    
    Args:
        main_directory: Path to the main GeoLab data directory
        s3_bucket: S3 bucket name for data synchronization
        
    Raises:
        ValueError: If required directories don't exist
    """

    # Validate main directory exists
    main_path = Path(config['MAIN_DIRECTORY_GEOLAB'])
    if not main_path.exists():
        raise ValueError(f"Main directory does not exist: {config['MAIN_DIRECTORY_GEOLAB']}")

    # Configure dynamic library path for conda environment
    conda_prefix = os.environ.get("CONDA_PREFIX", "")
    if conda_prefix:
        current_dyld = os.environ.get("DYLD_LIBRARY_PATH", "")
        os.environ["DYLD_LIBRARY_PATH"] = f"{conda_prefix}/lib:{current_dyld}"
    

    
    for var_name, var_value in config.items():
        os.environ[var_name] = var_value


# =============================================================================
# MAIN PROCESSING FUNCTIONS
# =============================================================================

setup_geolab_environment(DEFAULT_CONFIG)

from es_sfgtools.workflows.workflow_handler import WorkflowHandler

# GARPOS filtering configuration
FILTER_CONFIG = {
    "pride_residuals": {
        "enabled": False,
        "max_residual_mm": 8,
        "description": "Filter based on GNSS positioning residuals",
    },
    "max_distance_from_center": {
        "enabled": False,
        "max_distance_m": 500.0,
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
    workflow.midprocess_parse_surveys()
    workflow.midprocess_prep_garpos(custom_filters=FILTER_CONFIG)
