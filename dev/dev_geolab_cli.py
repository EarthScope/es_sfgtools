DEFAULT_CONFIG = {
    # GeoLab environment settings
    "WORKING_ENVIRONMENT": "GEOLAB",
    "MAIN_DIRECTORY_GEOLAB": "/home/jovyan/sfgmain",
    "S3_SYNC_BUCKET": "seafloor-public-bucket-bucket83908e77-gprctmuztrim",
   }
import os
from pathlib import Path

for key, value in DEFAULT_CONFIG.items():
    os.environ[key] = value

from es_sfgtools.config.env_config import Environment

# This will read the environment variables set above
Environment.load_working_environment()
Environment.load_aws_credentials()

import sys
sys.path.append("/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/")
from app.src.commands import run_manifest,PipelineManifest
if __name__ == "__main__":

    manifest_path = Path("dev/dev_cli.yaml")
    manifest = PipelineManifest.from_yaml(manifest_path)
    run_manifest(manifest_object=manifest)
