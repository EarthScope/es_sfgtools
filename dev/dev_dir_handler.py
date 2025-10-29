from pathlib import Path
from cloudpathlib import S3Path

from es_sfgtools.data_mgmt.directorymgmt import (
    DirectoryHandler,
    NetworkDir,
    StationDir,
    CampaignDir,
    SurveyDir,
    GARPOSSurveyDir,
    TileDBDir,
)

local_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain")

test_dir_handler = DirectoryHandler.load_from_path(local_dir)

# print(test_dir_handler)

s3_bucket = S3Path(
    "s3://seafloor-public-bucket-bucket83908e77-gprctmuztrim"
)

test_dir_handler_s3 = DirectoryHandler.load_from_path(s3_bucket)

print(test_dir_handler_s3)
