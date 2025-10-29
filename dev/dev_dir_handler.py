from pathlib import Path
from cloudpathlib import S3Path

from es_sfgtools.data_mgmt.directorymgmt import DirectoryHandler
from es_sfgtools.data_mgmt.directorymgmt.schemas import NetworkDir,StationDir,CampaignDir,SurveyDir,GARPOSSurveyDir,TileDBDir

local_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain")

test_dir_handler = DirectoryHandler.load_from_path(local_dir)

print(test_dir_handler)

