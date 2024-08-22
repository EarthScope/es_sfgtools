import es_sfgtools
from pathlib import Path

gage_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestGage")
data_handler = es_sfgtools.DataHandler(gage_dir)

network = "aleutian"
site = "IVB1"
survey = "2018_A_SFG1"

# file_list = es_sfgtools.utils.list_survey_files(network, site, survey)

# data_handler.add_campaign_data(
#     network=network,
#     station=site,
#     survey=survey,
#     remote_filepaths=file_list
# )
# data_handler.download_campaign_data(network=network, station=site, survey=survey,show_details=True)

data_handler.process_campaign_data(network=network, station=site, survey=survey,show_details=True)