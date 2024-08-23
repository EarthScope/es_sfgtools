import es_sfgtools
from pathlib import Path
import os
import time 
import logging

pride_dir = "/Users/franklyndunbar/.PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + pride_dir
gage_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestGage")

logging.basicConfig(level=logging.INFO,filename=str(gage_dir / "runtime.log"),filemode="w")
data_handler = es_sfgtools.DataHandler(gage_dir)

network = "aleutian"
site = "IVB1"
survey = "2018_A_SFG1"

file_list = es_sfgtools.utils.list_survey_files(network, site, survey)

data_handler.add_campaign_data(
    network=network,
    station=site,
    survey=survey,
    remote_filepaths=file_list
)
data_handler.download_campaign_data(network=network, station=site, survey=survey,show_details=True)

start = time.time()
data_handler.process_campaign_data(network=network, station=site, survey=survey,show_details=True)
end = time.time()
response = f"Time to process: {end-start}"
logging.info(response)
print(f"Time to process: {end-start}")