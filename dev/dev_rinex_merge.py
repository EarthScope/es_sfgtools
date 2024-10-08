import es_sfgtools
from es_sfgtools.processing.pipeline import DataHandler

from pathlib import Path

dh_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3")
network = "NCB"
station = "NCB1"
survey = "TestSV3"
dh = DataHandler(network=network, station=station, survey=survey, data_dir=dh_dir)
dh.add_data_directory(dh_dir)
print(dh.get_dtype_counts())
dh.pipeline_sv3(override=True,show_details=True)