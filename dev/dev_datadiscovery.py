from pathlib import Path
from es_sfgtools.pipeline import DataHandler

directory = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/NCB1")
dh = DataHandler(network="NCB", station="NCB1", survey="TestSV3",data_dir=directory)
dh.add_data_directory(directory)

print(dh.get_dtype_counts())