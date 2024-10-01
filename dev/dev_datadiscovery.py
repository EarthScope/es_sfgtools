from pathlib import Path
from es_sfgtools.processing.pipeline import DataHandler
from es_sfgtools.processing.operations import sv3_ops
directory = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/NCB1")
dh = DataHandler(network="NCB", station="NCB1", survey="TestSV3",data_dir=directory)
dh.add_data_directory(directory)

print(dh.get_dtype_counts())

query = "SELECT * FROM assets WHERE network='NCB' AND station='NCB1' AND survey='TestSV3' AND type='DFOP00"

entries = dh.query_catalog(query)
