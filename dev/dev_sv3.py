from pathlib import Path
import logging
logging.basicConfig(level=logging.WARNING,filename="dev.log",filemode="w")
from es_sfgtools.processing.pipeline.temp import DataHandler
import os

data_dir = (
    Path().home()
    / "Project/SeaFloorGeodesy/Data/Cascadia2023/NFL1"
)
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
catalog_path = data_dir/"catalog.sqlite"
# add to path
os.environ["PATH"] += os.pathsep + str(pride_path)
if __name__ == "__main__":

    network = "Cascadia"
    station = "NFL1"
    survey = "2023"
    dh = DataHandler(data_dir=data_dir,
                     network=network,
                     station=station,
                     survey=survey,)

    dh.add_data_directory(data_dir,show_details=True)
    print(dh.get_dtype_counts())
    dh.pipeline_sv3(show_details=True,plot=True)
  
