import logging
logging.basicConfig(level=logging.DEBUG,filename="dev_rinex_merge.log",filemode="w")
import es_sfgtools
import os
from es_sfgtools.processing.pipeline import DataHandler
from es_sfgtools.processing.assets.file_schemas import AssetType,AssetEntry
from es_sfgtools.processing.operations.gnss_ops import novatel_to_rinex,rinex_to_kin,kin_to_gnssdf
from pathlib import Path
from typing import List
if __name__ == "__main__":
    pride_dir = "/Users/franklyndunbar/.PRIDE_PPPAR_BIN"
    os.environ["PATH"] += os.pathsep + pride_dir
    dh_dir_sv2 = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestGage")
    dh_dir_sv3 = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NCL1/"
    )
    network = "NCB"
    station = "NCB1"
    survey = "2023"

    dh = DataHandler(network=network, station=station, survey=survey, data_dir=dh_dir_sv3)
    dh.add_data_directory(dh_dir_sv3)
    print(dh.get_dtype_counts())
    
    rinex_entries = dh.process_novatel(show_details=True)
    kin_entries = dh.process_rinex(show_details=True)
    
    gnss_entries = dh.process_kin(show_details=True)


    print(gnss_entries[0].model_dump())