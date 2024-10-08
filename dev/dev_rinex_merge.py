import logging
logging.basicConfig(level=logging.DEBUG,filename="dev_rinex_merge.log")
import es_sfgtools
from es_sfgtools.processing.pipeline import DataHandler
from es_sfgtools.processing.assets.file_schemas import AssetType,AssetEntry
from pathlib import Path
if __name__ == "__main__":
    dh_dir_sv2 = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestGage")
    dh_dir_sv3 = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NCL1/"
    )
    network = "NCB"
    station = "NCB1"
    survey = "TestSV3"

    dh = DataHandler(network=network, station=station, survey=survey, data_dir=dh_dir_sv3)
    dh.add_data_directory(dh_dir_sv3)
    print(dh.get_dtype_counts())
    dh.query_catalog(
        f"DELETE FROM assets WHERE network='NCB' AND station='NCB1' AND survey='TestSV3' AND type='{AssetType.NOVATELPIN.value}'")
    dh.pipeline_sv3(override=True,show_details=True)
