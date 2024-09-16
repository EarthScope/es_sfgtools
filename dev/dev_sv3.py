import sys
from pathlib import Path
import json
import es_sfgtools
import logging
logging.basicConfig(level=logging.INFO,filename="dev.log",filemode="w")
from es_sfgtools.pipeline import DataHandler
from es_sfgtools.modeling.garpos_tools import merge_to_shotdata
import os
import pandas as pd
from collections import defaultdict

data_dir = Path().home() / "Project/SeaFloorGeodesy/Data/NCB1/HR/"
data_files = [str(x) for x in data_dir.glob("*")]
qc_dir = Path().home() / "Project/SeaFloorGeodesy/Data/Sample_QC_UNI1"
qc_files = [str(x) for x in qc_dir.glob("*.pin") if "pin" in str(x)]
catalog_path = Path().home() / "Project/SeaFloorGeodesy/Data/TestSV3"
catalog_path.mkdir(exist_ok=True)
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
# add to path
os.environ["PATH"] += os.pathsep + str(pride_path)
if __name__ == "__main__":

    network = "NCB"
    station = "NCB1"
    survey = "TestSV3"
    dh = DataHandler(data_dir=catalog_path,
                     network=network,
                     station=station,
                     survey=survey,)

    #dh.process_sv3_data()
    dh.add_data_local(qc_files)
    #dh.add_data_local(data_files)
    #dh.process_sv3_data()
    dh.process_qc_data(override=True,show_details=True)
    # dh.process_campaign_data(
    #     override=True,
    #     show_details=True
    # )
    print(dh.get_dtype_counts())

    # survey_entries = dh.query_catalog(
    #     network=network,
    #     station=station,
    #     survey=survey,
    #     type=["gnss", "acoustic", "imu"]
    # )

    # entries_grouped = dh.group_observation_session_data(
    #     data=survey_entries,
    #     timespan="DAY"
    # )

    # processed = {}
    # dtypes = ["gnss", "acoustic", "imu"]
    # for key, value in entries_grouped.items():
    #     merged = {}
    #     for dtype in dtypes:
    #         for x in value[dtype]:
    #             try:
    #                 merged[dtype] = pd.concat([merged.get(dtype,pd.DataFrame()), pd.read_csv(x)])
    #             except:
    #                 pass
    #     if list(merged.keys()) == dtypes:
    #         processed[key] = merged

    # path_dict = defaultdict(lambda: defaultdict(str))
    # for ts,data in processed.items():
    #     for dtype,df in data.items():
    #         ts = ts.replace(":","-").replace(" ","-")
    #         path_name = f"{ts}_{dtype}.csv"
    #         path = dh.garpos_dir / path_name
    #         df.to_csv(path)
    #         path_dict[ts][dtype] = str(path)

    # cat_path = dh.garpos_dir / "prep_NCB1_catalog.json"
    # with open(cat_path, "w") as f:
    #     json.dump(path_dict, f)

    # print(f"Data processed and saved to {str(cat_path)}")
