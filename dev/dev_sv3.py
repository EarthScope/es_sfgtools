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

data_dir = Path().home() / "Project/SeaFloorGeodesy/Data/NCB1/HR/"
data_files = [str(x) for x in data_dir.glob("*")]
catalog_path = Path().home() / "Project/SeaFloorGeodesy/Data/TestSV3"
catalog_path.mkdir(exist_ok=True)
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
# add to path
os.environ["PATH"] += os.pathsep + str(pride_path)
if __name__ == "__main__":
    dh = DataHandler(catalog_path)

    network = "NCB"
    station = "NCB1"
    survey = "TestSV3"

    # dh.add_data_local(
    #     network=network,
    #     station=station,
    #     survey=survey,
    #     local_filepaths=data_files,
    #     discover_file_type=True
    # )

    # dh.process_campaign_data(
    #     network=network,
    #     station=station,
    #     survey=survey,
    #     override=False,
    #     show_details=True
    # )

    # dh.process_target(
    #     network=network,
    #     station=station,
    #     survey=survey,
    #     parent="kin",
    #     child="gnss"
    # )

    entries = dh.catalog_data[
        dh.catalog_data.type.isin(["gnss", "acoustic", "imu"])
    ]
    entries_grouped:dict = dh.group_observation_session_data(
        data=entries,
        timespan="DAY"
    )
    processed = {}
    dtypes = ["gnss", "acoustic", "imu"]
    for key, value in entries_grouped.items():
        merged = {}
        for dtype in dtypes:
            merged[dtype] = pd.DataFrame()
            for x in value[dtype]:
                try:
                    merged[dtype] = pd.concat(
                        [merged[dtype], pd.read_csv(x)]
                    )
            
                except:
                    pass
        for k in dtypes:
            if merged[k].shape[0] == 0:
                del merged[k]
        if list(merged.keys()) == dtypes:
            processed[key] = merged

print(processed)

for ts,data in processed.items():
    for dtype,df in data.items():
        path_name = f"{ts}_{dtype}.csv"
        path = dh.garpos_dir / path_name
        df.to_csv(path)
        processed[ts][dtype] = str(path)
with open(dh.garpos_dir / "prep_NCB1.json", "w") as f:
    json.dump(processed, f)