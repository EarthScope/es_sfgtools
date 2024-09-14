import pandas as pd
import sys
from pathlib import Path
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))
from src.es_sfgtools.pipeline import DataHandler

if __name__ == "__main__":
    pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
    # add to path
    os.environ["PATH"] += os.pathsep + str(pride_path)
    qc_data_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Sample_QC_UNI1/")

    data_handler = DataHandler(qc_data_dir.parent,
                               network="QC",
                               station="QC1",
                               survey="QC"
                               )

    pin_files = qc_data_dir.glob("*.pin")


    data_handler.add_data_local(
        local_filepaths=pin_files
    )

    data_handler.process_qc_data(update_timestamp=True,
                                 override=False,show_details=False)

    # qc_entries = data_handler.get_observation_session_data(
    #     network=network,
    #     station=station,
    #     survey=survey
    # )

    # qc_hourly = data_handler.group_observation_session_data(qc_entries,timespan='HOUR')

