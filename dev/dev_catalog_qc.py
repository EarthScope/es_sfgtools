import pandas as pd
import sys
from pathlib import Path
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))
from src.es_sfgtools.pipeline import DataHandler

if __name__ == "__main__":
    qc_data_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Sample_QC_UNI1/")

    data_handler = DataHandler(qc_data_dir.parent)

    pin_files = qc_data_dir.glob("*.pin")

    network="QC"
    station="QC"
    survey="QC"

    data_handler.add_qc_data(
        network=network,
        station=station,
        survey=survey,
        local_filepaths=pin_files
    )

    data_handler.process_qc_data(network=network, station=station, survey=survey,update_timestamp=True)
    
    qc_entries = data_handler.get_observation_session_data(
        network=network,
        station=station,
        survey=survey
    )

    qc_hourly = data_handler.group_observation_session_data(qc_entries,timespan='HOUR')

