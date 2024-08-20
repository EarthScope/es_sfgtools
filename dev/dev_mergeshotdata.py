import pandas as pd
import sys
from pathlib import Path
import os
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))

from src.es_sfgtools.modeling.garpos_tools.functions import merge_to_shotdata

imu_df = pd.read_csv("tests/resources/garpos_etl/inspva_from_novatel.csv")
acoustic_df = pd.read_csv("tests/resources/garpos_etl/acoustic_from_sondardyne.csv")
gnss_df = pd.read_csv("tests/resources/garpos_etl/test_gnss.csv")

shotdata = merge_to_shotdata(acoustic_df,imu_df, gnss_df)
