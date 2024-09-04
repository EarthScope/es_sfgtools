import pandas as pd
import sys
from pathlib import Path
import os
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))

# from src.es_sfgtools.modeling.garpos_tools.functions import merge_to_shotdata

# imu_df = pd.read_csv("tests/resources/garpos_etl/inspva_from_novatel.csv")
# acoustic_df = pd.read_csv("tests/resources/garpos_etl/acoustic_from_sondardyne.csv")
# gnss_df = pd.read_csv("tests/resources/garpos_etl/test_gnss.csv")

# shotdata = merge_to_shotdata(acoustic_df,imu_df, gnss_df)

from src.es_sfgtools.processing.schemas import DFPO00RawFile
from src.es_sfgtools.processing.functions import dfpo00_to_acousticdf

if __name__ == "__main__":
    dfp_path = "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/NCB1/HR/329653_002_20210906_141932_00051_DFOP00.raw"
    dfp = DFPO00RawFile(location=dfp_path)
    acoustic_df: pd.DataFrame = dfpo00_to_acousticdf(dfp)
    print(acoustic_df.head())
