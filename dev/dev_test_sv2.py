import pandas as pd
import sys
from pathlib import Path
import os
import logging
logging.basicConfig(level=logging.INFO,filename="dev.log",filemode="w")
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))

from src.es_sfgtools.processing.assets.file_schemas import NovatelFile,SonardyneFile
from src.es_sfgtools.processing.operations import sv2_ops

data_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SEM1Test")

novatel = data_dir / "bcnovatel_20180514030000.txt"
sonardyne = data_dir / "bcsonardyne_20180514030000.txt"

novatel_file = NovatelFile(local_path=novatel)
sonardyne_file = SonardyneFile(local_path=sonardyne)

acousticdf = sv2_ops.sonardyne_to_acousticdf(sonardyne_file)
positiondf = sv2_ops.novatel_to_positiondf(novatel_file)
shotdf = sv2_ops.dev_merge_to_shotdata(position=positiondf,acoustic=acousticdf)
print(shotdf)