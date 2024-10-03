import pandas as pd
import sys
from pathlib import Path
import os
import logging
logging.basicConfig(level=logging.INFO,filename="dev.log",filemode="w")
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))

from src.es_sfgtools.processing.assets.file_schemas import NovatelFile,SonardyneFile,QCPinFile,DFPO00RawFile
from src.es_sfgtools.processing.operations import sv2_ops,sv3_ops

data_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SEM1Test")

novatel = data_dir / "bcnovatel_20180514030000.txt"
sonardyne = data_dir / "bcsonardyne_20180514030000.txt"

novatel_file = NovatelFile(local_path=novatel)
sonardyne_file = SonardyneFile(local_path=sonardyne)
dfofile = DFPO00RawFile(
    local_path=Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NFL1/HR/329653_001_20230604_203732_00260_DFOP00.raw"
    )
)
qcfile = QCPinFile(local_path=Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Sample_QC_UNI1/329653-003_20240802_000528_000292_SCRIPPS.pin"
))
acousticdf = sv2_ops.sonardyne_to_acousticdf(sonardyne_file)
positiondf = sv2_ops.novatel_to_positiondf(novatel_file)
shotdf = sv2_ops.dev_merge_to_shotdata(position=positiondf,acoustic=acousticdf)
print(shotdf)
shotdf_sv3 = sv3_ops.dev_dfop00_to_shotdata(dfofile)
print(shotdf_sv3)

qcshotdf = sv3_ops.dev_qcpin_to_shotdata(qcfile)
print(qcshotdf)