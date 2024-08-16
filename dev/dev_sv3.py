import sys
from pathlib import Path
import json
import es_sfgtools

from es_sfgtools.processing.schemas.files import QCPinFile,NovatelPinFile
from es_sfgtools.processing.functions.acoustic_functions import qcpin_to_acousticdf
from es_sfgtools.processing.functions.gnss_functions import qcpin_to_novatelpin,novatelpin_to_rinex
from es_sfgtools.processing.functions.imu_functions import qcpin_to_imudf
dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/")

fp = dir/"329653-003_20240806_042015_000296_SCRIPPS.pin"

qcnov_path = dir/"qc_nov.text"

qcpin = QCPinFile(location=fp)

df = qcpin_to_acousticdf(qcpin)

print(df.head())

imu_df = qcpin_to_imudf(qcpin)

print(imu_df.head())
#novatel = qcpin_to_novatelpin(qcpin,outpath=qcnov_path)
# novatel = NovatelPinFile(location=qcnov_path)
# rinex = novatelpin_to_rinex(novatel,site="SEM1",year=2024)