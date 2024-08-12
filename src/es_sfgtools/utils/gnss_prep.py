import os
import subprocess
import logging
import sys
from pandera.typing import DataFrame
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tempfile
from schemas import PositionDataFrame
from typing import List,Union

logger = logging.getLogger(os.path.basename(__file__))

def meta_data(site,position=[0.0,0.0,0.0],offsetHEN=[0.0,0.0,0.0]):

    out = {
    "markerName": site,
    "markerType": "GEODETIC",
    "observer": "EarthScope",
    "agency": "EarthScope",
    "receiver": {"serialNumber": "123456789", "model": "NOV", "firmware": "0.0.0"},
    "antenna": {
        "serialNumber": "987654321",
        "model": "TRM59800.00 SCIT",
        "position": position,
        "offsetHEN":offsetHEN,
        },
    }
    return out


def get_kin_files(directory: str):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if "kin_" in file:
                yield os.path.join(root, file)


def rinex_to_gnss(rinex_files:Union[str,List[str]]) -> List[str]:
    """
    Convert a RINEX file to a position file
    """
    logger.info(f"Converting RINEX file {rinex_file} to 'PositionDataFrame' format")
    if not isinstance(rinex_files, list):
        rinex_files = [rinex_files]
    
    out = []
    working_dir = "/tmp/"
    with tempfile.TemporaryDirectory(dir=working_dir) as working_dir:
        dfs = []
        for rinex_file in rinex_files:
            if not os.path.exists(rinex_file):
                logger.error(f"RINEX file {rinex_file} not found")
                return None
            result = subprocess.run(
                ["pdp3", "-m", "K", "--site", "IVB1", rinex_file],
                capture_output=True,
                cwd=working_dir,
            )

            if result.stdout:
                logger.info(result.stdout.decode('utf-8'))
                continue

            if result.stderr:
                logger.error(result.stderr)
        for root,_,files in os.walk(working_dir):
            for file in files:
                if "kin_" in file:
                    out.append(os.path.join(root,file))
        return out


     


if __name__ == "__main__":
    rinex_file = "/Users/franklyndunbar/Project/SeaFloorGeodesy/seafloor-geodesy/tests/resources/garpos_etl/rinex/bcnovatel_20180605000000.18O"
    df = rinex_to_gnss(rinex_file)
    print(df)
