import platform
import os
import subprocess
from typing import List
import json
import sys

RINEX_BIN_PATH = {
    "darwin_amd64": "src/nova2rnxo/nova2rnxo-darwin-amd64",
    "darwin_arm64": "src/nova2rnxo/nova2rnxo-darwin-arm64",
    "linux_amd64": "src/nova2rnxo/nova2rnxo-linux-amd64",
    "linux_arm64": "src/nova2rnxo/nova2rnxo-linux-arm64"}

def get_metadata(site:str):
    return {
        "markerName": site,
        "markerType": "WATER_CRAFT",
        "observer": "PGF",
        "agency": "Pacific GPS Facility",
        "receiver": {
            "serialNumber": "XXXXXXXXXX",
            "model": "NOV OEMV1",
            "firmware": "4.80",
        },
        "antenna": {
            "serialNumber": "ACC_G5ANT_52AT1",
            "model": "NONE",
            "position": [
                0.000,
                0.000,
                0.000,
            ],  # reference position for site what ref frame?
            "offsetHEN": [0.0, 0.0, 0.0],  # read from lever arms file?
        },
    }

def batch_novatel2rinex(files:List[str], outdir:str, site:str):
    """
    Batch convert Novatel files to RINEX
    """
    # get system platform and architecture
    system = platform.system().lower()
    arch = platform.machine().lower()
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")

    binary_path = RINEX_BIN_PATH[f"{system}_{arch}"]
    assert os.path.exists(binary_path), f"Binary not found: {binary_path}"

    metadata = get_metadata(site)
    metadata_path = os.path.join(outdir, "metadata.json")
    with open(metadata_path, "w") as f:
        json_object = json.dumps(metadata, indent=4)
        f.write(json_object)
    file_date = os.path.splitext(os.path.basename(files[0]))[0].split("_")[1][:8]
    year = file_date[2:4]
    rinex_outfile = os.path.join(outdir, f"{site}_{file_date}_rinex.{year}O")
    

    
    cmd = [
        binary_path,
        "-meta", metadata_path,
        "-out", rinex_outfile,
    ]
    cmd.extend(files)
    subprocess.run(cmd, check=True)
    return rinex_outfile


if __name__ == "__main__":
    sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))    
    from schemas.gnss import PositionDataFrame
    path_cmd = "/Users/franklyndunbar/.PRIDE_PPPAR_BIN"
    os.environ["PATH"] += os.pathsep + path_cmd

    files = [
        "bcnovatel_20180605000000.txt",
        "bcnovatel_20180605010000.txt",
        "bcnovatel_20180605060000.txt",
        "bcnovatel_20180605070000.txt",
        "bcnovatel_20180605051930.txt", 
    ]

    DIR = "/Users/franklyndunbar/Project/SeaFloorGeodesy/seafloor-geodesy/tests/resources/garpos_etl/novatel"
    files = [os.path.join(DIR, f) for f in files]

    rnx_out = batch_novatel2rinex(files, DIR, "IVB1")
    gnss_df = PositionDataFrame.from_rinex(rnx_out)[0]
    gnss_df.to_csv(os.path.join(DIR, "test_gnss.csv"))
