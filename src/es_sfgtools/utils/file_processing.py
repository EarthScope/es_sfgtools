from typing import List, Union
import logging
import os
import tempfile
import platform
import json
import shutil
import subprocess

logger = logging.getLogger(__name__)
pride_path = "/home/ec2-user/.PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + pride_path

nova2rinex_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),"nova2rnxo/")

RINEX_BIN_PATH = {
    "darwin_amd64": os.path.join(nova2rinex_dir, "nova2rnxo-darwin-amd64"),
    "darwin_arm64": os.path.join(nova2rinex_dir, "nova2rnxo-darwin-arm64"),
    "linux_amd64": os.path.join(nova2rinex_dir, "nova2rnxo-linux-amd64"),
    "linux_arm64": os.path.join(nova2rinex_dir, "nova2rnxo-linux-arm64"),
}

RINEX_BIN_PATH_BINARY = {
    "darwin_amd64": os.path.join(nova2rinex_dir, "novb2rnxo-darwin-amd64"),
    "darwin_arm64": os.path.join(nova2rinex_dir, "novb2rnxo-darwin-arm64"),
    "linux_amd64": os.path.join(nova2rinex_dir, "novb2rnxo-linux-amd64"),
    "linux_arm64": os.path.join(nova2rinex_dir, "novb2rnxo-linux-arm64"),
}


def get_metadata(site: str):
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


def novatel2rinex(file:str, outdir: str, site: str,binary=False,year:str=None) -> str:
    """
    Batch convert Novatel files to RINEX
    """
    # get system platform and architecture
    system = platform.system().lower()
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    if system not in ["darwin", "linux"]:
        raise ValueError(f"Unsupported platform: {system}")
    if arch not in ["amd64", "arm64"]:
        raise ValueError(f"Unsupported architecture: {arch}")
    if not binary:
        binary_path = RINEX_BIN_PATH[f"{system}_{arch}"]
    else:
        binary_path = RINEX_BIN_PATH_BINARY[f"{system}_{arch}"]
    assert os.path.exists(binary_path), f"Binary not found: {binary_path}"
    
    metadata = get_metadata(site)

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        metadata_path = os.path.join(workdir, "metadata.json")
        with open(metadata_path, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)
        file_date = os.path.splitext(os.path.basename(file))[0].split("_")[1]
        if year is None:
            year = file_date[2:4]
        rinex_outfile = os.path.join(outdir, f"{site}_{file_date}_rinex.{year}O")
        file_tmp_dest = shutil.copy(file, os.path.join(workdir,os.path.basename(file)))
        

        cmd = [
            binary_path,
            "-meta",
            metadata_path,
            "-out",
            rinex_outfile,
        ]
        cmd.extend([file_tmp_dest])
        subprocess.run(cmd, check=True)
        logger.info(f"Converted Novatel files to RINEX: {rinex_outfile}")
    return rinex_outfile


def rinex_to_kin(rinex_file: str,outdir:str,site:str="IVB1") -> List[str]:
    """
    Convert a RINEX file to a position file
    """
    logger.info(f"Converting RINEX file {rinex_file} to kin file")


    out = []

    with tempfile.TemporaryDirectory(dir="/tmp/") as tmpoutdir:
    
        if not os.path.exists(rinex_file):
            logger.error(f"RINEX file {rinex_file} not found")
            return None
        result = subprocess.run(
            ["pdp3", "-m", "K", "--site", site, rinex_file],
            capture_output=True,
            cwd=tmpoutdir,
        )

        if result.stdout:
            logger.info(result.stdout.decode("utf-8"))
            

        if result.stderr:
            logger.error(result.stderr)

        for root, _, files in os.walk(tmpoutdir):
            for file in files:
                if "kin_" in file:
                    source_path = os.path.join(root, file)
                    dest_path = os.path.join(outdir, os.path.basename(source_path))

                    shutil.copy(source_path, dest_path)
                    return dest_path
   
