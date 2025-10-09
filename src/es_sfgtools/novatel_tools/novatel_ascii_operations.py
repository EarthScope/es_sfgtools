# External imports
import json
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import List

import numpy as np

from ..logging import ProcessLogger as logger

# Local imports
from .utils import (
    get_metadata,
    get_nova2rnxo_binary_path,
    get_nova2tile_binary_path,
    parse_cli_logs,
)


def novatel_ascii_2tile(files: List[str], gnss_obs_tdb: Path, n_procs: int = 10) -> None:
    """
    This function is a python wrapper for the nova2tile golang binary.
    Given a list of novatel ascii files, get all the rangea logs and add them to a single tdb array

    Args:
        files (List[str]):  List of asset entries to process
        gnss_obs_tdb (Path): Path to the gnss_obs tiledb array
        n_procs (int, optional): number of processes to use. Defaults to 10.
    """

    binary_path = get_nova2tile_binary_path()

    cmd = [str(binary_path), "-tdb", str(gnss_obs_tdb), "-procs", str(n_procs)]
    for file in files:
        cmd.append(str(file))

    logger.logdebug(f" Running {cmd}")
    logger.loginfo(f"Running NOVA2TILE on {len(files)} files")
    result = subprocess.run(cmd)

    parse_cli_logs(result, logger)

def novatel_ascii_2rinex(file:str,writedir:Path,site:str) -> Path:

    # Get the binary path for nova2rnxo
    # This function will raise an error if the binary is not found
    # or if the system/architecture is not supported
    binary_path = get_nova2rnxo_binary_path()

    # Get metadata for the site
    metadata = get_metadata(site, serialNumber=uuid.uuid4().hex[:10])
    logger.loginfo(
        f"Converting and merging {file} ascii Novatel to RINEX"
    )
    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        metadata_path = Path(workdir) / "metadata.json"
        with open(metadata_path, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)

        cmd = [str(binary_path), "-meta", str(metadata_path),file]
        result = subprocess.run(cmd, check=True, capture_output=True, cwd=workdir)

        parse_cli_logs(result, logger)

        rinex_file_path = list(Path(workdir).rglob(f"*{site}*"))[0]
        logger.loginfo(
            f"Converted {file} to {rinex_file_path} Daily RINEX file"
        )

        new_rinex_path = writedir / rinex_file_path.name
        shutil.move(src=rinex_file_path, dst=new_rinex_path)
        logger.loginfo(f"Generated Daily RINEX file {str(new_rinex_path)}")

    return new_rinex_path


def qcpin_to_novatelpin(source: str|Path, writedir: Path) -> Path:
    """ Convert a QCPIN JSON file to a Novatel PIN text file.
    Args:
        source (str|Path): Path to the QCPIN JSON file.
        writedir (Path): Directory where the Novatel PIN text file will be saved.
    Returns:
        Path: Path to the generated Novatel PIN text file.
    """
    with open(source) as file:
        pin_data = json.load(file)

    range_headers = []
    time_stamps = []

    for data in pin_data.values():
        range_header = data.get("observations").get("NOV_RANGE")
        time_header = data.get("observations").get("NOV_INS").get("time").get("common")
        range_headers.append(range_header)
        time_stamps.append(time_header)

    time_sorted = np.argsort(time_stamps)
    range_headers = [range_headers[i] for i in time_sorted]

    file_path = writedir / (str(source.id) + "_novpin.txt")
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as temp_file:
        for header in range_headers:
            temp_file.write(header)
            temp_file.write("\n")
        temp_file.seek(0)
        shutil.copy(temp_file.name, file_path)
        novatel_pin = Path(file_path)

    return novatel_pin
