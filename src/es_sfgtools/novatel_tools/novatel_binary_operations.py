# External imports
import subprocess
from pathlib import Path
from typing import List

from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.utils.command_line_utils import parse_cli_logs

# Local imports
from .utils import (
    get_nov_000_tile_binary_path,
    get_nov_770_tile_binary_path,
)


def novatel_770_2tile(files: List[str], gnss_obs_tdb: Path, n_procs: int = 10) -> None:
    """Given a list of novatel 770 binary files, get all the range logs and add them to a single tdb array

    Args:
        files (List[AssetEntry]):  List of asset entries to process
        gnss_obs_tdb (Path): Path to the gnss_obs tiledb array
        n_procs (int, optional): _description_. Defaults to 10.
    """

    # Generate the command to run the novb2tile golang binary
    binary_path = get_nov_770_tile_binary_path()
    cmd = [str(binary_path), "-tdb", str(gnss_obs_tdb), "-procs", str(n_procs)]
    logger.logdebug(f" Running {cmd}")
    for file in files:
        cmd.append(str(file))
    logger.loginfo(f"Running NOVB2TILE with {' '.join(cmd)}")

    # Run the command
    result = subprocess.run(cmd,capture_output=True)

    # Parse the output and log messages
    parse_cli_logs(result,logger)

def novatel_000_2tile(files: List[str], gnss_obs_tdb: Path, position_tdb:Path, n_procs: int = 10) -> None:
    """Given a list of novatel 000 binary files, get all the range logs and add them to a single tdb array

    Args:
        files (List[AssetEntry]):  List of asset entries to process
        gnss_obs_tdb (Path): Path to the gnss_obs tiledb array
        n_procs (int, optional): _description_. Defaults to 10.
    """

    # Generate the command to run the nov0002tile golang binary
    binary_path = get_nov_000_tile_binary_path()
    cmd = [str(binary_path), "-tdb", str(gnss_obs_tdb), "-tdbpos", str(position_tdb), "-procs", str(n_procs)]
    logger.logdebug(f" Running {cmd}")
    for file in files:
        cmd.append(str(file))
    logger.loginfo(f"Running NOV0002TILE with {' '.join(cmd)}")

    # Run the command
    result = subprocess.run(cmd)

    # Parse the output and log messages
    parse_cli_logs(result,logger)