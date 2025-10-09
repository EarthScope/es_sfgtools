# External imports
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List

# Local imports
from es_sfgtools.utils.command_line_utils import parse_cli_logs

from ..logging import ProcessLogger as logger
from .utils import get_tile2rinex_binary_path


def tile2rinex(
    gnss_obs_tdb: Path,
    settings: Path,
    writedir: Path,
    time_interval: int = 1,
    processing_year: int = 0,
) -> List[Path]:
    """
    Converts GNSS observation tileDB data to RINEX format using the TILE2RINEX binary.

    Args:
        gnss_obs_tdb (Path): Path to the GNSS tiledb array.
        settings (Path): Path to the RINEX settings file.
        writedir (Path): Directory where the generated RINEX files will be written.
        time_interval (int, optional): Time interval (hours) of GNSS epochs loaded into memory from the tiledb array found at gnss_obs_tdb.
        processing_year (int, optional): Year of GNSS observations used to generate RINEX files from the tiledb array found at gnss_obs_tdb. Defaults to 0.

    Returns:
        List[Path]: A list of Paths representing the generated RINEX files.

    Raises:
        subprocess.CalledProcessError: If the TILE2RINEX command fails during execution.

    Notes:
        - The function uses a temporary directory to ensure only newly created RINEX files are returned.
        - Logs are captured from the TILE2RINEX binary's stdout and stderr for debugging and informational purposes.
        - The generated RINEX files are moved to the specified `writedir` and metadata is extracted for each file.
        - time_interval is used to control the tradeoff between memory usage and speed. The larger the time_interval, the more memory is used, but the faster the generation.
        - processing_year is used to prevent RINEX generation from years outside of a given campaign. When set to 0, all found observations are used to generate daily RINEX files.
    """

    binary_path = get_tile2rinex_binary_path()

    os.environ["LD_LIBRARY_PATH"] = os.environ["CONDA_PREFIX"] + "/lib"
    os.environ["DYLD_LIBRARY_PATH"] = os.environ["CONDA_PREFIX"] + "/lib"
    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        # Use a temp dir so as to only return newly created rinex files
        cmd = [
            str(binary_path),
            "-tdb",
            str(gnss_obs_tdb),
            "-settings",
            str(settings),
            "-timeint",
            str(time_interval),
            "-year",
            str(processing_year),
        ]
        logger.loginfo(f" Running {cmd}")
        result = subprocess.run(cmd, cwd=workdir,capture_output=True)

        parse_cli_logs(result, logger)

        rinex_files = list(Path(workdir).rglob("*"))
        rinex_assets = []
        for rinex_file_path in rinex_files:
            new_rinex_path = writedir / rinex_file_path.name
            shutil.move(src=rinex_file_path, dst=new_rinex_path)
            rinex_assets.append(new_rinex_path)
            logger.loginfo(f"Generated Daily RINEX file {str(new_rinex_path)}")

    return rinex_assets
