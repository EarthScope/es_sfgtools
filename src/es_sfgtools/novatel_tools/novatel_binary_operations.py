# External imports
import json
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import List
import os 
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.utils.command_line_utils import parse_cli_logs

# Local imports
from .utils import (
    MetadataModel,
    get_metadatav2,
    get_nov0002rnx_binary_path,
    get_nov_000_tile_binary_path,
    get_nov_770_tile_binary_path,
    check_metadata,
    check_metadata_path
)

os.environ["DYLD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib"
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


def novatel_000_2rinex(
    file: Path,
    writedir: Path | None = None,
    site: str = None,
    metadata: dict | MetadataModel | Path | str | None = None,
    **kwargs,
) -> List[Path]:
    """Convert a NovAtel 000 binary file to daily RINEX using nov0002rnx.

    This mirrors ``novatel_ascii_2rinex`` but targets NOV000 binary files and
    wraps the ``nov0002rnx`` Go binary.
    """

    if isinstance(file, str):
        file = Path(file)
    if writedir is None:
        writedir = file.parent
    elif isinstance(writedir, str):
        writedir = Path(writedir)

    if metadata is not None:
        if isinstance(metadata, (str, Path)):
            metadata = check_metadata_path(metadata)
        elif isinstance(metadata, (dict, MetadataModel)):
            metadata = check_metadata(metadata)
        else:
            raise ValueError(
                "Metadata must be a dict, MetadataModel, or path to a JSON file, "
                f"got {type(metadata)}",
            )
    else:
        assert site is not None, "Either metadata or site must be provided"
        assert isinstance(site, str), f"Site must be a string, got {type(site)}"
        assert len(site) == 4, f"Site must be 4 characters long, got {site}"
        metadata = get_metadatav2(site, serialNumber=uuid.uuid4().hex[:10])

    # Locate nov0002rnx binary
    binary_path = get_nov0002rnx_binary_path()

    # If metadata is a dict, prefer its marker_name as site code
    if isinstance(metadata, dict):
        site = metadata.get("marker_name", site or "SIT1")

    logger.loginfo(f"Converting {file} NOV000 binary to RINEX")
    if isinstance(metadata, dict):
        outpath = writedir / f"{site}_metadata.json"
        with open(outpath, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)
        metadata = outpath

    assert isinstance(
        metadata, (str, Path)
    ), "Metadata must be a path to a JSON file at this point"

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
        cmd = [str(binary_path), "-settings", str(metadata), str(file)]
        cmd_str = " ".join(cmd)
        logger.loginfo(f" Running {cmd_str} in {workdir}")
        # Use the argument list so subprocess can locate the binary
        result = subprocess.run(cmd, capture_output=True, cwd=workdir, text=True)

        # If the Go binary aborted, log its output clearly before raising
        if result.returncode != 0:
            print(
                {
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            )
            raise RuntimeError(
                f"nov0002rnx failed with return code {result.returncode}. "
                "See logs for stdout/stderr."
            )

        parse_cli_logs(result, logger)

        rinex_file_paths = list(Path(workdir).rglob(f"*{site}*"))
        logger.loginfo(
            f"Converted {file} to {rinex_file_paths} Daily RINEX files"
        )
        outpaths: List[Path] = []
        for rinex_file in rinex_file_paths:
            logger.logdebug(f" RINEX file: {str(rinex_file)}")
            new_rinex_path = writedir / rinex_file.name
            shutil.move(src=rinex_file, dst=new_rinex_path)
            logger.loginfo(f"Generated Daily RINEX file {str(new_rinex_path)}")
            outpaths.append(new_rinex_path)

    return outpaths

