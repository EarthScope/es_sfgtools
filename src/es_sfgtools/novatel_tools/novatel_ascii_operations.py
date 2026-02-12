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
    get_metadatav2,
    get_nova2rnx_binary_path,
    get_nova2tile_binary_path,
    MetadataModel,
    check_metadata_path,
    check_metadata
)
from es_sfgtools.utils.command_line_utils import parse_cli_logs


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

def novatel_ascii_2rinex(file:Path,writedir:Path=None,site:str="SIT1",metadata:dict|MetadataModel|Path|str=None,modulo_millis:int=0,**kwargs) -> List[Path]:
    """Convert a NovAtel ASCII file to a daily RINEX file using nova2rnxo.
    This function wraps the external `nova2rnxo` binary to convert a NovAtel ASCII
    observation file into a daily RINEX file. Metadata describing the site and
    receiver can be supplied directly or generated automatically from a site code.
    If `metadata` is a mapping or `MetadataModel`, it is validated and written
    to a JSON file in `writedir`. If it is a path (or string path), it is checked
    for existence and validated as JSON metadata. If `metadata` is not provided,
    a metadata JSON is generated using `get_metadatav2` with the given `site`
    code and a random serial number.
    The resulting RINEX file produced by `nova2rnxo` is moved from a temporary
    working directory into `writedir` and its final path is returned.

    Parameters
    ----------
    file : pathlib.Path or str
        Path to the input NovAtel ASCII file to convert.
    writedir : pathlib.Path or str, optional
        Directory where the output RINEX (and metadata JSON, if created) will
        be written. Defaults to the parent directory of `file`.
    site : str, optional
        Four-character site code used when generating metadata automatically,
        required if `metadata` is not provided. Must be exactly 4 characters.
    metadata : dict, MetadataModel, pathlib.Path or str, optional
        Site/receiver metadata. May be:
          * A dict containing metadata fields compatible with `MetadataModel`.
          * A `MetadataModel` instance.
          * A path (or string path) to a JSON file with metadata.
        If provided, it is validated against `MetadataModel`. If provided as a
        dict or `MetadataModel`, a JSON metadata file is written to `writedir`.
    modulo_millis : int, optional
        Decimation modulo in milliseconds (e.g., 1000 for 1 Hz, 15000 for 15s
        intervals). If 0, no decimation is applied. Loss-of-lock indicators
        from skipped epochs are propagated to the next written epoch. Default
        is 0 (no decimation).
    **kwargs
        Currently unused; accepted for future extensibility.

    Returns
    -------
    List[pathlib.Path]
        Paths to the generated daily RINEX files in `writedir`.

    Raises
    ------
    AssertionError
        If `metadata` is None and `site` is missing, not a string, or not
        exactly 4 characters long, or if `metadata` is not a path by the time
        it is passed to `nova2rnxo`.
    ValueError
        If the metadata dictionary, model, or JSON file fails validation
        against `MetadataModel`, or if `metadata` is of an unsupported type.
    FileNotFoundError
        If the provided metadata file path does not exist.
    subprocess.CalledProcessError
        If the `nova2rnxo` subprocess fails.

    Notes
    -----
    This function relies on `get_nova2rnxo_binary_path()` to locate a compatible
    `nova2rnxo` binary for the current system. A temporary working directory
    under `/tmp/` is used for intermediate files and is cleaned up automatically.
    """

    if isinstance(file, str):
        file = Path(file)
    if writedir is None:
        writedir = file.parent
    elif isinstance(writedir, str):
        writedir = Path(writedir)

    if metadata is not None:
        if isinstance(metadata, (str,Path)):
            metadata = check_metadata_path(metadata)

        elif isinstance(metadata, (dict, MetadataModel)):
            metadata = check_metadata(metadata)

        else:
            raise ValueError(f"Metadata must be a dict, MetadataModel, or path to a JSON file, got {type(metadata)}")
    else:
        assert site is not None, "Either metadata or site must be provided"
        assert isinstance(site, str), f"Site must be a string, got {type(site)}"
        assert len(site) == 4, f"Site must be 4 characters long, got {site}"
        metadata = get_metadatav2(site, serialNumber=uuid.uuid4().hex[:10])

    # Get the binary path for nova2rnxo
    # This function will raise an error if the binary is not found
    # or if the system/architecture is not supported
    binary_path = get_nova2rnx_binary_path()

    # Get metadata for the site
    logger.loginfo(
        f"Converting and merging {file} ascii Novatel to RINEX"
    )
    # write metadata to writedir
    if isinstance(metadata,dict):
        outpath = writedir / f"{site}_metadata.json"
        with open(outpath, "w") as f:
            json_object = json.dumps(metadata, indent=4)
            f.write(json_object)
        metadata = outpath
    
    assert isinstance(metadata,(str,Path)), "Metadata must be a path to a JSON file at this point"

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir:
  
        cmd = [str(binary_path), "-settings", str(metadata)]
        if modulo_millis > 0:
            cmd.extend(["-modulo", str(modulo_millis)])
        cmd.append(str(file))
        cmd_str = ' '.join(cmd)
        logger.loginfo(f" Running {cmd_str} in {workdir}")
        result = subprocess.run(cmd, check=True, capture_output=True, cwd=workdir)

        parse_cli_logs(result, logger)

        rinex_file_paths = list(Path(workdir).rglob(f"*{site}*"))
        logger.loginfo(
            f"Converted {file} to {rinex_file_paths} Daily RINEX files"
        )
        outpaths = []
        for rinex_file in rinex_file_paths:
            logger.logdebug(f" RINEX file: {str(rinex_file)}")
            new_rinex_path = writedir / rinex_file.name
            shutil.move(src=rinex_file, dst=new_rinex_path)
            logger.loginfo(f"Generated Daily RINEX file {str(new_rinex_path)}")
            outpaths.append(new_rinex_path)

    return outpaths


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
