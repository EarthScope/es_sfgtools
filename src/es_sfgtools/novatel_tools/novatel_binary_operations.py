# External imports
from collections import defaultdict
import json
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Dict, List, Optional
import os
import warnings 
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.utils.command_line_utils import parse_cli_logs

# Local imports
from .utils import (
    MetadataModel,
    get_metadatav2,
    get_nov0002rnx_binary_path,
    get_novb2rnxo_binary_path,
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


def _novatel_2rinex_wrapper(
    files: List[Path] | List[str],
    writedir: Path,
    metadata: dict | Path | str,
    binary_path: Path,
    modulo_millis: int = 0,
) -> List[Path]:
    """Internal helper to call a novatel-to-RINEX Go binary on a batch of files.

    Parameters
    ----------
    files
        Input NOV000 / NOV770 files to convert.
    writedir
        Directory where output RINEX files will be moved.
    metadata
        Metadata dictionary or path to a JSON metadata file. This is written
        to the temporary working directory for the Go binary to consume.
    binary_path
        Path to the nov0002rnx / novb2rnxo binary.
    modulo_millis
        Decimation modulo in milliseconds (e.g., 1000 for 1 Hz, 15000 for 15s
        intervals). If 0, no decimation is applied.
    """

    # Normalise the input file list to Paths for logging and command building
    if not files:
        raise ValueError("No input files provided to _novatel_2rinex_wrapper")

    file_paths: List[Path] = [Path(f) for f in files]

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir_str:
        workdir = Path(workdir_str)

        # Always end up with a metadata dict so we can consistently get marker_name
        if isinstance(metadata, dict):
            metadata_dict = metadata
        elif isinstance(metadata, (Path, str)):
            metadata_path = Path(metadata)
            if not metadata_path.exists():
                raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
            if not metadata_path.is_file():
                raise ValueError(f"Metadata path is not a file: {metadata_path}")
            if (suffix := metadata_path.suffix.lower()) != ".json":
                raise ValueError(
                    f"Metadata file must be a JSON file, got {suffix}"
                )
            with open(metadata_path) as f:
                metadata_dict = json.load(f)
        else:
            raise ValueError(
                f"Metadata must be a dict or path to JSON file, got {type(metadata)}"
            )

        site = metadata_dict.get("marker_name", "SIT1")

        # Write a metadata JSON into the temporary working directory that the
        # Go binary will read.
        metadata_tmp_path = workdir / f"{site}_metadata.json"
        with open(metadata_tmp_path, "w") as f:
            json.dump(metadata_dict, f, indent=4)

        cmd = [
            str(binary_path),
            "-settings",
            str(metadata_tmp_path),
        ]
        if modulo_millis > 0:
            cmd.extend(["-modulo", str(modulo_millis)])
        cmd.extend([str(p) for p in file_paths])
        cmd_str = " ".join(cmd)
        logger.loginfo(f" Running {cmd_str} in {workdir}")
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
                f"{binary_path.name} failed with return code {result.returncode}. "
                "See logs for stdout/stderr."
            )

        parse_cli_logs(result, logger)

        rinex_file_paths = [x for x in workdir.rglob(f"*{site}*") if not x.suffix == ".json"]
        logger.loginfo(
            f"Converted {len(file_paths)} input files to {len(rinex_file_paths)} Daily RINEX files"
        )
        outpaths: List[Path] = []
        for rinex_file in rinex_file_paths:
            logger.logdebug(f" RINEX file: {str(rinex_file)}")
            new_rinex_path = writedir / rinex_file.name
            if new_rinex_path.exists():
                warnings.warn(
                    f"RINEX file {new_rinex_path} already exists and will be overwritten.",
                    stacklevel=2,
                )
            shutil.move(src=rinex_file, dst=new_rinex_path)
            logger.loginfo(f"Generated Daily RINEX file {str(new_rinex_path)}")
            outpaths.append(new_rinex_path)

    if not outpaths:
        warnings.warn(
            f"No RINEX files were generated from files: {file_paths}", stacklevel=2
        )
    return outpaths

def novatel_2rinex(
    files: List[Path] | List[str] | str | Path,
    writedir: Optional[Path | str] = None,
    site: Optional[str] = None,
    metadata: Optional[dict | MetadataModel | Path | str] = None,
    modulo_millis: int = 0,
    **kwargs,
) -> List[Path]:
    """Convert NovAtel 000/770 binary files to daily RINEX.

    This function accepts a single file or a list of files containing NOV000
    (``.bin``) and/or NOV770 (``.raw``) data. It groups files by parent
    directory when ``writedir`` is not provided and dispatches to the
    appropriate Go conversion binary.

    Either ``metadata`` or a 4-character ``site`` code must be supplied.

    Parameters
    ----------
    modulo_millis : int, optional
        Decimation modulo in milliseconds (e.g., 1000 for 1 Hz, 15000 for 15s
        intervals). If 0, no decimation is applied. Default is 0.
    """

    # Normalise / validate metadata
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
        if site is None:
            raise ValueError("Either metadata or site must be provided")
        if not isinstance(site, str):
            raise ValueError(f"Site must be a string, got {type(site)}")
        if len(site) != 4:
            raise ValueError(f"Site must be 4 characters long, got {site}")
        metadata = get_metadatav2(site, serialNumber=uuid.uuid4().hex[:10])

    # Normalise input files to Paths
    if isinstance(files, (str, Path)):
        file_paths: List[Path] = [Path(files)]
    else:
        file_paths = [Path(f) for f in files]

    if not file_paths:
        raise ValueError("No input files provided to novatel_2rinex")

    for file in file_paths:
        if not file.exists():
            raise FileNotFoundError(f"File not found: {file}")

    bin_files: List[Path] = []  # for NOV000.bin files
    raw_files: List[Path] = []  # for NOV770.raw files

    for file in file_paths:
        suffix = file.suffix.lower()
        match suffix:
            case ".bin":
                bin_files.append(file)
            case ".raw":
                raw_files.append(file)
            case _:
                raise ValueError(
                    f"Unsupported file extension: {suffix} for file {file}"
                )

    all_rinex_paths: List[Path] = []

    # Process NOV000 (.bin) files
    if bin_files:
        binary_path = get_nov0002rnx_binary_path()
        if writedir is None:
            # group by parent directory
            write_dirs: Dict[Path, List[Path]] = defaultdict(list)
            for file in bin_files:
                write_dirs[file.parent].append(file)
            logger.loginfo(
                "Arg writdir is None, processing NOV000.bin files in grouped "
                f"directories: {list(write_dirs.keys())}"
            )
        else:
            write_dirs = {Path(writedir): bin_files}
            logger.loginfo(
                f"Processing NOV000.bin files to writedir: {Path(writedir)}"
            )
        for write_dir, files_to_process in write_dirs.items():
            rinex_paths = _novatel_2rinex_wrapper(
                files=files_to_process,
                writedir=write_dir,
                metadata=metadata,
                binary_path=binary_path,
                modulo_millis=modulo_millis,
            )
            logger.loginfo(
                f"Converted {len(files_to_process)} NOV000.bin files to "
                f"{len(rinex_paths)} RINEX files in {write_dir}"
            )
            all_rinex_paths.extend(rinex_paths)

    # Process NOV770 (.raw) files
    if raw_files:
        binary_path = get_novb2rnxo_binary_path()
        if writedir is None:
            write_dirs: Dict[Path, List[Path]] = defaultdict(list)
            for file in raw_files:
                write_dirs[file.parent].append(file)
            logger.loginfo(
                "Arg writdir is None, processing NOV770.raw files in grouped "
                f"directories: {list(write_dirs.keys())}"
            )
        else:
            write_dirs = {Path(writedir): raw_files}
            logger.loginfo(
                f"Processing NOV770.raw files to writedir: {Path(writedir)}"
            )
        for write_dir, files_to_process in write_dirs.items():
            rinex_paths = _novatel_2rinex_wrapper(
                files=files_to_process,
                writedir=write_dir,
                metadata=metadata,
                binary_path=binary_path,
                modulo_millis=modulo_millis,
            )
            logger.loginfo(
                f"Converted {len(files_to_process)} NOV770.raw files to "
                f"{len(rinex_paths)} RINEX files in {write_dir}"
            )
            all_rinex_paths.extend(rinex_paths)

    # check for overlap in output rinex paths
    counted_paths: Dict[Path, int] = defaultdict(int)
    for rinex_path in all_rinex_paths:
        counted_paths[rinex_path] += 1
    overlapping_paths = [path for path, count in counted_paths.items() if count > 1]
    if overlapping_paths:
        warnings.warn(
            f"The following RINEX files were generated multiple times: {overlapping_paths}",
            stacklevel=2,
        )

    return all_rinex_paths
