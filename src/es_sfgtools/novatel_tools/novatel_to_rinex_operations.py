from pathlib import Path
import subprocess
import tempfile
import shutil
import uuid
import json
from typing import List, Optional, Dict
from collections import defaultdict
import os
from colorama import Fore, Style
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ES_SFGTools.NovatelToRinex")

# Local imports
from es_sfgtools.utils.command_line_utils import parse_cli_logs
from .utils import (
    MetadataModel,
    get_metadatav2,
    get_nov0002rnx_binary_path,
    get_novb2rnxo_binary_path,
    check_metadata,
    check_metadata_path,
)

os.environ["DYLD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib"
os.environ["LD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib"

def _novatel_2rinex_wrapper(
    files: List[Path] | List[str],
    writedir: Path,
    metadata: dict | Path | str,
    binary_path: Path,
) -> List[Path]:
    """Internal helper to call a NovAtel-to-RINEX Go binary on a batch of files.

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

    Returns
    -------
    List[pathlib.Path]
        Paths to the RINEX files moved into ``writedir``.

    Raises
    ------
    ValueError
        If ``files`` is empty or ``metadata`` cannot be interpreted.
    FileNotFoundError
        If a provided metadata path does not exist.
    RuntimeError
        If the underlying Go binary exits with a non-zero return code.
    """

    # Normalise the input file list to Paths for logging and command building
    if not files:
        raise ValueError(Fore.RED + "No input files provided to _novatel_2rinex_wrapper" + Style.RESET_ALL)

    file_paths: List[Path] = [Path(f) for f in files]

    with tempfile.TemporaryDirectory(dir="/tmp/") as workdir_str:
        workdir = Path(workdir_str)

        # Always end up with a metadata dict so we can consistently get marker_name
        if isinstance(metadata, dict):
            metadata_dict = metadata
        elif isinstance(metadata, (Path, str)):
            metadata_path = Path(metadata)
            if not metadata_path.exists():
                raise FileNotFoundError(Fore.RED + f"Metadata file not found: {metadata_path}" + Style.RESET_ALL)
            if not metadata_path.is_file():
                raise ValueError(Fore.RED + f"Metadata path is not a file: {metadata_path}" + Style.RESET_ALL)
            if (suffix := metadata_path.suffix.lower()) != ".json":
                raise ValueError(Fore.RED + f"Metadata file must be a JSON file, got {suffix}" + Style.RESET_ALL)
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
            *[str(p) for p in file_paths],
        ]
        cmd_str = " ".join(cmd)
        logger.info(f" Running {Fore.CYAN}{cmd_str}{Style.RESET_ALL} in {workdir}")
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
                f"{Fore.RED}{binary_path.name} failed with return code {result.returncode}. "
                "See logs for stdout/stderr." + Style.RESET_ALL
            )

        parse_cli_logs(result, logger)

        rinex_file_paths = [
            x for x in workdir.rglob(f"*{site}*") if not x.suffix == ".json"
        ]
        print(f"\n{'='*40}")
        logger.info(
            f"\nConverted {Fore.GREEN}{len(file_paths)}{Style.RESET_ALL} input files to {Fore.GREEN}{len(rinex_file_paths)}{Style.RESET_ALL} Daily RINEX files"
        )
        outpaths: List[Path] = []
        for rinex_file in rinex_file_paths:
            logger.debug(f" RINEX file: {str(rinex_file)}")
            new_rinex_path = writedir / rinex_file.name
            if new_rinex_path.exists():
                logger.warning(
                    f"{Fore.YELLOW}RINEX file {new_rinex_path} already exists and will be overwritten.{Style.RESET_ALL}",
                    stacklevel=1,
                )
            shutil.move(src=rinex_file, dst=new_rinex_path)
            logger.info(f"Generated Daily RINEX file {str(new_rinex_path)}")
            outpaths.append(new_rinex_path)

    if not outpaths:
        logger.warning(
            f"No RINEX files were generated from files: {file_paths}", stacklevel=1
        )
    return outpaths


def novatel_2rinex(
    files: List[Path] | List[str] | str | Path,
    writedir: Optional[Path | str] = None,
    site: Optional[str] = None,
    metadata: Optional[dict | MetadataModel | Path | str] = None,
    **kwargs,
) -> List[Path]:
    """Convert NovAtel NOV000 / NOV770 binary files to daily RINEX.

    This high-level helper accepts a single file or a list of files
    containing NOV000 (``.bin``) and/or NOV770 (``.raw``) data. If
    ``writedir`` is not provided, files are grouped by parent directory and
    converted in-place (RINEX written alongside the source data). If
    ``writedir`` is supplied, all outputs are written to that directory.

    Parameters
    ----------
    files : List[pathlib.Path] | List[str] | str | pathlib.Path
        Input NOV000/NOV770 files to convert. Can be a single file path or a
        list of file paths.
    writedir : Optional[pathlib.Path | str], optional
        Directory where output RINEX files will be written. If not provided,
        files are grouped by their parent directory and RINEX files are
        written to those directories, by default ``None``.
    site : Optional[str], optional
        4-character site code. Required if ``metadata`` is not provided.
        Ignored if ``metadata`` is provided, by default ``None``.
    metadata : Optional[dict | MetadataModel | pathlib.Path | str], optional
        Metadata for the site. Can be a dictionary, a ``MetadataModel``
        instance, or a path to a JSON metadata file. If not provided, ``site``
        must be given so that minimal metadata can be generated, by default
        ``None``.
    **kwargs
        Currently ignored; reserved for future configuration options passed
        through to the underlying Go binaries.

    Returns
    -------
    List[pathlib.Path]
        List of generated daily RINEX file paths.

    Raises
    ------
    ValueError
        If input files are invalid, or if both ``metadata`` and ``site`` are
        missing/invalid.
    FileNotFoundError
        If any of the input files cannot be found.
    RuntimeError
        If the underlying Go binaries return a non-zero status code.

    Calls
    -----
    - ``_novatel_2rinex_wrapper``
    - ``get_nov0002rnx_binary_path``
    - ``get_novb2rnxo_binary_path``
    - ``check_metadata``
    - ``check_metadata_path``




    Notes
    -----
    - Either ``metadata`` or a 4-character ``site`` code must be supplied.
    - NOV000.bin files (``.bin``) typically have lower frequency GNSS observations. Because
    of this we process these first before NOV770.raw files (``.raw``). This ensures that
    RINEX files with higher frequency observations are not inadvertently overwritten by
    lower frequency data when ``writedir`` is shared.

    """

    # Normalise / validate metadata
    if metadata is not None:
        if isinstance(metadata, (str, Path)):
            metadata = check_metadata_path(metadata)
        elif isinstance(metadata, (dict, MetadataModel)):
            metadata = check_metadata(metadata)
        else:
            raise ValueError(
                Fore.RED + "Metadata must be a dict, MetadataModel, or path to a JSON file, "
                f"got {type(metadata)}" + Style.RESET_ALL,
            )
    else:
        if site is None:
            raise ValueError(Fore.RED + "Either metadata or site must be provided" + Style.RESET_ALL)
        if not isinstance(site, str):
            raise ValueError(f"Site must be a string, got {type(site)}")
        if len(site) != 4:
            raise ValueError(f"Site must be 4 characters long, got {site}")
        # Generate minimal metadata if none provided
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
            logger.info(
                "Arg writdir is None, processing NOV000.bin files in grouped "
                f"directories: {list(write_dirs.keys())}"
            )
        else:
            write_dirs = {Path(writedir): bin_files}
            logger.info(f"Processing NOV000.bin files to writedir: {Path(writedir)}")
        for write_dir, files_to_process in write_dirs.items():
            rinex_paths = _novatel_2rinex_wrapper(
                files=files_to_process,
                writedir=write_dir,
                metadata=metadata,
                binary_path=binary_path,
            )
            logger.info(
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
            logger.info(
                "Arg writdir is None, processing NOV770.raw files in grouped "
                f"directories: {list(write_dirs.keys())}"
            )
        else:
            write_dirs = {Path(writedir): raw_files}
            logger.info(f"Processing NOV770.raw files to writedir: {Path(writedir)}")
        for write_dir, files_to_process in write_dirs.items():
            rinex_paths = _novatel_2rinex_wrapper(
                files=files_to_process,
                writedir=write_dir,
                metadata=metadata,
                binary_path=binary_path,
            )
            logger.info(
                f"Converted {len(files_to_process)} NOV770.raw files to "
                f"{len(rinex_paths)} RINEX files in {write_dir}"
            )
            all_rinex_paths.extend(rinex_paths)

    # check for overlap in output rinex paths
    counted_paths: Dict[Path, int] = defaultdict(int)
    for rinex_path in all_rinex_paths:
        counted_paths[rinex_path] += 1
    overlapping_paths = [path for path, count in counted_paths.items() if count > 1]
    overlapping_paths = "\n".join([str(p) for p in overlapping_paths])

    if overlapping_paths:
        logger.warning(
            Fore.YELLOW + f"\nThe following RINEX files were generated multiple times: \n{overlapping_paths}\n" + Style.RESET_ALL,
            stacklevel=1,
        )

    return all_rinex_paths
