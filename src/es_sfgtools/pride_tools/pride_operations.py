import datetime
import shutil
import subprocess
from collections import namedtuple
from pathlib import Path
from typing import Tuple

from ..logging import PRIDELogger as logger
from ..utils.command_line_utils import parse_cli_logs
from .pride_cli_config import PrideCLIConfig
from .rinex_utils import rinex_get_time_range
from .kin_file_operations import validate_kin_file

# make output of subprocess.Popen identical to  subprocess.run
result = namedtuple("result", ["stdout", "stderr"])

def rinex_to_kin(
    source: str,
    writedir: Path,
    pridedir: Path,
    site="SIT1",
    pride_cli_config: PrideCLIConfig = None,
) -> Tuple[Path, Path]:
    """Generate kinematic and residual files from a RINEX file.

    This function is a wrapper for the PRIDE-PPP processing tool (pdp3).

    Parameters
    ----------
    source : str
        The source RINEX file to convert.
    writedir : Path
        The directory to write the converted kin file.
    pridedir : Path
        The directory where PRIDE-PPP observables are stored.
    site : str, optional
        The site name, by default "SIT1".
    pride_cli_config : PrideCLIConfig, optional
        The configuration for PRIDE-PPP processing. If None, uses default
        settings.

    Returns
    -------
    Tuple[Path, Path]
        The generated kin and result files as Path objects.

    Raises
    ------
    FileNotFoundError
        If the PRIDE-PPP binary is not found in the system path.
    FileNotFoundError
        If the source RINEX file does not exist.

    Examples
    --------
    >>> source = Path("/path/to/NCB12450.24o") # Example RINEX file path
    >>> writedir = Path("/writedir") # Directory to write the kin file
    >>> pridedir = Path("/pridedir") # Directory where PRIDE-PPP observables are stored
    # Get the PRIDE configuration file path
    >>> pride_configfile_path = get_gnss_products(
    ...     rinex_path=source,
    ...     pride_dir=pridedir,
    ...     override=False,
    ...     source="all"
    ... )
    # Create a PrideCLIConfig instance with the configuration file path
    >>> pride_config = PrideCLIConfig(
    ...     sample_frequency=1,
    ...     override=False,
    ...     pride_configfile_path=pride_configfile_path,
    ... )
    # Run PRIDE-PPP to generate kin and res files
    >>> kin_file, res_file = rinex_to_kin(
    ...     source=source,
    ...     writedir=writedir,
    ...     pridedir=pridedir,
    ...     site="NCB1",
    ...     pride_config=pride_config,
    ... )
    >>> kin_file
    Path("writedir/kin_2024126_ncb1.kin")
    >>> res_file
    Path("writedir/res_2024126_ncb1.res")
    """

    # Check if the pride binary is in the path
    if not shutil.which("pdp3"):
        raise FileNotFoundError("PRIDE-PPP binary 'pdp3' not found in path")

    # Ensure source is a Path object
    if isinstance(source, str):
        source = Path(source)

    if not source.exists():
        logger.logerr(f"RINEX file {source} not found")
        raise FileNotFoundError(f"RINEX file {source} not found")

    # If PridePdpConfig is not provided, use the default configuration
    if pride_cli_config is None:
        pride_cli_config = PrideCLIConfig()

    '''
    Step 1: Determine the year and day of year from the RINEX file to construct the expected output file paths.
    '''
    timestamps: Tuple[datetime.datetime, datetime.datetime] = rinex_get_time_range(source)

    year, doy = (
        timestamps[0].year,
        timestamps[0].timetuple().tm_yday,
    )
    file_dir = Path(pridedir) / str(year) / str(doy)

    kin_file_path = file_dir / f"kin_{str(year)}{str(doy)}_{site.lower()}" # Expected kin file path after running pdp3
    res_file_path = file_dir / f"res_{str(year)}{str(doy)}_{site.lower()}" # Expected res file path after running pdp3
    kin_file_new = writedir / (kin_file_path.name + ".kin") # Where the kin file will be moved
    res_file_new = writedir / (res_file_path.name + ".res") # Where the res file will be moved
    kin_file = None
    res_file = None

    '''
    Step 2: Determine if processing is needed based on the existence of output files and the override flag.

    Case 1: If the kin file already exists in the writedir and override is False, skip processing.
    Case 2: If the kin file exists in the pridedir and override is False, move it to writedir and skip processing.
    Case 3: run pdp3 to generate the kin and res files.
    '''
    logger.loginfo(f"Determining if processing is needed for RINEX file {source}")

    # Case 1
    if validate_kin_file(kin_file_new) and not pride_cli_config.override:
        logger.loginfo(f"Kin file {kin_file_new} already exists, skipping processing")
            # continue to process the file
        kin_file = kin_file_new
        if res_file_new.exists():
            logger.loginfo(f"Res file {res_file_new} already exists, skipping processing")
            res_file = res_file_new
        else:
            logger.logwarn(f"Res file {res_file_new} not found")

        return kin_file, res_file
    
    # Case 2
    if validate_kin_file(kin_file_path) and not pride_cli_config.override:
        shutil.move(src=kin_file_path, dst=kin_file_new)
        kin_file = kin_file_new
        logger.loginfo(f"Kin file {kin_file} already exists, moved to {kin_file_new}")
        if res_file_path.exists():
            shutil.move(src=res_file_path, dst=res_file_new)
            res_file = res_file_new
            logger.loginfo(f"Res file {res_file} already exists, moved to {res_file_new}")
        else:
            logger.logwarn(f"Res file {res_file_path} not found")
        return kin_file, res_file

    # Case 3
    pdp_command = pride_cli_config.generate_pdp_command(
        site=site,
        local_file_path=source,
    )

    logger.loginfo(f"Running pdp3 with command: {' '.join(pdp_command)}")
    # Run pdp3 in the pride directory
    process = subprocess.Popen(
        " ".join(pdp_command),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(pridedir),
        text=True,
    )

    stdout, stderr = process.communicate()
    _results = result(stdout=stdout, stderr=stderr)
    parse_cli_logs(result=_results, logger=logger)

    if kin_file_path.exists():
        kin_file_new = writedir / (kin_file_path.name + ".kin")
        shutil.move(src=kin_file_path, dst=kin_file_new)
        kin_file = kin_file_new
        logger.loginfo(
            f"Generated kin file {kin_file} from RINEX file {source}"
        )
    else:
        response = f"No kin file generated from RINEX {source}"
        logger.logerr(response)
        return None, None

    if res_file_path.exists():
        res_file_new = writedir / (res_file_path.name + ".res")
        shutil.move(src=res_file_path, dst=res_file_new)
        res_file = res_file_new
        logger.loginfo(f"Found PRIDE res file {res_file}")

    else:
        response = f"No res file generated from RINEX {source}"
        logger.logerr(response)
        return kin_file, None

    return kin_file, res_file

def cleanup_pride_files(pride_workdir: Path,site_name: str) -> None:
    """Cleans up temporary files generated by PRIDE-PPP in the specified working directory.

    This function removes intermediate files created during PRIDE-PPP processing to free up disk space.

    Parameters
    ----------
    pride_workdir : Path
        The working directory where PRIDE-PPP temporary files are located.
    site_name : str
        The site name associated with the PRIDE-PPP processing.
    Returns
    -------
    None
    """
    assert len(site_name) == 4, "Site name must be 4 characters long."

    

    site_files = pride_workdir.glob(f"*_{site_name.lower()}*")
    if not site_files:
        logger.loginfo(f"No temporary PRIDE files found for site {site_name} in {pride_workdir}")
        return
    delete_count = 0
    deleted = []
    for file in site_files:
        try:
            file.unlink()
            deleted.append(str(file))
            delete_count += 1
            logger.loginfo(f"Deleted temporary PRIDE file: {file}")
        except Exception as e:
            logger.logwarn(f"Could not delete file {file}: {e}")
    logger.loginfo(f"Deleted {delete_count} temporary PRIDE files for site {site_name} in {pride_workdir}")
    logger.logdebug(f"Deleted files: {deleted}")