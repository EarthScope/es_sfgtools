import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Union, Tuple
import re

from .gnss_product_operations import get_gnss_products
from .pride_cli_config import PrideCLIConfig
from .rinex_utils import rinex_get_time_range
from ..logging import GNSSLogger as logger

def remove_ansi_escape(text):
    ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", text)


def rinex_to_kin(
    source: str,
    writedir: Path,
    pridedir: Path,
    site="SIT1",
    pride_cli_config: PrideCLIConfig = None,
) -> Tuple[Path, Path]:
    """
    This function is a wrapper for the PRIDE-PPP processing tool (pdp3) to generate kinematic and residual files from a RINEX file.

    Args:
        source (Union[AssetEntry,str,Path]): The source RINEX file to convert.
        writedir (Path): The directory to write the converted kin file.
        pridedir (Path): The directory where PRIDE-PPP observables are stored.
        site (str, optional): The site name. Defaults to "SITE1".
        pride_cli_config (PrideCLIConfig, optional): The configuration for PRIDE-PPP processing. If None, uses default settings.

    Returns:
        Tuple[Path, Path]: The generated kin and result files as Path objects.

    Raises:
        FileNotFoundError: If the PRIDE-PPP binary is not found in the system path.
        FileNotFoundError: If the source RINEX file does not exist.

    Notes:

    Examples:
        >>> source = Path("/path/to/NCB12450.24o") # Example RINEX file path
        >>> writedir = Path("/writedir") # Directory to write the kin file
        >>> pridedir = Path("/pridedir") # Directory where PRIDE-PPP observables are stored
        # Get the PRIDE configuration file path
        >>> pride_configfile_path = get_gnss_products(
            rinex_path=source,
            pride_dir=pridedir,
            override=False,
            source="all"
        )
        # Create a PrideCLIConfig instance with the configuration file path
        >>> pride_config = PrideCLIConfig(
            sample_frequency=1,
            override=False,
            pride_configfile_path=pride_configfile_path,
        )
        # Run PRIDE-PPP to generate kin and res files
        >>> kin_file, res_file = rinex_to_kin(
            source=source,
            writedir=writedir,
            pridedir=pridedir,
            site="NCB1",
            pride_config=pride_config,
        )
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

    logger.loginfo(f"Converting RINEX file {source} to kin file")

    if not source.exists():
        logger.logerr(f"RINEX file {source} not found")
        raise FileNotFoundError(f"RINEX file {source} not found")

    timestamp_data_start, _ = rinex_get_time_range(source)

    # If PridePdpConfig is not provided, use the default configuration
    if pride_cli_config is None:
        pride_cli_config = PrideCLIConfig()

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

    # Log stdout lines
    for line in stdout.splitlines():
        line = remove_ansi_escape(line.strip())
        # Add file name to the log line
        line = f"{source}: {line}"
        if "ERROR" in line or "error" in line or "line" in line:
            logger.logerr(line)
        elif "WARNING" in line or "warning" in line:
            logger.logwarn(line)
        else:
            logger.logdebug(line)

    # Log stderr lines
    for line in stderr.splitlines():
        line = remove_ansi_escape(line.strip())
        # Add file name to the log line
        line = f"{source}: {line}"
        logger.logerr(line)

    year, doy = (
        timestamp_data_start.year,
        timestamp_data_start.timetuple().tm_yday,
    )
    file_dir = Path(pridedir) / str(year) / str(doy)

    kin_file_path = file_dir / f"kin_{str(year)}{str(doy)}_{site.lower()}"
    res_file_path = file_dir / f"res_{str(year)}{str(doy)}_{site.lower()}"
    kin_file = None
    res_file = None

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
