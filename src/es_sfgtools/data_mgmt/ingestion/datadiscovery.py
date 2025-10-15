import datetime
import re
import warnings
from pathlib import Path
from typing import List, Union
from es_sfgtools.logging import ProcessLogger as logger
from ..assetcatalog.file_schemas import AssetType

pattern_map = {
    re.compile(r"\.\d{2}O$"): AssetType.RINEX,
    re.compile("sonardyne"): AssetType.SONARDYNE,
    re.compile(r"^(?=.*novatel)(?!.*pin).*$", re.IGNORECASE): AssetType.NOVATELPIN,
    re.compile("novatel"): AssetType.NOVATEL,
    re.compile("kin"): AssetType.KIN,
    re.compile("NOV000"): AssetType.NOVATEL000,
    # re.compile("rinex"): AssetType.RINEX,
    re.compile(r"\.\d{2}o$"): AssetType.RINEX,
    re.compile("NOV770"): AssetType.NOVATEL770,
    re.compile("DFOP00.raw"): AssetType.DFOP00,
    re.compile("lever_arms"): AssetType.LEVERARM,
    re.compile("master"): AssetType.MASTER,
    re.compile(r"\.pin$"): AssetType.QCPIN,
    re.compile("CTD"): AssetType.CTD,
    re.compile("svpavg"): AssetType.SEABIRD,
    re.compile(r"\.res$"): AssetType.KINRESIDUALS,
}

def _get_time(line):
    time_values = line.split("GPS")[0].strip().split()
    start_time = datetime.datetime(
        year=int(time_values[0]),
        month=int(time_values[1]),
        day=int(time_values[2]),
        hour=int(time_values[3]),
        minute=int(time_values[4]),
        second=int(float(time_values[5])),
    )
    return start_time

def _rinex_get_meta(data:AssetType) -> AssetType:
    assert data.type == AssetType.RINEX, f"Expected RINEX file, got {data.type}"
    with open(data.local_path) as f:
        files = f.readlines()
        for line in files:
            if "TIME OF FIRST OBS" in line:
                start_time = _get_time(line)
                file_date = start_time.strftime("%Y%m%d%H%M%S")
                data.timestamp_data_start = start_time

            if "TIME OF LAST OBS" in line:
                end_time = _get_time(line)
                data.timestamp_data_end = end_time
                break
    return data


def get_file_type_local(file_path: Path) -> tuple[Union[AssetType, None], Union[int, None]]:
    """
    Get the file type of a file.

    Args:
        file_path (Path): The file path.

    Returns:
        file_type: The file type.
        file_size: The file size.
    """
    file_type = None
    for pattern, ftype in pattern_map.items():
        if pattern.search(str(file_path.name)):
            file_type = ftype
            break
        
    size = file_path.stat().st_size

    if size == 0:
        logger.logwarn(f"File {str(file_path)} is empty, not processing")
        return None, None
    
    if file_type is None:
        logger.logdebug(f"File type not recognized for {str(file_path)}")
        return None, None
    
    return file_type, size


def get_file_type_remote(file_path: str) -> AssetType:
    """
    Get the file type of a file.

    Args:
        file_path (str): The file path.

    Returns:
        file_type: The file type.
    """
    file_type = None
    for pattern, ftype in pattern_map.items():
        if pattern.search(file_path):
            file_type = ftype
            break

    if file_type is None:
        logger.logwarn(f"File type not recognized for {file_path}")
        return None
    
    return file_type


def scrape_directory_local(directory: Path) -> List[Path]:
    """
    Scrape a directory for files.

    Args:
        directory (str): The directory to scrape files from.

    Returns:
        List[Path]: The list of files in the directory.

    """
    if isinstance(directory, str):
        directory = Path(directory)

    initial_files = list(directory.rglob("*"))

    output_files = []
    for file in initial_files:
        if file.is_file():
            output_files.append(file)
    
    if len(output_files) == 0:
        logger.logwarn("No files found in directory")
        return None
    
    return output_files
