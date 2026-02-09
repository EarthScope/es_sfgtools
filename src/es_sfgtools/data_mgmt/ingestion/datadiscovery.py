import datetime
import re
import warnings
from pathlib import Path
from typing import List, Union, Optional
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.config.file_config import AssetType
from ..assetcatalog.schemas import AssetEntry
from .config import pattern_map

def _get_time(line: str) -> datetime.datetime:
    """Gets the time from a RINEX file line.

    Parameters
    ----------
    line : str
        The line from the RINEX file.

    Returns
    -------
    datetime.datetime
        The time from the line.
    """
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

def _rinex_get_meta(data: AssetEntry) -> AssetEntry:
    """Gets the metadata from a RINEX file.

    Parameters
    ----------
    data : AssetType
        The RINEX asset entry.

    Returns
    -------
    AssetType
        The RINEX asset entry with metadata.
    """
    assert data.type == AssetType.RINEX2, f"Expected RINEX2 file, got {data.type}"
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
    """Get the file type of a file.

    Parameters
    ----------
    file_path : Path
        The file path.

    Returns
    -------
    tuple[Union[AssetType, None], Union[int, None]]
        The file type and size.
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


def get_file_type_remote(file_path: str) -> Optional[AssetType]:
    """Get the file type of a file.

    Parameters
    ----------
    file_path : str
        The file path.

    Returns
    -------
    AssetType
        The file type.
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


def scrape_directory_local(directory: Path) -> Optional[List[Path]]:
    """Scrape a directory for files.

    Parameters
    ----------
    directory : str
        The directory to scrape files from.

    Returns
    -------
    List[Path]
        The list of files in the directory.
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