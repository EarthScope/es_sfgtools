from pathlib import Path
import re
import warnings
from typing import List, Union
import datetime
from ..assets.file_schemas import AssetType,AssetEntry

pattern_map = {
    re.compile("sonardyne"): AssetType.SONARDYNE,
    re.compile("novatel"): AssetType.NOVATEL,
    re.compile("kin"): AssetType.KIN,
    re.compile("rinex"): AssetType.RINEX,
    re.compile(r"\.\d{2}O$"): AssetType.RINEX,
    re.compile("NOV770"): AssetType.NOVATEL770,
    re.compile("DFOP00.raw"): AssetType.DFOP00,
    re.compile("lever_arms"): AssetType.LEVERARM,
    re.compile("master"): AssetType.MASTER,
    re.compile(r"\.pin$"): AssetType.QCPIN,
    re.compile("CTD"): AssetType.CTD,
    re.compile("svpavg"): AssetType.SEABIRD,
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



def get_file_type_local(file_path: Path) -> AssetType:
    """
    Get the file type of a file.

    Args:
        file_path (Path): The file path.

    Returns:
        DiscoveredFile: The discovered file.
    """
    file_type = None
    for pattern, ftype in pattern_map.items():
        if pattern.search(str(file_path)):
            file_type = ftype
            break
        
    size = file_path.stat().st_size

    if size == 0:
        warnings.warn(f"File {str(file_path)} is empty")
        return
    if file_type is None:
        warnings.warn(f"File type not recognized for {str(file_path)}")
        return

    discoveredFile =AssetEntry(
        local_path=str(file_path),
        type=file_type,
        size=size,
        timestamp_data_start=None,
        timestamp_data_end=None,
    )
    match file_type:
        case AssetType.RINEX:
            return _rinex_get_meta(discoveredFile)
        case _:
            return discoveredFile

def get_file_type_remote(file_path: str) -> AssetType:
    """
    Get the file type of a file.

    Args:
        file_path (str): The file path.

    Returns:
        DiscoveredFile: The discovered file.
    """
    file_type = None
    for pattern, ftype in pattern_map.items():
        if pattern.search(file_path):
            file_type = ftype
            break
    if file_type is None:
        warnings.warn(f"File type not recognized for {file_path}")
        return

    discoveredFile = AssetType(
        remote_path=file_path,
        type=AssetType.value,
        timestamp_data_start=None,
        timestamp_data_end=None,
    )
    return discoveredFile

def scrape_directory_local(directory: Union[str, Path]) -> List[AssetType]:
    """
    Scrape a directory for files.

    Args:
        directory (Union[str, Path]): The directory.

    Returns:
        List[DiscoveredFile]: The discovered files.
    """
    if isinstance(directory, str):
        directory = Path(directory)
    files = list(directory.rglob("*"))
    output = []
    for file in files:
        if file.is_file():
            discoveredFile: Union[DiscoveredFile, None] = get_file_type_local(file)
            if discoveredFile is not None:
                output.append(discoveredFile)
    
    if output is None:
        warnings.warn("No files found in directory")
        
    return output
