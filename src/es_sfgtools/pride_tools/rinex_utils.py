from datetime import datetime
from pathlib import Path
from typing import Tuple

from ..logging import PRIDELogger as logger


def _header_get_time(line):
    time_values = line.split("GPS")[0].strip().split()
    start_time = datetime(
        year=int(time_values[0]),
        month=int(time_values[1]),
        day=int(time_values[2]),
        hour=int(time_values[3]),
        minute=int(time_values[4]),
        second=int(float(time_values[5])),
    )
    return start_time

def epoch_get_time(line: str) -> datetime:
    """Extracts the epoch time from a RINEX observation line.
    Args:
        line (str): A line from a RINEX observation file.
    Returns:
        datetime: The epoch time as a datetime object.
    """

    date_line = line.strip().split()
    return datetime(
        year=2000 + int(date_line[0]),
        month=int(date_line[1]),
        day=int(date_line[2]),
        hour=int(date_line[3]),
        minute=int(date_line[4]),
        second=int(float(date_line[5])),
    )

def rinex_get_time_range(source: str|Path) -> Tuple[datetime, datetime]:
    """
    Extracts the time range from a RINEX observation file.
    Args:
        source (str|Path): The path to the RINEX observation file.
    Returns:
        Tuple[datetime, datetime]: The start and end timestamps as datetime objects.
    """
    timestamp_data_start = None
    timestamp_data_end = None

    with open(source) as f:
        files = f.readlines()

        for line in files:

            if timestamp_data_start is None:
                if "TIME OF FIRST OBS" in line:
                    start_time = _header_get_time(line)
                    file_date = start_time.strftime("%Y%m%d%H%M")
                    timestamp_data_start = start_time
                    timestamp_data_end = start_time
                    year = str(timestamp_data_start.year)[2:]
                    break

            if timestamp_data_start is not None:
                # line sample: 23  6 24 23 59 59.5000000  0  9G21G27G32G08G10G23G24G02G18
                if line.strip().startswith(year):
                    try:
                        current_date = epoch_get_time(line)
                        if current_date and current_date > timestamp_data_start:
                            timestamp_data_end = current_date
                            pass
                    except Exception:
                        pass
    if (
        timestamp_data_start is not None
        and timestamp_data_end == timestamp_data_start
    ):
        # If the start and end times are the same, set the end time to the end of the day
        # This is to ensure that the end time is always after the start time
        timestamp_data_end = datetime(
            year=timestamp_data_start.year,
            month=timestamp_data_start.month,
            day=timestamp_data_start.day,
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,  # Set to the end of the day
        )
    if timestamp_data_start is None or timestamp_data_end is None:
        logger.logerr(f"Failed to extract time range from {source}")
        raise ValueError(f"Failed to extract time range from {source}")
    
    return timestamp_data_start, timestamp_data_end
