"""
Functions for processing sound velocity profile (SVP) data from various sources.
"""
# External imports
import re
import pandas as pd
import pandera as pa
import numpy as np
from pandera.typing import DataFrame
from pathlib import Path

# Local imports
from ..data_models.observables import SoundVelocityDataFrame
from ..logging import ProcessLogger as logger


@pa.check_types(lazy=True)
def seabird_to_soundvelocity(
    source: str | Path,
) -> DataFrame[SoundVelocityDataFrame]:
    """
    Reads sound velocity profile data from a Seabird-formatted file.

    The function expects a file where data starts after a line containing "*END*".
    It extracts depth and speed values to create a SoundVelocityDataFrame.

    :param source: The path to the Seabird data file.
    :type source: Union[str, Path]
    :returns: A DataFrame containing 'depth' and 'speed' columns.
    :rtype: DataFrame[SoundVelocityDataFrame]
    :raises FileNotFoundError: If the source file does not exist.
    :raises ValueError: If no data is found in the file after the "*END*" marker.
    """
 
    with open(source, "r") as f:
        lines = f.readlines()
        data = []
        data_start = re.compile("\*END\*")
        while lines:
            line = lines.pop(0)
            if data_start.match(line):
                break

        if not lines:
            logger.logerr(
                f"No data found in the sound speed profile file {source}"
            )
            return None

        for line in lines:

            values = line.split()
            data.append(
                {
                    "depth": float(values[0]),
                    "speed": float(values[5]),
                }
            )
        df = pd.DataFrame(data)
        logger.loginfo(
            f"Found SS data down to max depth of {df['depth'].max()} m\n"
            f"SS ranges from {df['speed'].min()} to {df['speed'].max()} m/s"
        )

    return SoundVelocityDataFrame(df, lazy=True)


@pa.check_types(lazy=True)
def CTD_to_svp_v1(
    source: str | Path,
) -> DataFrame[SoundVelocityDataFrame]:
    """
    Converts CTD data from a specified source file into a sound velocity profile DataFrame (version 1).

    This function reads a CSV file, expecting two columns (depth and speed), and negates the depth values.

    :param source: The path to the CTD data file.
    :type source: Union[str, Path]
    :returns: A DataFrame containing 'depth' and 'speed' columns.
    :rtype: DataFrame[SoundVelocityDataFrame]
    """
    df = pd.read_csv(source, names=["depth", "speed"])
    df["depth"] = -df["depth"]
    return SoundVelocityDataFrame(df, lazy=True)


@pa.check_types(lazy=True)
def CTD_to_svp_v2(
    source: str | Path,
) -> DataFrame[SoundVelocityDataFrame]:
    """
    Converts CTD data from a specified source file into a sound velocity profile DataFrame (version 2).

    This function reads a space-separated file, renames columns to 'depth' and 'speed',
    negates depth values, and applies a small random adjustment to speed values.

    :param source: The path to the CTD data file.
    :type source: Union[str, Path]
    :returns: A DataFrame containing 'depth' and 'speed' columns.
    :rtype: DataFrame[SoundVelocityDataFrame]
    """
    df = pd.read_csv(source, sep="\s+", names=["depth", "speed"])
    df["depth"] = -df["depth"]
    df["speed"] = df["speed"] + np.random.randn(len(df)) * 1e-6
    return SoundVelocityDataFrame(df, lazy=True)
