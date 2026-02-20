"""
Functions for processing sound velocity profile (SVP) data from various sources.
"""
# External imports
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

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

    Parameters
    ----------
    source : str or Path
        The path to the Seabird data file.

    Returns
    -------
    DataFrame[SoundVelocityDataFrame]
        A DataFrame containing 'depth' and 'speed' columns.

    Raises
    ------
    FileNotFoundError
        If the source file does not exist.
    ValueError
        If no data is found in the file after the "*END*" marker.
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

    Parameters
    ----------
    source : str or Path
        The path to the CTD data file.

    Returns
    -------
    DataFrame[SoundVelocityDataFrame]
        A DataFrame containing 'depth' and 'speed' columns.
    """
    df = pd.read_csv(source, names=["depth", "speed"])
    df["depth"] = -df["depth"]
    df = interpolate_svp(df, additional_depth=200.0)
    return SoundVelocityDataFrame(df, lazy=True)


@pa.check_types(lazy=True)
def CTD_to_svp_v2(
    source: str | Path,
) -> DataFrame[SoundVelocityDataFrame]:
    """
    Converts CTD data from a specified source file into a sound velocity profile DataFrame (version 2).

    This function reads a space-separated file, renames columns to 'depth' and 'speed',
    negates depth values, and applies a small random adjustment to speed values.

    Parameters
    ----------
    source : str or Path
        The path to the CTD data file.

    Returns
    -------
    DataFrame[SoundVelocityDataFrame]
        A DataFrame containing 'depth' and 'speed' columns.
    """
    df = pd.read_csv(source, sep="\s+", names=["depth", "speed"])
    df["depth"] = -df["depth"]
    df["speed"] = df["speed"] + np.random.randn(len(df)) * 1e-6
    df = interpolate_svp(df, additional_depth=200.0)
    return SoundVelocityDataFrame(df, lazy=True)


def interpolate_svp(
        svp:pd.DataFrame,
        additional_depth:float=200.0,
) -> pd.DataFrame:
    """
    Interpolates a sound velocity profile (SVP) DataFrame to ensure coverage down to a specified additional depth.

    This function checks the maximum depth in the provided SVP DataFrame and, if necessary,
    extends the profile by adding interpolated values down to the specified additional depth.

    Parameters
    ----------
    svp : pd.DataFrame
        A DataFrame containing 'depth' and 'speed' columns.
    additional_depth : float, default 200.0
        The additional depth (in meters) to extend the SVP profile.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the interpolated SVP data.
    """
    max_depth = svp['depth'].max()

    new_depths = np.arange(max_depth + 1, max_depth + additional_depth)
    new_speeds = np.interp(new_depths, svp['depth'].to_numpy(), svp['speed'].to_numpy())
    new_svp = pd.DataFrame({'depth': new_depths, 'speed': new_speeds})
    svp = pd.concat([svp, new_svp], ignore_index=True)
    logger.loginfo(f"Extended SVP to {additional_depth} m depth with interpolation.")
    return svp