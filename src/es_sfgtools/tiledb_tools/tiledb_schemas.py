"""
This module defines the TileDB array schemas and provides a set of classes
for interacting with those arrays.

The schemas are defined for various types of seafloor geodesy data, including
kinematic position data, IMU data, acoustic data, and raw shot data.

The TBDArray class and its subclasses provide a high-level interface for
creating, writing to, and reading from these TileDB arrays, handling both
local and S3 storage.
"""
import datetime
import os
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tiledb
from cloudpathlib import S3Path

from ..data_models.observables import (
    AcousticDataFrame,
    IMUPositionDataFrame,
    KinPositionDataFrame,
    ShotDataFrame,
)
from ..logging import ProcessLogger as logger

filters = tiledb.FilterList([tiledb.ZstdFilter(7)])
TimeDomain = tiledb.Dim(name="time", dtype="datetime64[ms]")
TransponderDomain = tiledb.Dim(name="transponderID", dtype="ascii")
attribute_dict: Dict[str, tiledb.Attr] = {
    "east": tiledb.Attr(name="east", dtype=np.float64),
    "north": tiledb.Attr(name="north", dtype=np.float64),
    "up": tiledb.Attr(name="up", dtype=np.float64),
    "east_std0": tiledb.Attr(name="east_std0", dtype=np.float64, nullable=True),
    "north_std0": tiledb.Attr(name="north_std0", dtype=np.float64, nullable=True),
    "up_std0": tiledb.Attr(name="up_std0", dtype=np.float64, nullable=True),
    "east_std1": tiledb.Attr(name="east_std1", dtype=np.float64, nullable=True),
    "north_std1": tiledb.Attr(name="north_std1", dtype=np.float64, nullable=True),
    "up_std1": tiledb.Attr(name="up_std1", dtype=np.float64, nullable=True),
    "latitude": tiledb.Attr(name="latitude", dtype=np.float64),
    "longitude": tiledb.Attr(name="longitude", dtype=np.float64),
    "height": tiledb.Attr(name="height", dtype=np.float64),
    "latitude_std": tiledb.Attr(name="latitude_std", dtype=np.float64, nullable=True),
    "longitude_std": tiledb.Attr(name="longitude_std", dtype=np.float64, nullable=True),
    "height_std": tiledb.Attr(name="height_std", dtype=np.float64, nullable=True),
    "returnTime": tiledb.Attr(name="returnTime", dtype="datetime64[ns]"),
    "tt": tiledb.Attr(name="tt", dtype=np.float64),
    "dbv": tiledb.Attr(name="dbv", dtype=np.float32),
    "xc": tiledb.Attr(name="xc", dtype=np.uint8),
    "snr": tiledb.Attr(name="snr", dtype=np.float64),
    "tat": tiledb.Attr(name="tat", dtype=np.float64),
    "isUpdated": tiledb.Attr(name="isUpdated", dtype=np.bool_),
    "northVelocity": tiledb.Attr(name="northVelocity", dtype=np.float64),
    "eastVelocity": tiledb.Attr(name="eastVelocity", dtype=np.float64),
    "upVelocity": tiledb.Attr(name="upVelocity", dtype=np.float64),
    "northVelocity_std": tiledb.Attr(
        name="northVelocity_std", dtype=np.float64, nullable=True
    ),
    "eastVelocity_std": tiledb.Attr(
        name="eastVelocity_std", dtype=np.float64, nullable=True
    ),
    "upVelocity_std": tiledb.Attr(
        name="upVelocity_std", dtype=np.float64, nullable=True
    ),
    "roll": tiledb.Attr(name="roll", dtype=np.float64),
    "pitch": tiledb.Attr(name="pitch", dtype=np.float64),
    "azimuth": tiledb.Attr(name="azimuth", dtype=np.float64),
    "roll_std": tiledb.Attr(name="roll_std", dtype=np.float64, nullable=True),
    "pitch_std": tiledb.Attr(name="pitch_std", dtype=np.float64, nullable=True),
    "azimuth_std": tiledb.Attr(name="azimuth_std", dtype=np.float64, nullable=True),
    # "status": tiledb.Attr(name="status", dtype=str, nullable=True),
}

KinPositionAttributes = [
    attribute_dict["latitude"],
    attribute_dict["longitude"],
    attribute_dict["height"],
    attribute_dict["east"],
    attribute_dict["north"],
    attribute_dict["up"],
    # attribute_dict["east_std"],
    # attribute_dict["north_std"],
    # attribute_dict["up_std"],
    tiledb.Attr(name="number_of_satellites", dtype="uint8"),
    tiledb.Attr(name="pdop", dtype=np.float64),
    tiledb.Attr(name="wrms", dtype=np.float64),
]
KinPositionArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain),
    attrs=KinPositionAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

IMUPositionAttributes = [
    attribute_dict["azimuth"],
    attribute_dict["pitch"],
    attribute_dict["roll"],
    attribute_dict["latitude"],
    attribute_dict["longitude"],
    attribute_dict["height"],
    attribute_dict["latitude_std"],
    attribute_dict["longitude_std"],
    attribute_dict["height_std"],
    attribute_dict["northVelocity"],
    attribute_dict["eastVelocity"],
    attribute_dict["upVelocity"],
    attribute_dict["northVelocity_std"],
    attribute_dict["eastVelocity_std"],
    attribute_dict["upVelocity_std"],
    attribute_dict["roll_std"],
    attribute_dict["pitch_std"],
    attribute_dict["azimuth_std"],
    # attribute_dict["status"],
]
IMUPositionArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain),
    attrs=IMUPositionAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

ShotDataAttributes = [
    tiledb.Attr(name="head0", dtype=np.float64),
    tiledb.Attr(name="pitch0", dtype=np.float64),
    tiledb.Attr(name="roll0", dtype=np.float64),
    tiledb.Attr(name="head1", dtype=np.float64),
    tiledb.Attr(name="pitch1", dtype=np.float64),
    tiledb.Attr(name="roll1", dtype=np.float64),
    tiledb.Attr(name="east0", dtype=np.float64),
    tiledb.Attr(name="north0", dtype=np.float64),
    tiledb.Attr(name="up0", dtype=np.float64),
    tiledb.Attr(name="east1", dtype=np.float64),
    tiledb.Attr(name="north1", dtype=np.float64),
    tiledb.Attr(name="up1", dtype=np.float64),
    attribute_dict["east_std0"],
    attribute_dict["north_std0"],
    attribute_dict["up_std0"],
    attribute_dict["east_std1"],
    attribute_dict["north_std1"],
    attribute_dict["up_std1"],
    attribute_dict["returnTime"],
    attribute_dict["tt"],
    attribute_dict["dbv"],
    attribute_dict["xc"],
    attribute_dict["snr"],
    attribute_dict["tat"],
    attribute_dict["isUpdated"],
]

ShotDataArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(
        tiledb.Dim(name="pingTime", dtype="datetime64[ns]"), TransponderDomain
    ),
    attrs=ShotDataAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

AcousticDataAttributes = [
    attribute_dict["returnTime"],
    attribute_dict["tt"],
    attribute_dict["dbv"],
    attribute_dict["xc"],
    attribute_dict["snr"],
    attribute_dict["tat"],
]

AcousticArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain, TransponderDomain),
    attrs=AcousticDataAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

# Create a configuration object
config = tiledb.Config()

# Set configuration parameters
config["vfs.s3.region"] = "us-east-2"
config["vfs.s3.scheme"] = "https"
config["vfs.s3.endpoint_override"] = ""
config["vfs.s3.use_virtual_addressing"] = "true"

# Support AWS profile or explicit credentials
# If AWS_PROFILE is set, use it; otherwise fall back to explicit credentials
aws_profile = os.environ.get("AWS_PROFILE", "")
if aws_profile:
    config["vfs.s3.aws_profile"] = aws_profile
else:
    # Fall back to explicit credentials for backward compatibility
    config["vfs.s3.aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID", "")
    config["vfs.s3.aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    config["vfs.s3.session_token"] = os.environ.get("AWS_SESSION_TOKEN", "")

ctx = tiledb.Ctx(config=config)

filters1 = tiledb.FilterList([tiledb.ZstdFilter(level=7)])
filters2 = tiledb.FilterList([tiledb.ByteShuffleFilter(), tiledb.ZstdFilter(level=7)])
filters3 = tiledb.FilterList(
    [tiledb.BitWidthReductionFilter(), tiledb.ZstdFilter(level=7)]
)
filters4 = tiledb.FilterList(
    [
        tiledb.FloatScaleFilter(0.0001, offset=None, bytewidth=8),
        tiledb.ZstdFilter(level=7),
    ]
)
filters5 = tiledb.FilterList(
    [
        tiledb.FloatScaleFilter(0.001, offset=None, bytewidth=4),
        tiledb.ZstdFilter(level=7),
    ]
)

roll_periods = {"1D": 43200000}
for roll_period, tile_value in roll_periods.items():

    # Dimensions
    # time - dimension w/ millisecond precision and 12 hour tiles
    # sys - system (e.g. 0:GPS, 1:GLONASS, 2:SBAS, ...)
    # sat - satellite PRN or slot number
    # obs - GNSS observation type (e.g. 1C, 2X, 5Q, ...) mapped to integer
    d0 = tiledb.Dim(
        name="time",
        domain=(315964800000, 4102444800000),
        tile=tile_value,
        dtype=np.int64,
        filters=filters1,
    )
    d1 = tiledb.Dim(
        name="sys", domain=(0, 254), tile=1, dtype=np.uint8, filters=filters1
    )
    d2 = tiledb.Dim(
        name="sat", domain=(0, 254), tile=1, dtype=np.uint8, filters=filters1
    )
    d3 = tiledb.Dim(
        name="obs", domain=(0, 65534), tile=1, dtype=np.uint16, filters=filters1
    )

    # Attributes
    # range - pseudorange measurement in meters
    # phase - phase measurement in cycles
    # doppler - doppler measurement in Hz
    # snr - signal to noise in dB-Hz
    # slip - slip counter or locktime
    # flags - bit 0: LLI; bit 1: half-cycle; bit 2: BOC-tracking
    # fcn - GLONASS Frequency Channel Number (FCN); reserved for other systems
    a0 = tiledb.Attr(name="range", dtype=np.float64, filters=filters4)
    a1 = tiledb.Attr(name="phase", dtype=np.float64, filters=filters4)
    a2 = tiledb.Attr(name="doppler", dtype=np.float64, filters=filters4)
    a3 = tiledb.Attr(name="snr", dtype=np.float32, filters=filters5)
    a4 = tiledb.Attr(name="slip", dtype=np.uint16, filters=filters3)
    a5 = tiledb.Attr(name="flags", dtype=np.uint16, filters=filters3)
    a6 = tiledb.Attr(name="fcn", dtype=np.int8, filters=filters1)

    dom = tiledb.Domain(d0, d1, d2, d3)

    offsets_filters = tiledb.FilterList(
        [
            tiledb.PositiveDeltaFilter(),
            tiledb.BitWidthReductionFilter(),
            tiledb.ZstdFilter(level=7),
        ]
    )

    GNSSObsSchema = tiledb.ArraySchema(
        domain=dom,
        sparse=True,
        attrs=[a0, a1, a2, a3, a4, a5, a6],
        cell_order="row-major",
        tile_order="row-major",
        capacity=500000,
        offsets_filters=offsets_filters,
        ctx=ctx,
    )


class TBDArray:
    """
    A base class for interacting with a TileDB array.

    This class provides common functionality for creating, reading, and writing
    pandas DataFrames to and from a TileDB array. It is intended to be subclassed
    for specific data types and schemas.

    Attributes:
        dataframe_schema: A pandera schema for validating DataFrames.
        array_schema: A tiledb.ArraySchema for creating the array.
        name (str): A human-readable name for the array type.
        uri (str): The URI of the TileDB array.
    """

    dataframe_schema = None
    array_schema = None
    name = "TBD Array"

    def __init__(self, uri: Path | S3Path | str):
        """
        Initializes the TBDArray object and creates the TileDB array if it
        does not already exist.

        Args:
            uri (Path | S3Path | str): The URI of the TileDB array. Can be a
                local path or an S3 path.
        """
        if "s3" in str(uri) and "s3://" not in str(uri):
            uri = str(uri).replace("s3:/", "s3://")  # temp fix
        self.uri = uri
        if not tiledb.array_exists(uri=str(uri), ctx=ctx):
            tiledb.Array.create(uri=str(uri), schema=self.array_schema, ctx=ctx)

    def write_df(self, df: pd.DataFrame, validate: bool = True):
        """
        Write a pandas DataFrame to the array.

        The DataFrame is validated against the class's `dataframe_schema`
        before being written.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            validate (bool, optional): Whether to validate the DataFrame.
                Defaults to True.
        """
        logger.logdebug(f" Writing dataframe to {self.uri}")
        if validate:
            df_val = self.dataframe_schema.validate(df, lazy=True)
        tiledb.from_pandas(str(self.uri), df_val, mode="append")

    def read_df(
        self,
        start: datetime.datetime | np.datetime64,
        end: datetime.datetime | np.datetime64 = None,
        validate: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Read a DataFrame from the array between a start and end date.

        Args:
            start (datetime.datetime | np.datetime64): The start date for the
                data slice.
            end (datetime.datetime | np.datetime64, optional): The end date for
                the data slice. If None, defaults to one day after start.
                Defaults to None.
            validate (bool, optional): Whether to validate the returned
                DataFrame. Defaults to True.

        Returns:
            pd.DataFrame: A DataFrame containing the data for the specified
            date range. Returns an empty DataFrame if no data is found or
            on error.
        """
        if isinstance(start, np.datetime64):
            start = start.astype(datetime.datetime)
        if isinstance(end, np.datetime64):
            end = end.astype(datetime.datetime)
        if isinstance(start, datetime.date):
            start = datetime.datetime.combine(start, datetime.datetime.min.time())
        if isinstance(end, datetime.date):
            end = datetime.datetime.combine(end, datetime.datetime.min.time())
        logger.logdebug(f" Reading dataframe from {self.uri}")
        # TODO slice array by start and end and return the dataframe
        if end is None:
            end = start + datetime.timedelta(days=1)
        else:
            end = end + datetime.timedelta(days=1)
        with tiledb.open(str(self.uri), mode="r") as array:
            try:
                df = array.df[slice(np.datetime64(start), np.datetime64(end))]
            except IndexError as e:
                logger.logerr(e)
                return pd.DataFrame()  # Return empty df on error
        if df.empty:
            logger.logwarn("Dataframe is empty")
            return pd.DataFrame()
        if validate:
            df = self.dataframe_schema.validate(df, lazy=True)
        return df

    def get_unique_dates(self, field: str) -> np.ndarray:
        """
        Gets the unique dates from a specified datetime field in the array.

        Args:
            field (str): The name of the datetime field to query.

        Returns:
            np.ndarray: An array of unique dates, or None if an error occurs.
        """
        with tiledb.open(str(self.uri), mode="r") as array:
            values = array[:][field]
            try:
                values = values.astype("datetime64[D]")
                return np.unique(values)
            except Exception as e:
                logger.logerr(e)
                return None

    def consolidate(self):
        """
        Consolidates and vacuums the TileDB array to improve performance.
        """
        ctx = tiledb.Ctx()
        config = tiledb.Config()
        config["sm.consolidation.steps"] = 3
        uri = tiledb.consolidate(uri=str(self.uri), ctx=ctx, config=config)
        logger.logdebug(f" Consolidated {self.name} to {uri}")
        tiledb.vacuum(str(self.uri))

    def view(self, network: str = "", station: str = ""):
        """
        Generates a plot showing the dates for which data is available.

        Args:
            network (str, optional): Network name to display in the title.
                Defaults to "".
            station (str, optional): Station name to display in the title.
                Defaults to "".

        Raises:
            ValueError: If no data is found in the array.
        """
        dates = self.get_unique_dates()
        if dates is None or dates.shape[0] == 0:
            raise ValueError("No dates found in the array")
        # Plot the values, with a marker seperating the dates
        fig, ax = plt.subplots()
        date_tick_map = {i: date for i, date in enumerate(dates)}

        for i, date in enumerate(dates):
            ax.axvline(x=i + 1, color="black", linestyle="-")

        ax.xaxis.set_ticks(
            [i + 1 for i in date_tick_map.keys()],
            [str(date) for date in date_tick_map.values()],
        )
        ax.yaxis.set_ticks([])
        ax.set_xlabel("Date")
        ax.set_ylabel("Found Data")
        fig.suptitle(f"Found {self.name} Dates For {network} {station}")
        plt.show()


class TDBAcousticArray(TBDArray):
    """Handles TileDB storage for acoustic ranging data."""

    dataframe_schema = AcousticDataFrame
    array_schema = AcousticArraySchema

    def __init__(self, uri: Path | S3Path | str):
        super().__init__(uri)

    def get_unique_dates(self, field="triggerTime") -> np.ndarray:
        """Gets unique dates from the 'triggerTime' field."""
        return super().get_unique_dates(field)

    def write_df(self, df: pd.DataFrame):
        """Writes an acoustic data DataFrame to the array."""
        df = self.dataframe_schema.validate(df, lazy=True)
        tiledb.from_pandas(str(self.uri), df, mode="append")

    def read_df(self, start: datetime, end: datetime = None, **kwargs) -> pd.DataFrame:
        """Reads acoustic data for a given time range."""
        if isinstance(start, datetime.date):
            start = datetime.datetime.combine(start, datetime.datetime.min.time())
        if end is None:
            end = start
        with tiledb.open(str(self.uri), mode="r") as array:
            df = array.df[slice(np.datetime64(start), np.datetime64(end)), :]
        df = self.dataframe_schema.validate(df, lazy=True)
        return df


class TDBKinPositionArray(TBDArray):
    """Handles TileDB storage for kinematic GNSS position data."""

    dataframe_schema = KinPositionDataFrame
    array_schema = KinPositionArraySchema
    name = "Kin Position Data"

    def __init__(self, uri: Path | S3Path | str):
        super().__init__(uri)

    def get_unique_dates(self, field="time") -> np.ndarray:
        """Gets unique dates from the 'time' field."""
        return super().get_unique_dates(field)


class TDBIMUPositionArray(TBDArray):
    """Handles TileDB storage for IMU position and orientation data."""

    dataframe_schema = IMUPositionDataFrame
    array_schema = IMUPositionArraySchema

    def __init__(self, uri: Path | S3Path | str):
        super().__init__(uri)

    def get_unique_dates(self, field="time") -> np.ndarray:
        """Gets unique dates from the 'time' field."""
        return super().get_unique_dates(field)


class TDBShotDataArray(TBDArray):
    """Handles TileDB storage for processed shot data."""

    dataframe_schema = ShotDataFrame
    array_schema = ShotDataArraySchema
    name = "Shot Data"

    def __init__(self, uri: Path | S3Path | str):
        super().__init__(uri)

    def get_unique_dates(self, field="pingTime") -> np.ndarray:
        """Gets unique dates from the 'pingTime' field."""
        return super().get_unique_dates(field)

    def read_df(self, start: datetime, end: datetime = None, **kwargs) -> pd.DataFrame:
        """
        Read a DataFrame from the array between the start and end dates.

        Args:
            start (datetime.datetime): The start date.
            end (datetime.datetime, optional): The end date. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame of shot data, or None on error.
        """
        if isinstance(start, datetime.date):
            start = datetime.datetime.combine(start, datetime.datetime.min.time())

        logger.logdebug(f" Reading dataframe from {self.uri} for {start} to {end}")
        # TODO slice array by start and end and return the dataframe
        if end is None:
            end = start + datetime.timedelta(
                hours=23, minutes=59, seconds=59, milliseconds=999
            )
        with tiledb.open(str(self.uri), mode="r") as array:
            try:
                df = array.df[slice(np.datetime64(start), np.datetime64(end)), :]
                if df.empty:
                    return df  # skip if the dataframe is empty
            except IndexError as e:
                logger.logerr(e)
                return None
        df.pingTime = df.pingTime.apply(lambda x: x.timestamp())
        df.returnTime = df.returnTime.apply(lambda x: x.timestamp())
        df = self.dataframe_schema.validate(df, lazy=True)
        return df

    def write_df(self, df: pd.DataFrame, validate: bool = True):
        """
        Write a shot data DataFrame to the array.

        Handles conversion of timestamp columns from float or datetime objects
        to the required nanosecond-precision numpy datetime64 format.

        Args:
            df (pd.DataFrame): The dataframe to write.
            validate (bool, optional): Whether to validate the dataframe.
                Defaults to True.
        """
        logger.logdebug(f" Writing dataframe to {self.uri}")
        if validate:
            df_val = self.dataframe_schema.validate(df, lazy=True)
        else:
            df_val = df
        if df_val.empty:
            logger.logwarn(f"Dataframe is empty, not writing to {self.uri}")
            return
        if isinstance(df_val.pingTime.iloc[0], float):
            # Convert pingTime and returnTime to datetime64[ns] if they are floats
            df_val.pingTime = df_val.pingTime.apply(
                lambda x: np.datetime64(int(x * 1e9), "ns")
            )
            df_val.returnTime = df_val.returnTime.apply(
                lambda x: np.datetime64(int(x * 1e9), "ns")
            )
        else:
            # Convert pingTime and returnTime to datetime64[ns] if they are datetime objects
            df_val.pingTime = df_val.pingTime.apply(lambda x: np.datetime64(x, "ns"))
            df_val.returnTime = df_val.returnTime.apply(
                lambda x: np.datetime64(x, "ns")
            )

        tiledb.from_pandas(str(self.uri), df_val, mode="append")


class TDBGNSSObsArray(TBDArray):
    """Handles TileDB storage for GNSS observation data."""

    array_schema = GNSSObsSchema

    def __init__(self, uri: Path | S3Path | str):
        super().__init__(uri)

    def get_unique_dates(self, field: str = "time") -> np.ndarray:
        """
        Gets unique dates from a specified datetime field in the array.

        Args:
            field (str, optional): The name of the datetime field to query.
                Defaults to "time".

        Returns:
            np.ndarray: An array of unique dates, or None if an error occurs.
        """
        with tiledb.open(str(self.uri), mode="r") as array:
            values = array[:][field]
            try:
                values = values.astype("datetime64[ms]")
                return np.unique(values)
            except Exception as e:
                logger.logerr(e)
                return None