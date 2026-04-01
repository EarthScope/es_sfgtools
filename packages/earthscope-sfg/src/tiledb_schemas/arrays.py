"""
TileDB array base class and typed subclasses for seafloor geodesy data.
"""

import datetime
import logging
import tempfile
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tiledb
from cloudpathlib import S3Path

from es_sfgtools.novatel_tools.rangea_parser import GNSSEpoch
from es_sfgtools.novatel_tools import novatel_ascii_operations as nova_ops
from es_sfgtools.data_models.observables import (
    AcousticDataFrame,
    IMUPositionDataFrame,
    KinPositionDataFrame,
    ShotDataFrame,
)
from es_sfgtools.tiledb_schemas.schemas import (
    AcousticArraySchema,
    GNSSObsSchema,
    IMUPositionArraySchema,
    KinPositionArraySchema,
    ShotDataArraySchema,
    config,
    ctx,
)

logger = logging.getLogger(__name__)


def as_py_datetime_object_col(s: pd.Series) -> pd.Series:
    """
    Convert a pandas Series of datetime-like objects to Python datetime objects.

    Parameters
    ----------
        s (pd.Series): A pandas Series containing datetime-like objects.

    Returns
    -------
        pd.Series: A pandas Series with Python datetime objects.
    """
    dt = pd.to_datetime(
        s, errors="coerce", utc=True
    )
    py = np.array(dt.dt.to_pydatetime())
    return pd.Series(py, index=s.index, dtype=object)


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
        logger.debug(f"Writing dataframe to {self.uri}")
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
        if end is None:
            end = start + datetime.timedelta(days=1)

        if isinstance(start, datetime.date) and not isinstance(
            start, datetime.datetime
        ):
            start = datetime.datetime.combine(start, datetime.datetime.min.time())
        if isinstance(end, datetime.date) and not isinstance(end, datetime.datetime):
            end = datetime.datetime.combine(end, datetime.datetime.max.time())

        logger.debug(f"Reading dataframe from {self.uri}")
        start = start.replace(tzinfo=datetime.timezone.utc)
        end = end.replace(tzinfo=datetime.timezone.utc)

        with tiledb.open(str(self.uri), mode="r") as array:
            try:
                df = array.df[slice(np.datetime64(start), np.datetime64(end))]
            except IndexError as e:
                logger.error(str(e))
                return pd.DataFrame()
        if df.empty:
            logger.warning("Dataframe is empty")
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
                logger.error(str(e))
                return None

    def consolidate(self):
        """
        Consolidates and vacuums the TileDB array to improve performance.
        """
        ctx = tiledb.Ctx()
        config = tiledb.Config()
        config["sm.consolidation.steps"] = 3
        uri = tiledb.consolidate(uri=str(self.uri), ctx=ctx, config=config)
        logger.debug(f"Consolidated {self.name} to {uri}")
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
        if isinstance(start, datetime.date) and not isinstance(
            start, datetime.datetime
        ):
            start = datetime.datetime.combine(start, datetime.datetime.min.time())

        logger.debug(f"Reading dataframe from {self.uri} for {start} to {end}")
        if end is None:
            end = datetime.datetime.combine(start.date(), datetime.datetime.max.time())
        start = start.replace(tzinfo=datetime.timezone.utc)
        end = end.replace(tzinfo=datetime.timezone.utc)

        with tiledb.open(str(self.uri), mode="r") as array:
            try:
                df = array.df[slice(np.datetime64(start), np.datetime64(end)), :]
                if df.empty:
                    return df
            except IndexError as e:
                logger.error(str(e))
                return None
        df.pingTime = as_py_datetime_object_col(df.pingTime)
        df.returnTime = as_py_datetime_object_col(df.returnTime)

        assert df.pingTime[0] >= start, "pingTime start date mismatch"
        assert df.pingTime.iloc[-1] <= end, "pingTime end date mismatch"

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
        if validate:
            df_val = self.dataframe_schema.validate(df, lazy=True)
        else:
            df_val = df
        if df_val.empty:
            logger.warning(f"Dataframe is empty, not writing to {self.uri}")
            return
        if isinstance(df_val.pingTime.iloc[0], float):
            df_val.pingTime = df_val.pingTime.apply(
                lambda x: np.datetime64(int(x * 1e9), "ns")
            )
            df_val.returnTime = df_val.returnTime.apply(
                lambda x: np.datetime64(int(x * 1e9), "ns")
            )
        else:
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
                logger.error(str(e))
                return None

    def write_epochs(self, epochs: List[GNSSEpoch], region: str = "us-east-2") -> int:
        """
        Write GNSS observation epochs to this TileDB array.

        This method is the Python equivalent of the Go WriteObsV3Array function.
        It flattens the hierarchical epoch/satellite/observation structure into
        columnar buffers and writes them to the TileDB array.

        Array Schema (dimensions):
            - time (int64): UTC timestamp in milliseconds since Unix epoch
            - sys (uint8): GNSS system identifier
            - sat (uint8): Satellite PRN/slot number
            - obs (uint16): Observation type code (2-char code as uint16)

        Array Schema (attributes):
            - range (float64): Pseudorange measurement in meters
            - phase (float64): Carrier phase in cycles
            - doppler (float64): Doppler frequency in Hz
            - snr (float32): Signal-to-noise ratio in dB-Hz
            - slip (uint16): Lock time / slip counter
            - flags (uint16): Observation flags
            - fcn (int8): GLONASS frequency channel number
        """

        d0_buffer, d1_buffer, d2_buffer, d3_buffer = [], [], [], []
        a0_buffer, a1_buffer, a2_buffer, a3_buffer, a4_buffer, a5_buffer, a6_buffer = (
            [],
            [],
            [],
            [],
            [],
            [],
            [],
        )
        for epoch in epochs:
            epoch_time_ms = int(epoch.time.timestamp() * 1000)
            for sat in epoch.satellites.values():
                sys_id = sat.system.value
                sat_id = sat.prn
                for obs in sat.observations.values():
                    obs_code = obs.signal_type
                    d0_buffer.append(epoch_time_ms)
                    d1_buffer.append(sys_id)
                    d2_buffer.append(sat_id)
                    d3_buffer.append(obs_code)
                    a0_buffer.append(obs.pseudorange)
                    a1_buffer.append(obs.carrier_phase)
                    a2_buffer.append(obs.doppler)
                    a3_buffer.append(obs.cn0)
                    a4_buffer.append(obs.locktime)
                    a5_buffer.append(obs.tracking_status)
                    a6_buffer.append(sat.fcn)

        d0_arr = np.array(d0_buffer, dtype=np.int64)
        d1_arr = np.array(d1_buffer, dtype=np.uint8)
        d2_arr = np.array(d2_buffer, dtype=np.uint8)
        d3_arr = np.array(d3_buffer, dtype=np.uint16)
        a0_arr = np.array(a0_buffer, dtype=np.float64)
        a1_arr = np.array(a1_buffer, dtype=np.float64)
        a2_arr = np.array(a2_buffer, dtype=np.float64)
        a3_arr = np.array(a3_buffer, dtype=np.float32)
        a4_arr = np.array(a4_buffer, dtype=np.uint16)
        a5_arr = np.array(a5_buffer, dtype=np.uint16)
        a6_arr = np.array(a6_buffer, dtype=np.int8)

        df = pd.DataFrame(
            {
                "time": d0_arr,
                "sys": d1_arr,
                "sat": d2_arr,
                "obs": d3_arr,
                "range": a0_arr,
                "phase": a1_arr,
                "doppler": a2_arr,
                "snr": a3_arr,
                "slip": a4_arr,
                "flags": a5_arr,
                "fcn": a6_arr,
            }
        ).drop_duplicates(
            subset=["time", "sys", "sat", "obs"], keep="first"
        )
        tiledb.from_pandas(str(self.uri), df, mode="append")
        return len(d0_buffer)

    def write_rangea_strings(
        self, rangea_strings: List[str], verbose: bool = False
    ) -> int:
        """
        Write GNSS observation epochs to this TileDB array from RINEX 3.05
        observation file lines.

        Parameters
        ----------
            rangea_strings (List[str])
                A list of strings
            verbose (bool, optional)
                Whether to print verbose output during processing. Defaults to False.
        """

        with tempfile.NamedTemporaryFile(mode="w+", delete=True) as tmp_file:
            for line in rangea_strings:
                tmp_file.write(line + "\n")
            tmp_file.flush()
            nova_ops.novatel_ascii_2tile(
                files=[tmp_file.name],
                gnss_obs_tdb=str(self.uri),
                n_procs=1,
                verbose=verbose,
            )
