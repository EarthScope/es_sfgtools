"""
Author: Franklyn Dunbar
Date: 2024-03-07
Email: franklyn.dunbar@earthscope.org
"""
import pandas as pd
import pandera as pa
from pandera.typing import Series
from typing import List, Dict
from .constants import GNSS_START_TIME, GNSS_START_TIME_JULIAN, GNSS_START_TIME_JULIAN_BOUNDS, TRIGGER_DELAY_SV2, TRIGGER_DELAY_SV3, ADJ_LEAP


class AcousticDataFrame(pa.DataFrameModel):
    """Handles the parsing and validation of acoustic data from a file.
    Attributes:
        TransponderID (Series[str]): Unique identifier for the transponder.
        TriggerTime (Series[datetime]): Time when the ping was triggered.
        PingTime (Series[float]): Time when ping was received (modified Julian day).
        ReturnTime (Series[float]): Return time in seconds since the start of day (modified Julian day).
        TwoWayTravelTime (Series[float]): Two-way travel time.
        DecibalVoltage (Series[int]): Signal relative to full scale voltage.
        CorrelationScore (Series[int]): Correlation score.

    """

    TransponderID: Series[str] = pa.Field(
        description="Unique identifier for the transponder", coerce=True
    )

    TriggerTime: Series[pd.Timestamp] = pa.Field(
        ge=GNSS_START_TIME.replace(tzinfo=None),
        coerce=True,
        description="Time when the ping was triggered [datetime]",
    )

    PingTime: Series[float] = pa.Field(
        ge=0,
        le=3600*24,
        coerce=True,
        description="Time when ping was received in seconds of day [seconds]",
    )

    ReturnTime: Series[float] = pa.Field(
        ge=0,
        le=3600*24,
        coerce=True,
        description="Return time in seconds since the start of day (modified Julian day) [days]",
    )

    TwoWayTravelTime: Series[float] = pa.Field(
        ge=0.0, le=600, coerce=True, description="Two-way travel time [s]"
    )

    DecibalVoltage: Series[int] = pa.Field(
        ge=-100,
        le=100,
        description="Signal relative to full scale voltage [dB]",
        coerce=True,
    )

    CorrelationScore: Series[int] = pa.Field(
        ge=0, le=100, coerce=True, description="Correlation score"
    )
    SignalToNoise: Series[float] = pa.Field(
        ge=-100, le=100.0, coerce=True,default=0, nullable=True,description="Signal to noise ratio"
    )

    # TODO include snr - signal to noise ratio

    class Config:
        coerce = True
        add_missing_columns = True

    @pa.parser("TriggerTime")
    def parse_trigger_time(cls, series: pd.Series) -> pd.Series:
        return pd.to_datetime(series,unit='ms')
    
    @pa.parser("TwoWayTravelTime")
    def parse_two_way_travel_time(cls, series: pd.Series) -> pd.Series:
        return series.apply(lambda x: max(x, 0))