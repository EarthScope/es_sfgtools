"""
Author: Franklyn Dunbar
Date: 2024-09-25
Email: franklyn.dunbar@earthscope.org
"""
# External imports
import pandas as pd
import pandera as pa
from pandera.typing import Series
from typing import Optional

# Local imports
from .constants import (
    GNSS_START_TIME,
    LEAP_SECONDS,
)


class AcousticDataFrame(pa.DataFrameModel):
    """Handles the parsing and validation of acoustic data from a file.
    Attributes:
        TransponderID (Series[str]): Unique identifier for the transponder.
        TriggerTime (Series[datetime]): Time when the ping was triggered.
        PingTime (Series[float]): Time when ping was send in seconds of day .
        ReturnTime (Series[float]): Return time in seconds since the start of day.
        tt (Series[float]): Two-way travel time.
        dbv (Series[int]): Signal relative to full scale voltage.
        CorrelationScore (Series[int]): Correlation score.

    """

    transponderID: Series[str] = pa.Field(
        description="Unique identifier for the transponder", coerce=True
    )

    pingTime: Series[float] = pa.Field(
        ge=GNSS_START_TIME.timestamp() - LEAP_SECONDS,
        coerce=True,
        description="Time when ping was sent in GPS time [seconds]",
    )

    returnTime: Series[float] = pa.Field(
        ge=GNSS_START_TIME.timestamp() - LEAP_SECONDS,
 
        coerce=True,
        description="Return time in GPS time [seconds]",
    )

    tt: Series[float] = pa.Field(
        ge=0.0, le=600, coerce=True, description="Two-way travel time [s]"
    )

    dbv: Series[int] = pa.Field(
        # ge=-100,
        # le=100,
        description="Signal relative to full scale voltage [dB]",
        coerce=True,
    )

    xc: Series[int] = pa.Field(
        ge=0, le=100, coerce=True, description="Correlation score"
    )
    snr: Series[float] = pa.Field(
        ge=-100,
        le=100.0,
        coerce=True,
        default=0,
        nullable=True,
        description="Signal to noise ratio",
    )

    tat: Series[float] = pa.Field(
        ge=0,
        le=10,
        coerce=True,
        description="Turn around time [s]",
        default=0,
        nullable=True,
    )

    class Config:
        coerce = True
        add_missing_columns = True
        #drop_invalid_rows = True
        # check_travel_time = {
        #     "TT": "tt",
        #     "ST": "pingTime",
        #     "RT": "returnTime",
        # }

class KinPositionDataFrame(pa.DataFrameModel):
    """
    Data frame Schema for GNSS Position Data
    """

    time: Series[pd.Timestamp] = pa.Field(
        ge=GNSS_START_TIME.replace(tzinfo=None),
        coerce=True,
        description="Timestamp of the measurement in millisecond precision (UTC) [Y-M-D-H-M-S]",
    )
    east: Series[float] = pa.Field(
        ge=-6378100, le=6378100, coerce=True, description="ECEF X coordinate [m]"
    )
    north: Series[float] = pa.Field(
        ge=-6378100, le=6378100, coerce=True, description="ECEF Y coordinate [m]"
    )
    up: Series[float] = pa.Field(
        ge=-6378100, le=6378100, coerce=True, description="ECEF Z coordinate [m]"
    )
    latitude: Series[float] = pa.Field(
        ge=-90,
        le=90,
        coerce=True,
        description="Latitude from the GNSS receiver (WGS84) [degrees]",
    )
    longitude: Series[float] = pa.Field(
        ge=-180,
        le=360,
        coerce=True,
        description="Longitude from the GNSS receiver (WGS84) [degrees]",
    )
    height: Series[float] = pa.Field(
        ge=-101,
        le=100,
        coerce=True,  # todo unsure of the range, ex. mountain lake
        description="Ellipsoidal Height (WGS84) [m]",
    )
    number_of_satellites: Series[int] = pa.Field(
        ge=0,
        le=125,
        coerce=True,  # todo unsure of the range, there are 125 GNSS satellites but obviously not all are visible
        description="Average number of satellites used in the position solution",
    )
    pdop: Series[float] = pa.Field(
        ge=0,
        le=1000,
        coerce=True,  # todo unsure of the full range, below 4 is great, 4-8 acceptable, above 8 is poor (should we throw these out?)
        description="Position Dilution of Precision",
    )
    wrms: Series[float] = pa.Field(
        ge=0,
        le=1000,
        coerce=True,  # todo unsure of the full range
        description="Weighted Root Mean Square of the position solution",
        nullable=True,
    )
    east_std: Optional[Series[float]] = pa.Field(
        nullable=True,
        description="Standard deviation of the ECEF X coordinate [m]",
    )
    north_std: Optional[Series[float]] = pa.Field(
        nullable=True,
        description="Standard deviation of the ECEF Y coordinate [m]",
    )
    up_std: Optional[Series[float]] = pa.Field(
        nullable=True,
        description="Standard deviation of the ECEF Z coordinate [m]",
    )

    @pa.parser("time")
    def parse_time(cls, series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, unit="ms")
    
    class Config:
        coerce = True
        add_missing_columns = True

class IMUPositionDataFrame(pa.DataFrameModel):
    time: Series[pd.Timestamp] = pa.Field(
        ge=GNSS_START_TIME.replace(tzinfo=None),
        coerce=True,
        description="Timestamp of the measurement in millisecond precision (UTC) [Y-M-D-H-M-S]",
    )
    head: Series[float] = pa.Field(
        ge=-180,
        le=180,
        coerce=True,
        description="Heading of the vessel at the time of the ping",
    )
    pitch: Series[float] = pa.Field(
        ge=-90,
        le=90,
        coerce=True,
        description="Pitch of the vessel at the time of the ping",
    )
    roll: Series[float] = pa.Field(
        ge=-180,
        le=180,
        coerce=True,
        description="Roll of the vessel at the time of the ping",
    )
    east: Series[float] = pa.Field(
        ge=-6378100,
        le=6378100,
        coerce=True,
        description="ECEF East position of the vessel at the time of the ping [m]",
    )
    north: Series[float] = pa.Field(
        ge=-6378100,
        le=6378100,
        coerce=True,
        description="ECEF North position of the vessel at the time of the ping [m]",
    )
    up: Series[float] = pa.Field(
        ge=-6378100,
        le=6378100,
        coerce=True,
        description="Height above ellipsoid of the vessel at the time of the ping",
    )
    east_std: Series[float] = pa.Field(
        nullable=True,
        description="Standard deviation of the ECEF East position of the vessel at the time of the ping [m]",
    )
    north_std: Series[float] = pa.Field(
        nullable=True,
        description="Standard deviation of the ECEF North position of the vessel at the time of the ping [m]",
    )
    up_std: Series[float] = pa.Field(
        nullable=True,
        description="Standard deviation of the height above ellipsoid of the vessel at the time of the ping",
    )
    latitude: Series[float] = pa.Field(
        ge=-90,
        le=90,
        coerce=True,
        description="Latitude from the GNSS receiver (WGS84) [degrees]",
    )
    longitude: Series[float] = pa.Field(
        ge=-180,
        le=360,
        coerce=True,
        description="Longitude from the GNSS receiver (WGS84) [degrees]",
    )

    class Config:
        coerce = True
        add_missing_columns = True
        drop_invalid_rows = True

    @pa.parser("time")
    def parse_time(cls, series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, unit="ms")

class ShotDataFrame(AcousticDataFrame):
    head0: Series[float] = pa.Field(
        description="Heading of the vessel at the time of the ping")
    pitch0: Series[float] = pa.Field(
        description="Pitch of the vessel at the time of the ping")
    roll0: Series[float] = pa.Field(
        description="Roll of the vessel at the time of the ping")
    east0: Series[float] = pa.Field(
        description="ECEF East position of the vessel at the time of the ping [m]")
    north0: Series[float] = pa.Field(
        description="ECEF North position of the vessel at the time of the ping [m]")
    up0: Series[float] = pa.Field(
        description="Height above ellipsoid of the vessel at the time of the ping")
    head1: Series[float] = pa.Field(
        description="Heading of the vessel at the time of the ping")
    pitch1: Series[float] = pa.Field(
        description="Pitch of the vessel at the time of the ping")
    roll1: Series[float] = pa.Field(
        description="Roll of the vessel at the time of the ping")
    east1: Series[float] = pa.Field(
        description="ECEF East position of the vessel at the time of the ping [m]")
    north1: Series[float] = pa.Field(
        description="ECEF North position of the vessel at the time of the ping [m]")
    up1: Series[float] = pa.Field(
        description="Height above ellipsoid of the vessel at the time of the ping")
    east_std0: Optional[Series[float]] = pa.Field(
        description="Standard deviation of the ECEF East position of the vessel at the time of the ping [m]",
        nullable=True,
    )
    north_std0: Optional[Series[float]] = pa.Field(
        description="Standard deviation of the ECEF North position of the vessel at the time of the ping [m]",
        nullable=True,
    )
    up_std0: Optional[Series[float]] = pa.Field(
        description="Standard deviation of the height above ellipsoid of the vessel at the time of the ping",
        nullable=True,
    )
    east_std1: Optional[Series[float]] = pa.Field(
        description="Standard deviation of the ECEF East position of the vessel at the time of the reply [m]",
        nullable=True,
    )
    north_std1: Optional[Series[float]] = pa.Field(
        description="Standard deviation of the ECEF North position of the vessel at the time of the reply [m]",
        nullable=True,
    )
    up_std1: Optional[Series[float]] = pa.Field(
        description="Standard deviation of the height above ellipsoid of the vessel at the time of the reply",
        nullable=True,
    )

    class Config:
        add_missing_columns = True
        coerce = True
        

class SoundVelocityDataFrame(pa.DataFrameModel):

    depth: Series[float] = pa.Field(
        ge=0, le=10000, description="Depth of the speed [m]", coerce=True
    )
    speed: Series[float] = pa.Field(unique=True,
        ge=0, le=3800, description="Spee of sound [m/s]", coerce=True
    )

    class Config:
        coerce = True
        drop_invalid_rows = True