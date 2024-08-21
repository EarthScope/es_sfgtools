from pydantic import BaseModel, Field, ValidationError
import pandera as pa
from pandera.typing import Series
from datetime import datetime
from typing import List, Optional
import pandas as pd

from .constants import GNSS_START_TIME

class PridePPP(BaseModel):
    """
    Data class for PPP GNSS kinematic position output
    Docs: https://github.com/PrideLab/PRIDE-PPPAR
    """

    modified_julian_date: float = Field(ge=0)
    second_of_day: float = Field(ge=0, le=86400)
    x: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF X coordinate
    y: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF Y coordinate
    z: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF Z coordinate
    latitude: float = Field(ge=-90, le=90)  # WGS84 latitude
    longitude: float = Field(ge=0, le=360)  # WGS84 longitude
    height: float = Field(ge=-101, le=100)  # WGS84 height (m)
    number_of_satellites: int = Field(
        default=1, ge=0, le=125
    )  # Average Number of available satellites
    pdop: float = Field(default=0, ge=0, le=20)  # Position Dilution of Precision
    time: Optional[datetime] = None

    class Config:
        coerce = True


class PositionDataFrame(pa.DataFrameModel):
    """
    Data frame Schema for GNSS Position Data
    """

    time: Series[pd.Timestamp] = pa.Field(
        ge=GNSS_START_TIME.replace(tzinfo=None),
        coerce=True,

        description="Timestamp of the measurement in millisecond precision (UTC) [Y-M-D-H-M-S]",
    )
    x: Series[float] = pa.Field(
        ge=-6378100, le=6378100, coerce=True, description="ECEF X coordinate [m]"
    )
    y: Series[float] = pa.Field(
        ge=-6378100, le=6378100, coerce=True, description="ECEF Y coordinate [m]"
    )
    z: Series[float] = pa.Field(
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
        le=20,
        coerce=True,  # todo unsure of the full range, below 4 is great, 4-8 acceptable, above 8 is poor (should we throw these out?)
        description="Position Dilution of Precision",
    )

    @pa.parser("time")
    def parse_time(cls, series: pd.Series) -> pd.Series:
        return pd.to_datetime(series,unit="ms")