import pandas as pd
import pandera as pa
from pandera.typing import Series
import pandera.extensions as paext
from typing import List, Dict
from .constants import (
    GNSS_START_TIME,
    GNSS_START_TIME_JULIAN,
    GNSS_START_TIME_JULIAN_BOUNDS,
    TRIGGER_DELAY_SV2,
    TRIGGER_DELAY_SV3,
    ADJ_LEAP,
)



class ShotDataFrame(pa.DataFrameModel):
    
    transponderID: Series[str] = pa.Field(
        description="Unique identifier for the transponder"
    )
    triggerTime: Series[pd.Timestamp] = pa.Field(
        ge=GNSS_START_TIME.replace(tzinfo=None),
        description="Time when the ping was triggered")
    pingTime: Series[float] = pa.Field(
        ge=0,le=3600*24, description="Time when ping was send in seconds of day [s]")
    returnTime: Series[float] = pa.Field(
        ge=0,le=3600*24, description="Return time in seconds since the start of day [s]")
    
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
    tt: Series[float] = pa.Field(
        description="Two-way travel time [s]")
    tat: Series[float] = pa.Field(ge=0.0, le=1, description="Turn around time [s]")
    xc: Series[float] = pa.Field(
        description="Cross-correlation coefficient")
    snr: Series[float] = pa.Field(
        description="Signal to noise ratio")
    east0_std: Series[float] = pa.Field(
        description="Standard deviation of the ECEF East position of the vessel at the time of the ping [m]",
        nullable=True)
    north0_std: Series[float] = pa.Field(
        description="Standard deviation of the ECEF North position of the vessel at the time of the ping [m]",
        nullable=True)
    up0_std: Series[float] = pa.Field(
        description="Standard deviation of the height above ellipsoid of the vessel at the time of the ping",
        nullable=True)
    east1_std: Series[float] = pa.Field(
        description="Standard deviation of the ECEF East position of the vessel at the time of the ping [m]",
        nullable=True)
    north1_std: Series[float] = pa.Field(
        description="Standard deviation of the ECEF North position of the vessel at the time of the ping [m]",
        nullable=True)
    up1_std: Series[float] = pa.Field(
        description="Standard deviation of the height above ellipsoid of the vessel at the time of the ping",
        nullable=True)
    
    class Config:
        add_missing_columns = True
        coerce=True
