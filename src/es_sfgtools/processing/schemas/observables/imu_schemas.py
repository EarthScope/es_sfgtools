import pandas as pd
from pydantic import BaseModel, Field, model_validator, ValidationError
import pandera as pa
from pandera.typing import Series
from datetime import datetime
from typing import List, Optional

from .constants import GNSS_START_TIME

class IMUDataFrame(pa.DataFrameModel):
    """Dataframe Schema for INS Position, Velocity, and Attitude (PVA) log data"""

    Time: Series[datetime] = pa.Field(
        ge=GNSS_START_TIME.replace(tzinfo=None),
        coerce=True,
        description="Timestamp of the measurement in millisecond precision (UTC) [Y-M-D-H-M-S]",
    )
    Roll: Series[float] = pa.Field(
        ge=-180,
        le=180,
        coerce=True,
        description="Right-handed rotation from local level around y‑axis in degrees",
    )
    Pitch: Series[float] = pa.Field(
        ge=-90,
        le=90,
        coerce=True,
        description="Right-handed rotation from local level around x‑axis in degrees",
    )
    Azimuth: Series[float] = pa.Field(
        ge=0,
        le=360,
        coerce=True,
        description="Left-handed rotation around z-axis in degrees clockwise from North. This is the inertial azimuth calculated from the IMU gyros and the SPAN filters.",
    )

