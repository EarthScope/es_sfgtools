import pandera as pa
from pandera.typing import Series

class SoundVelocityDataFrame(pa.DataFrameModel):

    depth: Series[float] = pa.Field(
        ge=0, le=10000, description="Depth of the speed [m]", coerce=True
    )
    speed: Series[float] = pa.Field(
        ge=0, le=3800, description="Spee of sound [m/s]", coerce=True
    )
# TODO enfore unique speed and depth values