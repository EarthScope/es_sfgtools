"""
This module contains default configuration settings for the GARPOS model,
including variance parameters for transponder and DPOS positions.
"""
from es_sfgtools.modeling.garpos_tools.schemas import GPPositionENU, InversionParams
from pydantic import BaseModel, Field

DEFAULT_DPOS_VARIANCE = GPPositionENU(east_sigma=10, north_sigma=10, up_sigma=10)
DEFAULT_TRANSPONDER_VARIANCE = GPPositionENU(east_sigma=0, north_sigma=0, up_sigma=0)


class GarposSiteConfig(BaseModel):
    """A Pydantic model for GARPOS site-specific configuration."""

    transponder_position_variance: GPPositionENU = Field(
        DEFAULT_TRANSPONDER_VARIANCE,
        description="Variance to add to the transponder positions (in meters).",
    )
    inversion_params: InversionParams = Field(
        default_factory=lambda: InversionParams(
            delta_center_position=DEFAULT_DPOS_VARIANCE
        ),
        description="Inversion parameters for GARPOS.",
    )


DEFAULT_SITE_CONFIG = GarposSiteConfig()