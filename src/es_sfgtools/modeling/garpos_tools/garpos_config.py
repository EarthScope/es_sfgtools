"""
This module contains configuration settings for the GARPOS model.
"""
from pydantic import BaseModel, Field

from es_sfgtools.modeling.garpos_tools.schemas import InversionParams,GPPositionENU


DEFAULT_DPOS_VARIANCE = GPPositionENU(east_sigma=10, north_sigma=10, up_sigma=10)
DEFAULT_TRANSPONDER_VARIANCE = GPPositionENU(east_sigma=1, north_sigma=1, up_sigma=1)

CENTER_DRIVE_DPOS_VARIANCE = GPPositionENU(east_sigma=0, north_sigma=0, up_sigma=0)
CENTER_DRIVE_TRANSPONDER_VARIANCE = GPPositionENU(east_sigma=10, north_sigma=10, up_sigma=10)

class GarposSiteConfig(BaseModel):

    transponder_position_variance: GPPositionENU = Field(
        GPPositionENU(east_sigma=10, north_sigma=10, up_sigma=10),
        description="Variance to add to the transponder positions (in meters)."
    )
    inversion_params: InversionParams = Field(
        default_factory=lambda: InversionParams(
            delta_center_position=DEFAULT_DPOS_VARIANCE
        ),
        description="Inversion parameters for GARPOS.",
    )
   


DEFAULT_SITE_CONFIG = GarposSiteConfig()

CENTER_DRIVE_SITE_CONFIG = GarposSiteConfig(
    transponder_position_variance=GPPositionENU(east_sigma=1, north_sigma=1, up_sigma=1),
    inversion_params=InversionParams(
        delta_center_position=CENTER_DRIVE_DPOS_VARIANCE)
)