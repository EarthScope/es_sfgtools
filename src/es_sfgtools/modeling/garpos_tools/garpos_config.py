"""
This module contains configuration settings for the GARPOS model.
"""

from es_sfgtools.modeling.garpos_tools.schemas import InversionParams

DEFAULT_FILTER_CONFIG = {
    "acoustic_filters": {
        "enabled": True,
        "level": "OK",
    },
    "ping_replies": {
        "enabled": True,
        "min_replies": 3,
    },
    "max_distance_from_center": {
        "enabled": True,
        "max_distance_m": 1000,
    },
    "pride_residuals": {
        "enabled": True,
        "max_residual_mm": 50,
    },
}

DEFAULT_INVERSION_PARAMS = InversionParams()
