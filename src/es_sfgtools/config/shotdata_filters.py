from es_sfgtools.prefiltering.schemas import PingRepliesFilterConfig, FilterConfig,MaxDistFromCenterConfig


default_filter_config = {
    "pride_residuals": {"enabled": False, "max_residual_mm": 8},
    "max_distance_from_center": {"enabled": False, "max_distance_m": 150.0},
    "ping_replies": {"enabled": False, "min_replies": 3},
    "acoustic_filters": {"enabled": False, "level": "OK"}
}


DEFAULT_FILTER_CONFIG = FilterConfig(
    **default_filter_config
)

CENTER_DRIVE_FILTER_CONFIG = FilterConfig(
    ping_replies=PingRepliesFilterConfig(min_replies=1),
    max_distance_from_center=MaxDistFromCenterConfig(max_distance_m=150.0)
)

CIRCLE_DRIVE_FILTER_CONFIG = FilterConfig(
    ping_replies=PingRepliesFilterConfig(min_replies=1)
)
