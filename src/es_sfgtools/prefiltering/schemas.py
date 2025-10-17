from pydantic import BaseModel, Field, ConfigDict, field_serializer
from typing import Any, Dict, Optional
from enum import Enum
from typing import Union

from es_sfgtools.data_models.metadata import SurveyType, classify_survey_type


class FilterLevel(str, Enum):
    GOOD = "GOOD"
    OK = "OK"
    DIFFICULT = "DIFFICULT"


class AcousticFilterConfig(BaseModel):
    enabled: bool = Field(
        True, description="Whether to enable acoustic diagnostics filtering"
    )
    level: FilterLevel = Field(
        FilterLevel.OK,
        description="Level of acoustic diagnostics to filter. Options: GOOD, OK, DIFFICULT",
    )

    @field_serializer("level")
    def serialize_level(level: FilterLevel) -> str:
        return level.value


class PingRepliesFilterConfig(BaseModel):
    enabled: bool = Field(True, description="Whether to enable ping replies filtering")
    min_replies: int = Field(
        2, description="Minimum number of replies required to keep a shot"
    )


class MaxDistFromCenterConfig(BaseModel):
    enabled: bool = Field(
        True, description="Whether to enable max distance from center filtering"
    )
    max_distance_m: float = Field(
        150.0,
        description="Maximum distance from the survey center in meters to keep a shot",
    )


class PrideResidualsConfig(BaseModel):
    enabled: bool = Field(
        True, description="Whether to enable PRIDE residuals filtering"
    )
    max_residual_mm: float = Field(
        5.0, description="Maximum PRIDE residual in millimeters to keep a shot"
    )


class FilterConfig(BaseModel):
    acoustic_filters: AcousticFilterConfig = Field(
        default_factory=AcousticFilterConfig,
        description="Configuration for acoustic diagnostics filtering",
    )
    ping_replies: PingRepliesFilterConfig = Field(
        default_factory=PingRepliesFilterConfig,
        description="Configuration for ping replies filtering",
    )
    max_distance_from_center: MaxDistFromCenterConfig = Field(
        default_factory=MaxDistFromCenterConfig,
        description="Configuration for max distance from center filtering",
    )
    pride_residuals: PrideResidualsConfig = Field(
        default_factory=PrideResidualsConfig,
        description="Configuration for PRIDE residuals filtering",
    )

    def update(self, custom_config: Dict[str, Any]) -> None:
        for key, value in custom_config.items():
            if hasattr(self, key):
                attr = getattr(self, key)
                if isinstance(attr, BaseModel) and isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        if hasattr(attr, sub_key):
                            setattr(attr, sub_key, sub_value)
                else:
                    setattr(self, key, value)
