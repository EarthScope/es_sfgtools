"""
This module provides functions for loading survey-specific configurations.

It determines which filter settings and GARPOS parameters to use based on the
type of survey being processed (e.g., CENTER, CIRCLE).
"""

from typing import Union

from .garpos_config import DEFAULT_SITE_CONFIG, GarposSiteConfig
from .shotdata_filters import (
    CENTER_DRIVE_FILTER_CONFIG,
    CIRCLE_DRIVE_FILTER_CONFIG,
    DEFAULT_FILTER_CONFIG,
)
from ..data_models.metadata import SurveyType, classify_survey_type
from ..prefiltering.schemas import FilterConfig


def get_survey_filter_config(survey_type: Union[SurveyType, str]) -> FilterConfig:
    """
    Get the filter configuration based on the survey type.

    Args:
        survey_type: The type of the survey.

    Returns:
        The filter configuration for the survey type.
    """
    if isinstance(survey_type, str):
        survey_type: SurveyType = classify_survey_type(survey_type)

    match survey_type:
        case SurveyType.CENTER:
            return CENTER_DRIVE_FILTER_CONFIG.model_copy()
        case SurveyType.CIRCLE:
            return CIRCLE_DRIVE_FILTER_CONFIG.model_copy()
        case _:
            return DEFAULT_FILTER_CONFIG.model_copy()


def get_garpos_site_config(survey_type: Union[SurveyType, str]) -> GarposSiteConfig:
    """
    Get the GARPOS site configuration based on the survey type.

    Args:
        survey_type: The type of the survey.

    Returns:
        The GARPOS site configuration for the survey type.
    """
    if isinstance(survey_type, str):
        survey_type: SurveyType = classify_survey_type(survey_type)

    match survey_type:
        case SurveyType.CENTER:
            return DEFAULT_SITE_CONFIG.model_copy()
        case _:
            return DEFAULT_SITE_CONFIG.model_copy()
