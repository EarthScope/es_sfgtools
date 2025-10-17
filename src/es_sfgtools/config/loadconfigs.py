from typing import Union, Optional
from es_sfgtools.data_models.metadata import SurveyType, classify_survey_type
from es_sfgtools.prefiltering.schemas import  FilterConfig

from es_sfgtools.config.shotdata_filters import (
    DEFAULT_FILTER_CONFIG,
    CENTER_DRIVE_FILTER_CONFIG,
    CIRCLE_DRIVE_FILTER_CONFIG,
)

from es_sfgtools.config.garpos_config import GarposSiteConfig,DEFAULT_SITE_CONFIG

def get_survey_filter_config(survey_type: Union[SurveyType, str]) -> FilterConfig:
    """Get the filter configuration based on the survey type.

    Args:
        survey_type (SurveyType): The type of the survey.

    Returns:
        FilterConfig: The filter configuration for the survey type.
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
    """Get the GARPOS site configuration based on the survey type.

    Args:
        survey_type (SurveyType): The type of the survey.

    Returns:
        GarposSiteConfig: The GARPOS site configuration for the survey type.
    """
    if isinstance(survey_type, str):
        survey_type: SurveyType = classify_survey_type(survey_type)

    match survey_type:
        case SurveyType.CENTER:
            return DEFAULT_SITE_CONFIG.model_copy()
        case _:
            return DEFAULT_SITE_CONFIG.model_copy()