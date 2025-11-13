"""
This module provides functions for loading survey-specific configurations.

It determines which filter settings and GARPOS parameters to use based on the
type of survey being processed (e.g., CENTER, CIRCLE). It also provides
helpers for loading settings from the environment.
"""
import re
from typing import Optional, Tuple, Union

from es_sfgtools.config.env_config import S3_SYNC_BUCKET_KEY, Environment, WorkingEnvironment
from es_sfgtools.config.garpos_config import DEFAULT_SITE_CONFIG, GarposSiteConfig
from es_sfgtools.config.shotdata_filters import (
    CENTER_DRIVE_FILTER_CONFIG,
    CIRCLE_DRIVE_FILTER_CONFIG,
    DEFAULT_FILTER_CONFIG,
)
from es_sfgtools.data_models.metadata import SurveyType, classify_survey_type
from es_sfgtools.prefiltering.schemas import FilterConfig


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


def load_working_environment() -> Tuple[WorkingEnvironment, str]:
    """
    Load the current working environment from environment variables.

    Returns:
        A tuple containing the current WorkingEnvironment and the main
        directory for the GEOLAB environment (if applicable).
    """
    Environment.load_working_environment()
    return Environment.working_environment(), Environment.main_directory_GEOLAB()


def load_s3_sync_bucket() -> Optional[str]:
    """
    Load and validate the S3 sync bucket from environment variables.

    Returns:
        The S3 sync bucket name, or None if not set.

    Raises:
        ValueError: If the S3_SYNC_BUCKET environment variable is not set or
                    contains an invalid bucket name.
    """
    s3_bucket_str = Environment.s3_sync_bucket()
    if s3_bucket_str is None:
        raise ValueError(f"Environment variable {S3_SYNC_BUCKET_KEY} is not set.")

    # Clean bucket name to ensure it meets S3 naming requirements
    s3_bucket_str_noprefix = s3_bucket_str.strip().rstrip("/").replace("s3://", "")

    if not re.match(r"^[a-z0-9.\-]{3,63}$", s3_bucket_str_noprefix):
        raise ValueError(
            f"Invalid S3 bucket name: '{s3_bucket_str_noprefix}'. "
            f"Bucket names must be 3-63 characters, contain only lowercase "
            f"letters, numbers, hyphens, and periods, and not end with slashes."
        )

    return s3_bucket_str_noprefix