from typing import Union, Optional,Tuple
import os
from es_sfgtools.data_models.metadata import SurveyType, classify_survey_type
from es_sfgtools.prefiltering.schemas import  FilterConfig
from es_sfgtools.config.env_config import ( 
    WorkingEnvironment, 
    WORKING_ENVIRONMENT,S3_SYNC_BUCKET,S3_SYNC_BUCKET_KEY,MAIN_DIRECTORY_GEOHUB,MAIN_DIRECTORY_GEOHUB_KEY)

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

def load_working_environment() -> Tuple[WorkingEnvironment, str]:
    """Load the current working environment from environment variables.

    Returns:
        WorkingEnvironment: The current working environment.
    """
    global WORKING_ENVIRONMENT
    global MAIN_DIRECTORY_GEOHUB
    env_str = os.environ.get("WORKING_ENVIRONMENT", "").lower()
   

    match env_str:
        case ("LOCAL", ""):
            WORKING_ENVIRONMENT = WorkingEnvironment.LOCAL
        case "GEOHUB":
            WORKING_ENVIRONMENT = WorkingEnvironment.GEOHUB
            if MAIN_DIRECTORY_GEOHUB is None:
                if (MAIN_DIRECTORY_GEOHUB := os.environ.get(f"{MAIN_DIRECTORY_GEOHUB_KEY}", None)) is None:
                    raise ValueError(
                        f"{MAIN_DIRECTORY_GEOHUB} environment variable must be set in GEOHUB environment."
                    )
                else:
                    return WORKING_ENVIRONMENT, MAIN_DIRECTORY_GEOHUB

        case _:
            raise ValueError(
                f"Unknown WORKING_ENVIRONMENT: {env_str}. "
                "Valid options are 'LOCAL' or 'GEOHUB'."
            )

    return WORKING_ENVIRONMENT, ""

def load_s3_sync_bucket() -> Optional[str]:
    """Load the S3 sync bucket from environment variables.

    Returns:
        Optional[str]: The S3 sync bucket name, or None if not set.
    """
    global S3_SYNC_BUCKET
    
   
    if (s3_bucket_str := os.environ.get(S3_SYNC_BUCKET_KEY, None)) is None:
        raise ValueError(
            f"Environment variable {S3_SYNC_BUCKET_KEY} is not set."
        )
    else:
        # Clean bucket name to ensure it meets S3 naming requirements
        s3_bucket_str = s3_bucket_str.strip().rstrip('/')
        
        # Validate bucket name format
        import re
        if not re.match(r'^[a-z0-9.\-]{3,63}$', s3_bucket_str):
            raise ValueError(
                f"Invalid S3 bucket name: '{s3_bucket_str}'. "
                f"Bucket names must be 3-63 characters, contain only lowercase "
                f"letters, numbers, hyphens, and periods, and not end with slashes."
            )
        
        S3_SYNC_BUCKET = s3_bucket_str

    return S3_SYNC_BUCKET

    