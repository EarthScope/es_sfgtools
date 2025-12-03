"""
This module manages environment-specific configurations.

It defines a class `Environment` that detects the current working environment
(e.g., LOCAL or GEOLAB) and loads relevant settings from environment variables.
"""
import os
import warnings
from enum import Enum


class WorkingEnvironment(Enum):
    """Enumeration for the possible working environments."""

    LOCAL = "LOCAL"
    GEOLAB = "GEOLAB"


WORKING_ENVIRONMENT_KEY = "WORKING_ENVIRONMENT"
S3_SYNC_BUCKET_KEY = "S3_SYNC_BUCKET"
MAIN_DIRECTORY_GEOLAB_KEY = "MAIN_DIRECTORY_GEOLAB"


class Environment:
    """
    A class to manage and provide access to environment-specific settings.

    This class should not be instantiated. It provides its functionality through
    class methods.
    """

    _working_environment: WorkingEnvironment = WorkingEnvironment.LOCAL
    _s3_sync_bucket: str | None = None
    _main_directory_GEOLAB: str | None = None

    @classmethod
    def working_environment(cls) -> WorkingEnvironment:
        """Returns the current working environment."""
        return cls._working_environment

    @classmethod
    def s3_sync_bucket(cls) -> str | None:
        """Returns the S3 sync bucket name, if configured."""
        return "s3://" + cls._s3_sync_bucket

    @classmethod
    def main_directory_GEOLAB(cls) -> str | None:
        """Returns the main directory for the GEOLAB environment, if configured."""
        return cls._main_directory_GEOLAB

    @classmethod
    def load_working_environment(cls) -> None:
        """
        Loads configuration from environment variables.

        This method checks for WORKING_ENVIRONMENT, S3_SYNC_BUCKET, and
        MAIN_DIRECTORY_GEOLAB and sets the class-level attributes accordingly.

        Raises:
            ValueError: If the environment is set to GEOLAB but the required
                        directory is not specified, or if an unknown environment
                        is specified.
        """
        env_str = os.environ.get(WORKING_ENVIRONMENT_KEY, "LOCAL").upper()

        match env_str:
            case "LOCAL":
                cls._working_environment = WorkingEnvironment.LOCAL
            case "GEOLAB":
                cls._working_environment = WorkingEnvironment.GEOLAB
                md_geolab = os.environ.get(MAIN_DIRECTORY_GEOLAB_KEY)
                if md_geolab is None:
                    raise ValueError(
                        f"{MAIN_DIRECTORY_GEOLAB_KEY} environment variable must be set in GEOLAB environment."
                    )
                cls._main_directory_GEOLAB = md_geolab
            case _:
                raise ValueError(
                    f"Unknown WORKING_ENVIRONMENT: {env_str}. "
                    "Valid options are 'LOCAL' or 'GEOLAB'."
                )

        s3_sync_bucket_str = os.environ.get(S3_SYNC_BUCKET_KEY)
        if s3_sync_bucket_str is None:
            warnings.warn(f"Environment variable {S3_SYNC_BUCKET_KEY} is not set.",stacklevel=2)
        else:
            cls._s3_sync_bucket = s3_sync_bucket_str

    @classmethod
    def load_aws_credentials(cls) -> None:
        """
        Loads AWS credentials from environment variables.

        This method checks for AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
        environment variables and sets them for use with AWS SDKs.

        Raises:
            Warning: If either of the required AWS credentials is not set.
        """
        
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_session_token = os.environ.get("AWS_SESSION_TOKEN")

        if aws_access_key is None or aws_secret_key is None or aws_session_token is None:
            warnings.warn(
                "AWS credentials are not fully set in environment variables. \n Please set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN.",
                stacklevel=2,
            )
        return aws_access_key, aws_secret_key, aws_session_token