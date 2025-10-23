from enum import Enum
import os
import warnings

class WorkingEnvironment(Enum):
    LOCAL = "LOCAL"
    GEOLAB = "GEOLAB"


WORKING_ENVIRONMENT = WorkingEnvironment.LOCAL
WORKING_ENVIRONMENT_KEY = "WORKING_ENVIRONMENT"

S3_SYNC_BUCKET_KEY = "S3_SYNC_BUCKET"
S3_SYNC_BUCKET = None

MAIN_DIRECTORY_GEOLAB_KEY = "MAIN_DIRECTORY_GEOLAB"
MAIN_DIRECTORY_GEOLAB = None

class Environment:
    _working_environment: WorkingEnvironment = WorkingEnvironment.LOCAL
    _s3_sync_bucket: str | None = S3_SYNC_BUCKET
    _main_directory_GEOLAB: str | None = MAIN_DIRECTORY_GEOLAB

    @classmethod
    def working_environment(cls) -> WorkingEnvironment:
        return cls._working_environment

    @classmethod
    def s3_sync_bucket(cls) -> str | None:
        return cls._s3_sync_bucket

    @classmethod
    def main_directory_GEOLAB(cls) -> str | None:
        return cls._main_directory_GEOLAB
    
    @classmethod
    def load_working_environment(cls) -> None:

        env_str = os.environ.get("WORKING_ENVIRONMENT", None)
        if isinstance(env_str, str):
            env_str = env_str.upper()
        match env_str:
            case None:
                cls._working_environment = WorkingEnvironment.LOCAL
            case "LOCAL":
                cls._working_environment = WorkingEnvironment.LOCAL
            case "GEOLAB":
                cls._working_environment = WorkingEnvironment.GEOLAB
                if cls._main_directory_GEOLAB is None:
                    md_GEOLAB = os.environ.get(f"{MAIN_DIRECTORY_GEOLAB_KEY}", None)
                    if md_GEOLAB is None:
                        raise ValueError(
                            f"{MAIN_DIRECTORY_GEOLAB_KEY} environment variable must be set in GEOLAB environment."
                        )
                    else:
                        cls._main_directory_GEOLAB = md_GEOLAB
            case _:
                raise ValueError(
                    f"Unknown WORKING_ENVIRONMENT: {env_str}. "
                    "Valid options are 'LOCAL' or 'GEOLAB'."
                )

        s3_sync_bucket_str = os.environ.get(S3_SYNC_BUCKET_KEY, None)
        if s3_sync_bucket_str is None:
            warnings.warn(
                f"Environment variable {S3_SYNC_BUCKET_KEY} is not set."
            )
        else:
            cls._s3_sync_bucket = s3_sync_bucket_str