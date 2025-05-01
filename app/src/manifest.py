from pathlib import Path
from typing import List, Optional
import yaml
from enum import Enum
import json
from pydantic import BaseModel, Field, field_serializer, field_validator
from es_sfgtools.processing.pipeline.pipelines import SV3PipelineConfig
from rich import table

class PipelineJobType(str, Enum):
    PREPROCESSING = "preprocessing"
    INGESTION = "ingestion"
    DOWNLOAD = "download"

class PipelinePreprocessJob(BaseModel):
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    config: Optional[SV3PipelineConfig] = Field(..., title="Pipeline Configuration")

class PipelineIngestJob(BaseModel):
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    directory: Path = Field(..., title="Directory Data Path")

    class Config:
        arbitrary_types_allowed = True

    @field_serializer("directory")
    def _directory_s(cls, v: Path):
        return str(v)

    @field_validator("directory",mode="before")
    def _directory_v(cls, v: str):
        directory = Path(v.strip())
        if not directory.exists():
            raise ValueError(f"Directory {directory} does not exist")
        return directory


class ArchiveDownloadJob(BaseModel):
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")


class PipelineManifest(BaseModel):
    main_dir: Path = Field(..., title="Main Directory")
    ingestion_jobs: List[PipelineIngestJob] = Field(
        default=[], title="List of Pipeline Ingestion Jobs"
    )
    process_jobs: List[PipelinePreprocessJob] = Field(
        default=[], title="List of Pipeline Jobs"
    )
    download_jobs: Optional[List[ArchiveDownloadJob]] = Field(
        default=[], title="List of Archive Download Jobs"
    )
    global_config: SV3PipelineConfig = Field(..., title="Global Config")

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def from_json(cls, json_data:Path) -> 'PipelineManifest':
        """
        Instantiates a PipelineManifest object from a JSON schema.

        Args:
            json_data (dict): The JSON data to parse.

        Returns:
            PipelineManifest: An instance of the PipelineManifest class.
        """
        # Load JSON data
        with open(json_data, "r") as f:
            json_data = json.load(f)
        # Load global configuration
        global_config = SV3PipelineConfig(**json_data["globalConfig"])

        # Initialize lists for jobs
        process_jobs = []
        ingestion_jobs = []
        download_jobs = []

        # Parse operations
        for operation in json_data.get("operations", []):
            # Extract network, station, and campaign
            try:
                network = operation["network"]
                station = operation["station"]
                campaign = operation["campaign"]
            except KeyError as e:
                raise ValueError(f"Missing key in operations: {e}")     
                   
            for job in operation.get("jobs", []):
                # Validate job type
                try:
                    job_type = PipelineJobType(job["type"])
                except (KeyError,ValueError) as e:
                    raise ValueError(f"Invalid job type: {job['type']}") from e
                
                match job_type:
                    case PipelineJobType.INGESTION:
                        ingestion_jobs.append(
                            PipelineIngestJob(
                                network=network,
                                station=station,
                                campaign=campaign,
                                directory=job["directory"] # need to validate this
                            )
                        )
                    case PipelineJobType.PREPROCESSING:
                        # Merge job-specific config with global config
                        job_config = SV3PipelineConfig(**job["config"]) if "config" in job else global_config
                        job_config = global_config.model_copy(update=dict(job_config))
                        process_jobs.append(
                            PipelinePreprocessJob(
                                network=network,
                                station=station,
                                campaign=campaign,
                                config=job_config
                            )
                        )
                    case PipelineJobType.DOWNLOAD:
                        download_jobs.append(
                            ArchiveDownloadJob(
                                network=network,
                                station=station,
                                campaign=campaign
                            )
                        )

        # Instantiate the PipelineManifest
        return cls(
            main_dir=Path(json_data["projectDir"]),
            ingestion_jobs=ingestion_jobs,
            process_jobs=process_jobs,
            download_jobs=download_jobs,
            global_config=global_config
        )