from pathlib import Path
from typing import List, Optional
import yaml
from enum import Enum
import json
from pydantic import BaseModel, Field, field_serializer, field_validator
from es_sfgtools.processing.pipeline.pipelines import SV3PipelineConfig


class PipelineJobType(str, Enum):
    PROCESSING = "processing"
    INGESTION = "ingestion"
    DOWNLOAD = "download"

class PipelineProcessJob(BaseModel):
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    config: SV3PipelineConfig = Field(..., title="Pipeline Configuration")


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

    @field_validator("directory")
    def _directory_v(cls, v: str):
        return Path(v)


class ArchiveDownloadJob(BaseModel):
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")


class PipelineManifest(BaseModel):
    main_dir: Path = Field(..., title="Main Directory")
    ingestion_jobs: List[PipelineIngestJob] = Field(
        default=[], title="List of Pipeline Ingestion Jobs"
    )
    process_jobs: List[PipelineProcessJob] = Field(
        default=[], title="List of Pipeline Jobs"
    )
    download_jobs: Optional[List[ArchiveDownloadJob]] = Field(
        default=[], title="List of Archive Download Jobs"
    )
    global_config: SV3PipelineConfig = Field(..., title="Global Config")

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def from_yaml(cls, filepath: Path) -> "PipelineManifest":
        with open(filepath, "r") as f:
            data = yaml.safe_load(f)

        config_loaded = SV3PipelineConfig(**data["global_config"])
        process_jobs = []
        ingestion_jobs = []
        download_jobs = []

        for job in data.get("processing", {}).get("jobs", []):

            if hasattr(job, "config"):
                job_config = SV3PipelineConfig(**job["config"])
                # update config_loaded with job_config
                job_config = config_loaded.model_copy(update=dict(job_config))
            else:
                job_config = config_loaded
            if job_config.rinex_config.processing_year == -1:
                # Infer the campaign year from the campaign name using string splitting
                try:
                    year = int(job["campaign"].split("_")[0])
                    job_config.rinex_config.processing_year = year
                except (ValueError, IndexError):
                    raise ValueError(
                        f"Invalid campaign format: {job['campaign']}. Expected format: '<year>_<details>'."
                    )

            process_jobs.append(
                PipelineProcessJob(
                    network=job["network"],
                    station=job["station"],
                    campaign=job["campaign"],
                    config=job_config,
                )
            )
        for job in data.get("ingestion", {}).get("jobs", []):
            ingestion_jobs.append(PipelineIngestJob(**job))

        for job in data.get("download", {}).get("jobs", []):
            download_jobs.append(
                ArchiveDownloadJob(
                    network=job["network"],
                    station=job["station"],
                    campaign=job["campaign"],
                )
            )
        return cls(
            main_dir=Path(data["main_dir"]),
            process_jobs=process_jobs,
            ingestion_jobs=ingestion_jobs,
            download_jobs=download_jobs,
            global_config=config_loaded,
        )

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
            network = operation["network"]
            station = operation["station"]
            campaign = operation["campaign"]

            
            for job in operation.get("jobs", []):
                job_type = PipelineJobType(job["type"])
                
                match job_type:
                    case PipelineJobType.INGESTION:
                        ingestion_jobs.append(
                            PipelineIngestJob(
                                network=network,
                                station=station,
                                campaign=campaign,
                                directory=Path(job["directory"]) # need to validate this
                            )
                        )
                    case PipelineJobType.PROCESSING:
                        # Merge job-specific config with global config
                        job_config = SV3PipelineConfig(**job["config"]) if "config" in job else global_config
                        job_config = global_config.model_copy(update=dict(job_config))
                        process_jobs.append(
                            PipelineProcessJob(
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