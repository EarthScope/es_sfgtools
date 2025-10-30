"""
This module defines the Pydantic models for parsing the pipeline manifest file.

These models provide data validation and a structured interface for accessing
manifest contents, which define the ingestion, download, and processing jobs
for the pipeline.
"""
import json
import os
from enum import Enum
from pathlib import Path
from typing import List, Optional

import yaml
from es_sfgtools.modeling.garpos_tools.schemas import InversionParams
from es_sfgtools.workflows.pipelines import SV3PipelineConfig
from es_sfgtools.utils.model_update import validate_and_merge_config
from pydantic import BaseModel, Field, field_serializer, field_validator


class PipelineJobType(str, Enum):
    """Enumeration for the different types of pipeline jobs."""

    PREPROCESSING = "preprocessing"
    INGESTION = "ingestion"
    DOWNLOAD = "download"
    GARPOS = "garpos"


class PreprocessJobType(str, Enum):
    """Enumeration for the different types of preprocessing jobs."""

    ALL = "all"
    RINEX = "build_rinex"
    PRIDE = "run_rinex_ppp"
    KINEMATIC = "process_kinematic"
    SVP = "process_svp"
    REFINE_SHOTDATA = "refine_shotdata"
    DFOP00 = "process_dfop00"
    NOVATEL = "process_novatel"


class PipelinePreprocessJob(BaseModel):
    """Defines a job for the preprocessing pipeline."""

    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    job_type: PreprocessJobType = Field(
        PreprocessJobType.ALL, title="Preprocessing Job Type"
    )
    config: Optional[SV3PipelineConfig] = Field(..., title="Pipeline Configuration")


class PipelineIngestJob(BaseModel):
    """Defines a job for ingesting local data."""

    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    directory: Path = Field(..., title="Directory Data Path")

    class Config:
        arbitrary_types_allowed = True

    @field_serializer("directory")
    def _directory_s(cls, v: Path):
        return str(v)

    @field_validator("directory", mode="before")
    def _directory_v(cls, v: str):
        directory = Path(v.strip())
        if not directory.exists():
            raise ValueError(f"Directory {directory} does not exist")
        return directory


class ArchiveDownloadJob(BaseModel):
    """Defines a job for downloading data from the archive."""

    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")


class GARPOSConfig(BaseModel):
    """Defines the configuration for a GARPOS processing run."""

    garpos_path: Optional[Path] = Field(
        default=None, title="GARPOS Path", description="Path to GARPOS repository"
    )
    iterations: Optional[int] = Field(
        2,
        title="Number of Iterations",
        description="Number of GARPOS inversion iterations to perform",
    )
    run_id: Optional[str] = Field(
        None,
        title="Run ID",
        description="Optional run ID for GARPOS processing",
        coerce_numbers_to_str=True,
    )
    override: Optional[bool] = Field(
        False,
        title="Override Existing Data",
        description="Whether to override existing data",
    )
    inversion_params: Optional[InversionParams] = Field(
        None,
        title="Inversion Parameters",
        description="Parameters for GARPOS inversion",
    )

    class Config:
        arbitrary_types_allowed = True
        coerce = True

    @field_serializer("garpos_path")
    def _garpos_path_s(v: Path):
        if v is None:
            return None
        return str(v)

    @field_validator("garpos_path", mode="before")
    def _garpos_path_v(cls, v: str):
        if v is None:
            return None
        garpos_path = Path(v.strip())
        if not garpos_path.exists():
            raise ValueError(f"GARPOS path {garpos_path} does not exist")
        return garpos_path


class GARPOSProcessJob(BaseModel):
    """Defines a job for running GARPOS processing."""

    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    surveys: Optional[List[str]] = Field(
        default_factory=list,
        title="Survey Name",
        description="Optional survey name for GARPOS processing",
    )
    config: GARPOSConfig = Field(
        default_factory=GARPOSConfig,
        title="GARPOS Configuration",
        description="Configuration for GARPOS processing",
    )

    class Config:
        arbitrary_types_allowed = True


class PipelineManifest(BaseModel):
    """
    The main Pydantic model for parsing and validating the pipeline manifest file.
    """

    main_directory: Path = Field(..., title="Main Directory")

    ingestion_jobs: List[PipelineIngestJob] = Field(
        default_factory=list, title="List of Pipeline Ingestion Jobs"
    )
    process_jobs: List[PipelinePreprocessJob] = Field(
        default_factory=list, title="List of Pipeline Jobs"
    )
    download_jobs: Optional[List[ArchiveDownloadJob]] = Field(
        default_factory=list, title="List of Archive Download Jobs"
    )
    garpos_jobs: Optional[List[GARPOSProcessJob]] = Field(
        default_factory=list, title="List of GARPOS Process Jobs"
    )
    global_config: SV3PipelineConfig = Field(..., title="Global Config")

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def _load(cls, data: dict) -> "PipelineManifest":
        """
        Load a PipelineManifest from a dictionary.

        This private method contains the core logic for parsing the manifest
        dictionary, handling global configurations, and constructing the
        list of jobs.

        Args:
            data: The dictionary to load from, typically from a JSON or YAML file.

        Returns:
            An instance of the PipelineManifest.
        """
        global_config = SV3PipelineConfig(**data.get("globalConfig", {}))
        garpos_config = GARPOSConfig(**data.get("garposConfig", {}))

        # Set GARPOS_PATH if provided
        if hasattr(garpos_config, "garpos_path"):
            os.environ["GARPOS_PATH"] = str(garpos_config.garpos_path)

        # Initialize lists for jobs
        process_jobs = []
        ingestion_jobs = []
        download_jobs = []
        garpos_jobs = []

        # Parse operations
        for operation in data.get("operations", []):
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
                except (KeyError, ValueError) as e:
                    raise ValueError(f"Invalid job type: {job['type']}") from e

                match job_type:
                    case PipelineJobType.INGESTION:
                        ingestion_jobs.append(
                            PipelineIngestJob(
                                network=network,
                                station=station,
                                campaign=campaign,
                                directory=job["directory"],  # need to validate this
                            )
                        )
                    case PipelineJobType.PREPROCESSING:
                        # Merge job-specific config with global config

                        job_config = global_config.model_copy(
                            update=job.get("config", {})
                        )
                        job_config = SV3PipelineConfig(**job_config.model_dump())
                        process_jobs.append(
                            PipelinePreprocessJob(
                                network=network,
                                station=station,
                                campaign=campaign,
                                config=job_config,
                            )
                        )
                    case PipelineJobType.DOWNLOAD:
                        download_jobs.append(
                            ArchiveDownloadJob(
                                network=network, station=station, campaign=campaign
                            )
                        )
                    case PipelineJobType.GARPOS:
                        config = garpos_config.model_copy(
                            update=dict(job.get("config", {}))
                        )
                        config = GARPOSConfig(**config.model_dump())
                        garpos_jobs.append(
                            GARPOSProcessJob(
                                network=network,
                                station=station,
                                campaign=campaign,
                                surveys=job.get("surveys", []),
                                config=config,
                            )
                        )

        # Instantiate the PipelineManifest
        return cls(
            main_dir=Path(data["projectDir"]),
            ingestion_jobs=ingestion_jobs,
            process_jobs=process_jobs,
            download_jobs=download_jobs,
            garpos_jobs=garpos_jobs,
            global_config=global_config,
        )

    @classmethod
    def from_json(cls, json_data: Path) -> "PipelineManifest":
        """
        Instantiates a PipelineManifest object from a JSON schema.

        Args:
            json_data: The path to the JSON file.

        Returns:
            An instance of the PipelineManifest class.
        """
        # Load JSON data
        with open(json_data, "r") as f:
            data = json.load(f)
        return cls._load(data)

    @classmethod
    def from_yaml(cls, yaml_data: Path) -> "PipelineManifest":
        """
        Instantiates a PipelineManifest object from a YAML schema.

        Args:
            yaml_data: The path to the YAML file.

        Returns:
            An instance of the PipelineManifest class.
        """
        # Load YAML data
        with open(yaml_data, "r") as f:
            data = yaml.safe_load(f)
        return cls._load(data)
