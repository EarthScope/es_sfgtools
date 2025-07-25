from pathlib import Path
from typing import List, Optional
import yaml
from enum import Enum
import json
import os
from pydantic import BaseModel, Field, field_serializer, field_validator
from es_sfgtools.pipelines.sv3_pipeline import SV3PipelineConfig
from es_sfgtools.modeling.garpos_tools.schemas import (
    InversionParams
)

from rich import table

class PipelineJobType(str, Enum):
    PREPROCESSING = "preprocessing"
    INGESTION = "ingestion"
    DOWNLOAD = "download"
    GARPOS = "garpos"

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

class GARPOSConfig(BaseModel):
    garpos_path: Optional[Path] = Field(
        default=None, title="GARPOS Path", description="Path to GARPOS repository"
    )
    run_id: Optional[str] = Field(
        None, title="Run ID", description="Optional run ID for GARPOS processing",coerce_numbers_to_str=True
    )
    override: Optional[bool] = Field(
        False, title="Override Existing Data", description="Whether to override existing data"
    )
    inversion_params: Optional[InversionParams] = Field(
        None, title="Inversion Parameters", description="Parameters for GARPOS inversion"
    )
    class Config:
        arbitrary_types_allowed = True
        coerce= True

    @field_serializer("garpos_path")
    def _garpos_path_s( v: Path):
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
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    surveys: Optional[List[str]] = Field(
        default=[], title="Survey Name", description="Optional survey name for GARPOS processing"
    )
    config: GARPOSConfig = Field(
        default=GARPOSConfig(),
        title="GARPOS Configuration",
        description="Configuration for GARPOS processing"
    )
    class Config:
        arbitrary_types_allowed = True

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
    garpos_jobs: Optional[List[GARPOSProcessJob]] = Field(
        default=[], title="List of GARPOS Process Jobs"
    )
    global_config: SV3PipelineConfig = Field(..., title="Global Config")

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def _load(cls,data:dict) -> 'PipelineManifest':
        global_config = SV3PipelineConfig(**data.get("globalConfig",{})) 
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
                        config = garpos_config.model_copy(update=dict(job.get("config", {})))
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
        return cls._load(json_data)
    
    @classmethod
    def from_yaml(cls, yaml_data:Path) -> 'PipelineManifest':
        """
        Instantiates a PipelineManifest object from a YAML schema.

        Args:
            yaml_data (dict): The YAML data to parse.

        Returns:
            PipelineManifest: An instance of the PipelineManifest class.
        """
        # Load YAML data
        with open(yaml_data, "r") as f:
            yaml_data = yaml.safe_load(f)
        return cls._load(yaml_data)