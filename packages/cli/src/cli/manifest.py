"""
This module defines the Pydantic models for parsing the pipeline manifest file.

These models provide data validation and a structured interface for accessing
manifest contents, which define the workspace, ingestion, download, and
processing jobs for the pipeline.
"""

import json
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_serializer, field_validator

from earthscope_sfg_workflows.config.workspace import Workspace, WorkspaceType
from earthscope_sfg_workflows.modeling.garpos_tools.schemas import InversionParams
from earthscope_sfg_workflows.prefiltering.schemas import FilterConfig
from earthscope_sfg_workflows.workflows.pipelines.config import SV3PipelineConfig


class ManifestWorkspaceConfig(BaseModel):
    """Workspace configuration section of the pipeline manifest.

    Controls which deployment environment is used and how cloud credentials
    are supplied.  Maps directly onto the ``Workspace`` factory classmethods.
    """

    type: str = Field("local", description="Workspace type: 'local', 'geolab', or 'ecs'")
    s3_sync_bucket: str | None = Field(
        None, alias="s3SyncBucket", description="S3 bucket for sync (geolab/ecs)"
    )
    aws_profile: str | None = Field(None, alias="awsProfile")
    aws_access_key_id: str | None = Field(None, alias="awsAccessKeyId")
    aws_secret_access_key: str | None = Field(None, alias="awsSecretAccessKey")
    aws_session_token: str | None = Field(None, alias="awsSessionToken")
    pride_dir: Path | None = Field(None, alias="prideDir")

    model_config = {"populate_by_name": True}

    def build(self, root_directory: Path) -> Workspace:
        """Instantiate a ``Workspace`` for *root_directory* using this config."""
        wtype = WorkspaceType(self.type.lower())
        match wtype:
            case WorkspaceType.LOCAL:
                return Workspace.local(
                    root_directory,
                    pride_binary_dir=self.pride_dir,
                    aws_profile=self.aws_profile,
                    s3_sync_bucket=self.s3_sync_bucket,
                )
            case WorkspaceType.GEOLAB:
                if not self.s3_sync_bucket:
                    raise ValueError("s3SyncBucket is required for geolab workspaces")
                return Workspace.geolab(
                    root_directory,
                    self.s3_sync_bucket,
                    aws_profile=self.aws_profile,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_session_token=self.aws_session_token,
                    pride_binary_dir=self.pride_dir,
                )
            case WorkspaceType.ECS:
                if not self.s3_sync_bucket:
                    raise ValueError("s3SyncBucket is required for ecs workspaces")
                return Workspace.ecs(
                    root_directory,
                    self.s3_sync_bucket,
                    aws_profile=self.aws_profile,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_session_token=self.aws_session_token,
                    pride_binary_dir=self.pride_dir,
                )


class PipelineJobType(StrEnum):
    """Enumeration for the different types of pipeline jobs."""

    PREPROCESSING = "preprocessing"
    INGESTION = "ingestion"
    DOWNLOAD = "download"
    GARPOS = "garpos"


class PreprocessJobType(StrEnum):
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
    global_config: SV3PipelineConfig | None = Field(
        ..., title="Pipeline Configuration"
    )
    secondary_config: dict | None = Field(
        default_factory=dict, title="Secondary Configuration Overrides"
    )


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

    garpos_path: Path | None = Field(
        default=None, title="GARPOS Path", description="Path to GARPOS repository"
    )
    iterations: int | None = Field(
        2,
        title="Number of Iterations",
        description="Number of GARPOS inversion iterations to perform",
    )
    run_id: str | None = Field(
        "0",
        title="Run ID",
        description="Optional run ID for GARPOS processing",
        coerce_numbers_to_str=True,
    )
    override: bool | None = Field(
        False,
        title="Override Existing Data",
        description="Whether to override existing data",
    )
    inversion_params: InversionParams | None = Field(
        None,
        title="Inversion Parameters",
        description="Parameters for GARPOS inversion",
    )
    filter_config: FilterConfig | None = Field(
        default_factory=FilterConfig,
        title="Filter Configuration",
        description="Configuration for prefiltering GARPOS shot data",
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
    surveys: list[str] | None = Field(
        default_factory=list,
        title="Survey Name",
        description="Optional survey name for GARPOS processing",
    )
    global_config: GARPOSConfig = Field(
        default_factory=GARPOSConfig,
        title="GARPOS Configuration",
        description="Configuration for GARPOS processing",
    )
    secondary_config: dict | None = Field(
        default_factory=dict, title="Secondary Configuration Overrides"
    )

    class Config:
        arbitrary_types_allowed = True


class PipelineManifest(BaseModel):
    """
    The main Pydantic model for parsing and validating the pipeline manifest file.
    """

    main_directory: Path = Field(..., title="Main Directory")
    workspace_config: ManifestWorkspaceConfig = Field(
        default_factory=ManifestWorkspaceConfig, title="Workspace Configuration"
    )

    ingestion_jobs: list[PipelineIngestJob] = Field(
        default_factory=list, title="List of Pipeline Ingestion Jobs"
    )
    process_jobs: list[PipelinePreprocessJob] = Field(
        default_factory=list, title="List of Pipeline Jobs"
    )
    download_jobs: list[ArchiveDownloadJob] | None = Field(
        default_factory=list, title="List of Archive Download Jobs"
    )
    garpos_jobs: list[GARPOSProcessJob] | None = Field(
        default_factory=list, title="List of GARPOS Process Jobs"
    )
    global_config: SV3PipelineConfig = Field(..., title="Global Config")

    class Config:
        arbitrary_types_allowed = True

    def build_workspace(self) -> Workspace:
        """Build and return a ``Workspace`` for this manifest's project directory."""
        return self.workspace_config.build(self.main_directory)

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
        if (global_config_data := data.get("globalConfig")) is not None:
            global_config = SV3PipelineConfig(**global_config_data)
        else:
            global_config = SV3PipelineConfig()

        if (garpos_config_data := data.get("garposConfig")) is not None:
            garpos_config = GARPOSConfig(**garpos_config_data)
        else:
            garpos_config = GARPOSConfig()

        if (workspace_config_data := data.get("workspaceConfig")) is not None:
            workspace_config = ManifestWorkspaceConfig(**workspace_config_data)
        else:
            workspace_config = ManifestWorkspaceConfig()

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
                raise ValueError(f"Missing key in operations: {e}") from e

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

                        process_jobs.append(
                            PipelinePreprocessJob(
                                network=network,
                                station=station,
                                campaign=campaign,
                                global_config=global_config,
                                secondary_config=job.get("config", {}),
                            )
                        )
                    case PipelineJobType.DOWNLOAD:
                        download_jobs.append(
                            ArchiveDownloadJob(
                                network=network, station=station, campaign=campaign
                            )
                        )
                    case PipelineJobType.GARPOS:
                        garpos_jobs.append(
                            GARPOSProcessJob(
                                network=network,
                                station=station,
                                campaign=campaign,
                                surveys=job.get("surveys", []),
                                global_config=garpos_config,
                                secondary_config=job.get("config", {}),
                            )
                        )

        # Instantiate the PipelineManifest
        return cls(
            main_directory=Path(data["projectDir"]),
            workspace_config=workspace_config,
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
        with open(json_data) as f:
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
        with open(yaml_data) as f:
            data = yaml.safe_load(f)
        return cls._load(data)

    @classmethod
    def load(cls, file_path: Path | str) -> "PipelineManifest":
        """
        Instantiates a PipelineManifest object from a JSON or YAML schema.

        Args:
            file_path: The path to the JSON or YAML file.
        Returns:
            An instance of the PipelineManifest class.
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)

        match file_path.suffix:
            case ".json":
                return cls.from_json(file_path)
            case ".yaml" | ".yml":
                return cls.from_yaml(file_path)
            case _:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")
