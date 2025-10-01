# External imports
from pathlib import Path
from pydantic import BaseModel, Field, field_validator, field_serializer
from multiprocessing import cpu_count
import yaml
from typing import Optional

# Local imports
from ..pride_tools import PrideCLIConfig,PRIDEPPPFileConfig


class NovatelConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    n_processes: int = Field(
        default_factory=cpu_count, title="Number of Processes to Use"
    )


class RinexConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    n_processes: int = Field(
        default_factory=cpu_count, title="Number of Processes to Use"
    )
    settings_path: Optional[Path] = Field("", title="Settings Path")
    time_interval: Optional[int] = Field(1, title="Tile to Rinex Time Interval [h]")
    processing_year: Optional[int] = Field(
        default=-1, description="Processing year to query tiledb", le=2100
    )
    use_secondary: bool = Field(
        False,
        title="Use Secondary GNSS observation Data",
        description="If True, uses the secondary GNSS observation data for processing.",
    )

    class Config:
        arbitrary_types_allowed = True

    @field_serializer("settings_path")
    def _s_path(self, v):
        return str(v)

    @field_validator("settings_path")
    def _v_path(cls, v: str):
        return Path(v)


class DFOP00Config(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")

class QCpinConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")

class PositionUpdateConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    lengthscale: float = Field(
        default=0.1, ge=0.1, le=1, title="Length Scale for Interpolation in seconds"
    )
    plot: bool = Field(False)


class SV3PipelineConfig(BaseModel):
    pride_config: PrideCLIConfig = PrideCLIConfig()
    novatel_config: NovatelConfig = NovatelConfig()
    rinex_config: RinexConfig = RinexConfig()
    dfop00_config: DFOP00Config = DFOP00Config()
    position_update_config: PositionUpdateConfig = PositionUpdateConfig()

    class Config:
        title = "SV3 Pipeline Configuration"
        arbitrary_types_allowed = True

    def update(self, update_dict: dict) -> "SV3PipelineConfig":
        # update the object with values from a dict and returns a new copy
        copy = self.model_copy().model_dump()
        for key, value in update_dict.items():
            if key in copy:
                copy[key] = value | copy[key]
        return SV3PipelineConfig(**copy)

    def to_yaml(self, filepath: Path):
        with open(filepath, "w") as f:
            yaml.dump(self.model_dump(), f)

    @classmethod
    def load_yaml(cls, filepath: Path):
        with open(filepath) as f:
            data = yaml.load(f)
        return cls(**data)
    
class SV3QCPipelineConfig(BaseModel):
    pride_config: PrideCLIConfig = PrideCLIConfig()
    novatel_config: NovatelConfig = NovatelConfig()
    rinex_config: RinexConfig = RinexConfig()
    qcpin_config: QCpinConfig = QCpinConfig()
    position_update_config: PositionUpdateConfig = PositionUpdateConfig()

    class Config:
        title = "SV3 Pipeline Configuration"
        arbitrary_types_allowed = True

    def update(self, update_dict: dict) -> "SV3PipelineConfig":
        # update the object with values from a dict and returns a new copy
        copy = self.model_copy().model_dump()
        for key, value in update_dict.items():
            if key in copy:
                copy[key] = value | copy[key]
        return SV3PipelineConfig(**copy)

    def to_yaml(self, filepath: Path):
        with open(filepath, "w") as f:
            yaml.dump(self.model_dump(), f)

    @classmethod
    def load_yaml(cls, filepath: Path):
        with open(filepath) as f:
            data = yaml.load(f)
        return cls(**data)


class PrepSiteData(BaseModel):
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    inter_dir: Path = Field(..., title="Intermediate Directory")
    pride_dir: Path = Field(..., title="Pride Directory")
    gnss_obs_data_dest: str | Path = Field(..., title="GNSS Obs Data Destination")
    kin_position_data_dest: str | Path = Field(..., title="Kin Position Data Destination")
    shot_data_dest: str | Path = Field(..., title="Shot Data Destination")

    class Config:
        arbitrary_types_allowed = True

    @field_serializer(
        "inter_dir", "gnss_obs_data_dest", "kin_position_data_dest", "shot_data_dest", "pride_dir"
    )
    def _s_path(self, v):
        if isinstance(v, Path):
            return str(v)
        return v

    @field_validator("inter_dir", "pride_dir")
    def _v_path(cls, v: str | Path):
        if isinstance(v, str):
            return Path(v)
        return v
