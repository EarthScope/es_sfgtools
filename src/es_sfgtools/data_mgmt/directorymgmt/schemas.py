import datetime
import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, PrivateAttr

from .config import (
    ACOUSTIC_TDB,
    CAMPAIGN_METADATA_FILE,
    GNSS_OBS_SECONDARY_TDB,
    GNSS_OBS_TDB,
    GARPOS_DEFAULT_OBS_FILE,
    GARPOS_DEFAULT_SETTINGS_FILE,
    GARPOS_RESULTS_DIR,
    GARPOS_SHOTDATA_DIRECTORY,
    IMU_POSITION_TDB,
    KIN_TDB,
    LOGS_DIR,
    PRIDE_DIR,
    PROCESSED_DATA_DIR,
    QC_DIR,
    RAW_DATA_DIR,
    SHOTDATA_PRE_TDB,
    SHOTDATA_TDB,
    SVP_FILE_NAME,
    TILEDB_DIR,
    SURVEY_METADATA_FILE,
    INTERMEDIATE_DATA_DIR
)

class _Base(BaseModel):
    """Base Pydantic model with custom configuration.

    This class is used as a base for all other models in this module.
    It sets up a custom JSON encoder for Path objects and allows arbitrary
    types.
    """

    model_config = {
        "json_encoders": {
            Path: lambda v: str(v),
            datetime.datetime: lambda v: v.isoformat(),
        },
        "arbitrary_types_allowed": True,
    }


class GARPOSSurveyDir(_Base):
    """
    Represents a GARPOS survey directory structure.
    """

    _name = PrivateAttr("GARPOS")

    location: Optional[Path] = Field(
        default=None, description="The survey directory path"
    )
    log_directory: Optional[Path] = Field(
        default=None, description="The log directory path"
    )
    default_obsfile: Optional[Path] = Field(
        default=None, description="The default observation file path"
    )
    default_settings: Optional[Path] = Field(
        default=None, description="The default GARPOS settings file path"
    )
    svp_file: Optional[Path] = Field(
        default=None, description="The sound velocity profile file path"
    )
    results_dir: Optional[Path] = Field(
        default=None, description="The results directory path"
    )
    shotdata_rectified: Optional[Path] = Field(
        default=None, description="The survey shotdata file path"
    )
    shotdata_filtered: Optional[Path] = Field(
        default=None, description="The filtered shotdata file path"
    )

    survey_dir: Path = Field(..., description="The parent survey directory path")

    def build(self):
        """Creates the directory structure for the GARPOS survey."""
        if not self.location:
            self.location = self.survey_dir / self._name

        if not self.default_obsfile:
            self.default_obsfile = self.location / GARPOS_DEFAULT_OBS_FILE

        if not self.default_settings:
            self.default_settings = self.location / GARPOS_DEFAULT_SETTINGS_FILE

        if not self.log_directory:
            self.log_directory = self.location / "logs"

        if not self.results_dir:
            self.results_dir = self.location / GARPOS_RESULTS_DIR

        if not self.svp_file:
            self.svp_file = self.location / SVP_FILE_NAME

        # Create directories if they don't exist
        for path in [
            self.location,
            self.results_dir,
            self.log_directory,
        ]:
            path.mkdir(parents=True, exist_ok=True)


class TileDBDir(_Base):
    """
    Represents a directory structure for TileDB arrays.
    """

    # Optional directory paths, if not provided, will be auto-generated
    location: Optional[Path] = Field(
        default=None, description="The TileDB directory path"
    )
    shot_data: Optional[Path] = Field(
        default=None, description="The shotdata TileDB path"
    )
    shot_data_pre: Optional[Path] = Field(
        default=None, description="The preprocessed shotdata TileDB path"
    )
    kin_position_data: Optional[Path] = Field(
        default=None, description="The kinematic position TileDB path"
    )
    gnss_obs_data: Optional[Path] = Field(
        default=None, description="The GNSS observation TileDB path"
    )
    gnss_obs_data_secondary: Optional[Path] = Field(
        default=None, description="The secondary GNSS observation TileDB path"
    )
    imu_position_data: Optional[Path] = Field(
        default=None, description="The IMU position TileDB path"
    )
    acoustic_data: Optional[Path] = Field(
        default=None, description="The acoustic TileDB path"
    )

    station: Path = Field(..., description="The station directory path")

    def build(self):
        """Creates the directory structure for the TileDB arrays."""
        if not self.location:
            self.location = self.station / TILEDB_DIR
            self.location.mkdir(parents=True, exist_ok=True)
        if not self.shot_data:
            self.shot_data = self.location / SHOTDATA_TDB
        if not self.shot_data_pre:
            self.shot_data_pre = self.location / SHOTDATA_PRE_TDB
        if not self.kin_position_data:
            self.kin_position_data = self.location / KIN_TDB
        if not self.gnss_obs_data:
            self.gnss_obs_data = self.location / GNSS_OBS_TDB
        if not self.gnss_obs_data_secondary:
            self.gnss_obs_data_secondary = self.location / GNSS_OBS_SECONDARY_TDB
        if not self.imu_position_data:
            self.imu_position_data = self.location / IMU_POSITION_TDB
        if not self.acoustic_data:
            self.acoustic_data = self.location / ACOUSTIC_TDB


class SurveyDir(_Base):
    """
    Represents a survey directory structure.
    """

    location: Optional[Path] = Field(
        default=None, description="The survey directory path"
    )
    shotdata: Optional[Path] = Field(default=None, description="The shotdata file path")
    kinpositiondata: Optional[Path] = Field(
        default=None, description="The kinematic position file path"
    )
    imupositiondata: Optional[Path] = Field(
        default=None, description="The IMU position file path"
    )
    metadata: Optional[Path] = Field(
        default=None, description="The survey metadata file path"
    )
    garpos: Optional[GARPOSSurveyDir] = Field(
        default=None, description="GARPOS data directory path"
    )

    name: str = Field(..., description="The survey name")
    campaign: Path = Field(..., description="The campaign directory path")

    def build(self):
        """Creates the directory structure for the survey."""
        if not self.location:
            self.location = self.campaign / self.name
            self.location.mkdir(parents=True, exist_ok=True)

        if not self.metadata:
            self.metadata = self.location / SURVEY_METADATA_FILE

        if not self.garpos:
            self.garpos = GARPOSSurveyDir(survey_dir=self.location)
            self.garpos.build()


class CampaignDir(_Base):
    """
    Represents a campaign directory structure.
    """

    # Optional directory paths, if not provided, will be auto-generated
    location: Optional[Path] = Field(
        default=None, description="The campaign directory path"
    )
    raw: Optional[Path] = Field(default=None, description="Raw data directory path")
    processed: Optional[Path] = Field(
        default=None, description="Processed data directory path"
    )
    intermediate: Optional[Path] = Field(
        default=None, description="Intermediate data directory path"
    )
    surveys: Optional[dict[str, SurveyDir]] = Field(
        default={}, description="Surveys in the campaign"
    )
    log_directory: Optional[Path] = Field(
        default=None, description="Logs directory path"
    )
    qc: Optional[Path] = Field(
        default=None, description="Quality control directory path"
    )
    metadata_directory: Optional[Path] = Field(
        default=None, description="Metadata directory path"
    )
    campaign_metadata: Optional[Path] = Field(
        default=None, description="The campaign metadata file path"
    )
    rinex_metadata: Optional[Path] = Field(
        default=None, description="The RINEX metadata file path"
    )
    svp_file: Optional[Path] = Field(
        default=None, description="The sound velocity profile file path"
    )
    # Fields needed to auto-generate paths
    station: Path
    name: str

    def build(self):
        """Creates the directory structure for the campaign."""
        if not self.location:
            self.location = self.station / self.name

        if not self.metadata_directory:
            self.metadata_directory = self.location / "metadata"

        if not self.campaign_metadata:
            self.campaign_metadata = self.metadata_directory / CAMPAIGN_METADATA_FILE

        if not self.raw:
            self.raw = self.location / RAW_DATA_DIR

        if not self.processed:
            self.processed = self.location / PROCESSED_DATA_DIR
        if not self.intermediate:
            self.intermediate = self.location / INTERMEDIATE_DATA_DIR

        if not self.log_directory:
            self.log_directory = self.location / LOGS_DIR
        if not self.qc:
            self.qc = self.location / QC_DIR
        if not self.svp_file:
            self.svp_file = self.processed / SVP_FILE_NAME

        # Create subdirectories
        for subdir in [
            self.location,
            self.raw,
            self.processed,
            self.intermediate,
            self.log_directory,
            self.qc,
            self.metadata_directory,
        ]:
            subdir.mkdir(parents=True, exist_ok=True)

    def add_survey(self, name: str) -> SurveyDir:
        """Adds a new survey to the campaign.

        Parameters
        ----------
        name : str
            The name of the survey to add.

        Returns
        -------
        SurveyDir
            The newly created or existing SurveyDir object.
        """
        if name in self.surveys:
            print(f"Survey {name} already exists in campaign {self.name}")
            return self.surveys[name]
        new_survey = SurveyDir(name=name, campaign=self.location)
        new_survey.build()
        self.surveys[name] = new_survey
        return self.surveys[name]


class StationDir(_Base):
    """
    Represents a station directory structure.
    """

    # Optional location and stations
    campaigns: Optional[dict[str, CampaignDir]] = Field(
        default={}, description="Campaigns in the station"
    )
    location: Optional[Path] = Field(
        default=None, description="The station directory path"
    )
    tiledb_directory: Optional[TileDBDir] = Field(
        default=None, description="The TileDB directory path"
    )
    metadata_directory: Optional[Path] = Field(
        default=None, description="The metadata directory path"
    )
    site_metadata: Optional[Path] = Field(
        default=None, description="The site metadata file path"
    )
    # Fields needed to auto-generate paths
    name: str = Field(..., description="The station name")
    network: Path = Field(..., description="The network directory path")

    def build(self):
        """Creates the directory structure for the station."""
        if not self.location:
            self.location = self.network / self.name
            self.location.mkdir(parents=True, exist_ok=True)

        if not self.tiledb_directory:
            self.tiledb_directory = TileDBDir(station=self.location)
            self.tiledb_directory.build()

        if not self.metadata_directory:
            self.metadata_directory = self.location / "metadata"
            self.metadata_directory.mkdir(parents=True, exist_ok=True)

        if not self.site_metadata:
            self.site_metadata = self.metadata_directory / "site_metadata.json"
        # Build each campaign directory
        for campaign in self.campaigns.values():
            campaign.station = self.location
            campaign.build()

    def __getitem__(self, key: str) -> Optional[CampaignDir]:
        """Gets a campaign by name.

        Parameters
        ----------
        key : str
            The name of the campaign.

        Returns
        -------
        Optional[CampaignDir]
            The CampaignDir object if found, None otherwise.
        """
        try:
            return self.campaigns[key]
        except KeyError:
            print(f"Campaign {key} not found in station {self.name}")
            return None

    def add_campaign(self, name: str) -> CampaignDir:
        """Adds a new campaign to the station.

        Parameters
        ----------
        name : str
            The name of the campaign to add.

        Returns
        -------
        CampaignDir
            The newly created or existing CampaignDir object.
        """
        if name in self.campaigns:
            print(f"Campaign {name} already exists in station {self.name}")
            return self.campaigns[name]
        new_campaign = CampaignDir(name=name, station=self.location)
        new_campaign.build()
        self.campaigns[name] = new_campaign
        return self.campaigns[name]


class NetworkDir(_Base):
    """
    Represents a network directory structure.
    """

    # Optional location and stations
    stations: Optional[dict[str, StationDir]] = Field(
        default={}, description="Stations in the network"
    )
    location: Optional[Path] = Field(
        default=None, description="The network directory path"
    )

    name: str = Field(..., description="The network name")
    main_directory: Path = Field(..., description="The main directory path")

    def build(self):
        """Creates the directory structure for the network."""
        if not self.location:
            self.location = self.main_directory / self.name
        # Create network directory if it doesn't exist
        if not self.location.exists():
            self.location.mkdir(parents=True, exist_ok=True)
        # Build each station directory
        for station in self.stations.values():
            station.build()

    def __getitem__(self, key: str) -> Optional[StationDir]:
        """Gets a station by name.

        Parameters
        ----------
        key : str
            The name of the station.

        Returns
        -------
        Optional[StationDir]
            The StationDir object if found, None otherwise.
        """
        try:
            return self.stations[key]
        except KeyError:
            print(f"Station {key} not found in network {self.name}")
            return None

    def add_station(self, name: str) -> StationDir:
        """Adds a new station to the network.

        Parameters
        ----------
        name : str
            The name of the station to add.

        Returns
        -------
        StationDir
            The newly created or existing StationDir object.
        """
        if name in self.stations:
            print(f"Station {name} already exists in network {self.name}")
            return self.stations[name]
        new_station = StationDir(name=name, network=self.location)
        new_station.build()
        self.stations[name] = new_station
        return self.stations[name]
