"""
This module defines a set of Pydantic models for managing a directory structure for geodesy data processing.

The models define a hierarchical structure of directories and files, starting from a main directory,
and branching into networks, stations, campaigns, and various data and results directories.

The main classes are:
- DirectoryHandler: The main class that manages the entire directory structure.
- NetworkDir: Represents a network of stations.
- StationDir: Represents a single station with multiple campaigns.
- CampaignDir: Represents a data collection campaign.
- TileDBDir: Manages TileDB arrays for a station.
- GARPOSCampaignDir: Manages GARPOS-specific data and results.
- GARPOSSurveyDir: Represents a single GARPOS survey.
"""
from pathlib import Path
import os
from pydantic import BaseModel,Field
from typing import Optional


# GARPOS-specific directory and file names
GARPOS_DATA_DIR = "GARPOS"
GARPOS_RESULTS_DIR = "results"
GARPOS_DEFAULT_SETTINGS_FILE = "default_settings.ini"
GARPOS_DEFAULT_OBS_FILE = "observation.ini"
SVP_FILE_NAME = "svp.csv"
GARPOS_SHOTDATA_DIRECTORY = "shotdata"

# General data directories
RAW_DATA_DIR = "raw"
PROCESSED_DATA_DIR = "processed"
INTERMEDIATE_DATA_DIR = "intermediate"
LOGS_DIR = "logs"
QC_DIR = "qc"
PRIDE_DIR = "Pride"
TILEDB_DIR = "TileDB"

# TileDB array names
ACOUSTIC_TDB = "acoustic.tdb"
KIN_TDB = "kin_position.tdb"
IMU_POSITION_TDB = "imu_position.tdb"
SHOTDATA_TDB = "shotdata.tdb"
SHOTDATA_PRE_TDB = "shotdata_pre.tdb"
GNSS_OBS_TDB = "gnss_obs.tdb"
GNSS_OBS_SECONDARY_TDB = "gnss_obs_secondary.tdb"

# Asset catalog database file name
ASSET_CATALOG = "catalog.sqlite"

CAMPAIGN_METADATA_FILE = "campaign_meta.json"
SURVEY_METADATA_FILE = "survey_meta.json"

class _Base(BaseModel):
    """
    Base Pydantic model with custom configuration.

    This class is used as a base for all other models in this module.
    It sets up a custom JSON encoder for Path objects and allows arbitrary types.
    """
    model_config = {
        "json_encoders": {
            Path: lambda v: str(v)
        },
        'arbitrary_types_allowed': True
    }

class GARPOSSurveyDir(_Base):
    """
    Represents a GARPOS survey directory structure.
    """
    survey_metadata: Optional[Path] = Field(default=None, description="The survey metadata file path",regex=r".*\.json$")
    default_obsfile: Optional[Path] = Field(default=None, description="The default observation file path")
    location: Optional[Path] = Field(default=None, description="The survey directory path")
    results_dir: Optional[Path] = Field(default=None, description="The results directory path")
    shotdata: Optional[Path] = Field(default=None, description="The survey shotdata file path")
    name: str = Field(..., description="The survey name")
    garpos_campaign_dir: Path = Field(
     description="The GARPOS campaign directory path"
    )

    def build(self):
        """
        Creates the directory structure for the GARPOS survey.
        """
        if not self.location:
            self.location = self.garpos_campaign_dir / self.name
        
        if not self.default_obsfile:
            self.default_obsfile = self.location / GARPOS_DEFAULT_OBS_FILE

        self.results_dir = self.location / GARPOS_RESULTS_DIR
        if not self.survey_metadata:
            self.survey_metadata = self.location / "survey_meta.json"

        # Create directories if they don't exist
        for path in [self.location, self.results_dir]:
            path.mkdir(parents=True, exist_ok=True)

class GARPOSCampaignDir(_Base):
    """
    Represents a GARPOS campaign directory structure.
    """
    shotdata: Optional[Path] = Field(default=None, description="The shotdata file path")
    location: Optional[Path] = Field(default=None, description="The GARPOS directory path")
    surveys: Optional[dict[str, GARPOSSurveyDir]] = Field(default={}, description="Surveys in the campaign")
    default_settings: Optional[Path] = Field(default=None, description="The default GARPOS settings file path")
    campaign: Path = Field(..., description="The campaign directory path")
    svp_file: Optional[Path] = Field(default=None, description="The sound velocity profile file path",regex=r".*\.csv$")

    def add_survey(self, name: str) -> bool:
        """
        Adds a new survey to the campaign.

        Args:
            name: The name of the survey to add.

        Returns:
            True if the survey was added successfully, False otherwise.
        """
        if name in self.surveys:
            print(f"Survey {name} already exists in campaign {self.campaign.name}")
            return False
        new_survey = GARPOSSurveyDir(name=name, garpos_campaign_dir=self.location)
        new_survey.build()
        self.surveys[name] = new_survey
        return True
    
    def build(self):
        """
        Creates the directory structure for the GARPOS campaign.
        """

        if not self.location:
            self.location = self.campaign / GARPOS_DATA_DIR
           
        if not self.svp_file:
            self.svp_file = self.location / SVP_FILE_NAME

        if not self.default_settings:
            self.default_settings = self.location / GARPOS_DEFAULT_SETTINGS_FILE

        if not self.shotdata:
            self.shotdata = self.location / GARPOS_SHOTDATA_DIRECTORY

        for subdir in [self.location, self.shotdata]:
            subdir.mkdir(parents=True, exist_ok=True)

        for survey in self.surveys.values():
            survey.build()

    def __getitem__(self, key: str) -> Optional[GARPOSSurveyDir]:
        """
        Gets a survey by name.

        Args:
            key: The name of the survey.

        Returns:
            The GARPOSSurveyDir object if found, None otherwise.
        """
        try:
            return self.surveys[key]
        except KeyError:
            print(f"Survey {key} not found in campaign {self.campaign.name}")
            return None

class TileDBDir(_Base):
    """
    Represents a directory structure for TileDB arrays.
    """
    # Optional directory paths, if not provided, will be auto-generated
    location: Optional[Path] = Field(default=None, description="The TileDB directory path")
    shot_data: Optional[Path] = Field(default=None, description="The shotdata TileDB path")
    shot_data_pre: Optional[Path] = Field(default=None, description="The preprocessed shotdata TileDB path")
    kin_position_data: Optional[Path] = Field(default=None, description="The kinematic position TileDB path")
    gnss_obs_data: Optional[Path] = Field(default=None, description="The GNSS observation TileDB path")
    gnss_obs_data_secondary: Optional[Path] = Field(default=None, description="The secondary GNSS observation TileDB path")
    imu_position_data: Optional[Path] = Field(default=None, description="The IMU position TileDB path")
    acoustic_data: Optional[Path] = Field(default=None, description="The acoustic TileDB path")

    station: Path = Field(..., description="The station directory path")

    def build(self):
        """
        Creates the directory structure for the TileDB arrays.
        """
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

class CampaignDir(_Base):
    """
    Represents a campaign directory structure.
    """
    # Optional directory paths, if not provided, will be auto-generated
    location: Optional[Path] = Field(default=None, description="The campaign directory path")
    raw: Optional[Path] = Field(default=None, description="Raw data directory path")
    processed: Optional[Path] = Field(default=None, description="Processed data directory path")
    intermediate: Optional[Path] = Field(default=None, description="Intermediate data directory path")
    garpos: Optional[GARPOSCampaignDir] = Field(default=None, description="GARPOS data directory path")
    log_directory: Optional[Path] = Field(default=None, description="Logs directory path")
    qc: Optional[Path] = Field(default=None, description="Quality control directory path")
    campaign_metadata: Optional[Path] = Field(default=None, description="The campaign metadata file path",regex=r".*\.json$")

    # Fields needed to auto-generate paths
    station: Path
    name: str

    def build(self):
        """
        Creates the directory structure for the campaign.
        """
        if not self.location:
            self.location = self.station / self.name

        if not self.campaign_metadata:
            self.campaign_metadata = self.location / CAMPAIGN_METADATA_FILE
        # Auto-generate subdirectory paths if not provided
        if not self.raw:
            self.raw = self.location / RAW_DATA_DIR

        if not self.processed:
            self.processed = self.location / PROCESSED_DATA_DIR
        if not self.intermediate:
            self.intermediate = self.location / INTERMEDIATE_DATA_DIR
        if not self.garpos:
            self.garpos = GARPOSCampaignDir(campaign=self.location)

        if not self.log_directory:
            self.log_directory = self.location / LOGS_DIR
        if not self.qc:
            self.qc = self.location / QC_DIR

        # Create subdirectories
        for subdir in [self.location,self.raw, self.processed, self.intermediate, self.log_directory, self.qc]:
            subdir.mkdir(parents=True, exist_ok=True)

        for tobuild in [self.garpos]:
            tobuild.build()

class StationDir(_Base):
    """
    Represents a station directory structure.
    """
    # Optional location and stations
    campaigns: Optional[dict[str, CampaignDir]] = Field(default={}, description="Campaigns in the station")
    location: Optional[Path] = Field(default=None, description="The station directory path")
    tiledb_directory: Optional[TileDBDir] = Field(default=None, description="The TileDB directory path")

    # Fields needed to auto-generate paths
    name: str = Field(..., description="The station name")
    network: Path = Field(..., description="The network directory path")

    def build(self):
        """
        Creates the directory structure for the station.
        """
        if not self.location:
            self.location = self.network / self.name
            self.location.mkdir(parents=True, exist_ok=True)
        
        if not self.tiledb_directory:
            self.tiledb_directory = TileDBDir(station=self.location)
            self.tiledb_directory.build()

        # Build each campaign directory
        for campaign in self.campaigns.values():
            campaign.station = self.location
            campaign.build()

    def __getitem__(self, key: str) -> Optional[CampaignDir]:
        """
        Gets a campaign by name.

        Args:
            key: The name of the campaign.

        Returns:
            The CampaignDir object if found, None otherwise.
        """
        try:
            return self.campaigns[key]
        except KeyError:
            print(f"Campaign {key} not found in station {self.name}")
            return None

    def add_campaign(self,name:str) -> bool:
        """
        Adds a new campaign to the station.

        Args:
            name: The name of the campaign to add.

        Returns:
            True if the campaign was added successfully, False otherwise.
        """
        if name in self.campaigns:
            print(f"Campaign {name} already exists in station {self.name}")
            return False
        new_campaign = CampaignDir(name=name,station=self.location)
        new_campaign.build()
        self.campaigns[name] = new_campaign
        return True


class NetworkDir(_Base):
    """
    Represents a network directory structure.
    """
    # Optional location and stations
    stations: Optional[dict[str, StationDir]] = Field(default={}, description="Stations in the network")
    location: Optional[Path] = Field(default=None, description="The network directory path")

    name: str = Field(..., description="The network name")
    main_directory:Path = Field(..., description="The main directory path")

    def build(self):
        """
        Creates the directory structure for the network.
        """
        if not self.location:
            self.location = self.main_directory / self.name
        # Create network directory if it doesn't exist
        if not self.location.exists():
            self.location.mkdir(parents=True, exist_ok=True)
        # Build each station directory
        for station in self.stations.values():
            station.build()
    
    def __getitem__(self, key: str) -> Optional[StationDir]:
        """
        Gets a station by name.

        Args:
            key: The name of the station.

        Returns:
            The StationDir object if found, None otherwise.
        """
        try:
            return self.stations[key]
        except KeyError:
            print(f"Station {key} not found in network {self.name}")
            return None
    
    def add_station(self,name:str) -> bool:
        """
        Adds a new station to the network.

        Args:
            name: The name of the station to add.

        Returns:
            True if the station was added successfully, False otherwise.
        """
        if name in self.stations:
            print(f"Station {name} already exists in network {self.name}")
            return False
        new_station = StationDir(name=name,network=self.location)
        new_station.build()
        self.stations[name] = new_station
        return True


class DirectoryHandler(BaseModel):
    """
    The main class for managing the directory structure.
    """
    filepath:str = "directoryCatalog.json"
    asset_catalog_db_path: Optional[Path] = Field(default=None, description="Path to the asset catalog database")
    location: Path = Field(..., description="The main directory path")

    networks: Optional[dict[str, NetworkDir]] = {}
    pride_directory: Optional[Path] = Field(default=None, description="The PRIDE PPPAR binary directory path")

    def save(self):
        """
        Saves the directory structure to a JSON file.
        """
        with open(self.filepath, "w") as file:
            file.write(self.model_dump_json())
    
    @classmethod
    def load(cls, path: str | Path) -> "DirectoryHandler":
        """
        Loads the directory structure from a JSON file.

        Args:
            path: The path to the JSON file.

        Returns:
            A DirectoryHandler object.
        """
        with open(path, "r") as file:
            raw_data = file.read()
        return cls.model_validate_json(raw_data)

    def add_network(self, name: str) -> bool:
        """
        Adds a new network to the directory structure.

        Args:
            name: The name of the network to add.

        Returns:
            True if the network was added successfully, False otherwise.
        """
        if name in self.networks:
            print(f"Network {name} already exists.")
            return False
        new_network = NetworkDir(name=name,main_directory=self.location)
        new_network.build()
        self.networks[name] = new_network
        return True
    
    def __getitem__(self, key: str) -> Optional[NetworkDir]:
        """
        Gets a network by name.

        Args:
            key: The name of the network.

        Returns:
            The NetworkDir object if found, None otherwise.
        """
        try:
            return self.networks[key]
        except KeyError:
            print(f"Network {key} not found.")
            return None
        
    def build(self):
        """
        Creates the main directory structure.
        """
        if not self.pride_directory:
            self.pride_directory = self.location / PRIDE_DIR
            self.pride_directory.mkdir(parents=True, exist_ok=True)

        if not self.asset_catalog_db_path:
            self.asset_catalog_db_path = self.location / ASSET_CATALOG
            if not self.asset_catalog_db_path.exists():
                self.asset_catalog_db_path.touch()

    def build_station_directory(self,network_name:str,station_name:str=None,campaign_name:str=None) -> bool:
        """
        Builds a station directory, and optionally a campaign directory.

        Args:
            network_name: The name of the network.
            station_name: The name of the station.
            campaign_name: The name of the campaign.

        Returns:
            True if the directory was built successfully, False otherwise.
        """

        if campaign_name and not station_name:
            print("Campaign name provided without station name.")
            return False
        
        network = self.networks.get(network_name)
        if not network:
            self.add_network(network_name)
        
        if station_name:
            network = self.networks[network_name]
            station = network.stations.get(station_name)
            if not station:
                network.add_station(station_name)
            station = network.stations[station_name]

            if campaign_name:
                if campaign_name not in station.campaigns:
                    station.add_campaign(campaign_name)

        return True