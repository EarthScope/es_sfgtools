import datetime
import json
from pathlib import Path
from typing import Optional, Union
from es_sfgtools.config.env_config import Environment,WorkingEnvironment
from cloudpathlib import S3Path
from pydantic import BaseModel, Field, PrivateAttr,model_serializer

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
    @model_serializer
    def serialize(self) -> dict[str, any]:
        """Custom serializer to include private attributes."""
        raw = self.__dict__
        output = {}
        for key, value in raw.items():
            if isinstance(value, (Path, S3Path)):
                if value.exists():
                    output[key] = str(value)
                else:
                    output[key] = None
            else:
                output[key] = value
        return output

    model_config = {
        "json_encoders": {
            Path: lambda v: str(v),
            S3Path: lambda v: str(v),
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

    @classmethod
    def is_garpos_directory(cls,path: Path| S3Path) -> bool:
        """Check if the given path is a valid GARPOS survey directory.

        A valid GARPOS survey directory contains GARPOS default files.

        Parameters
        ----------
        path : Path | S3Path
            The path to check.

        Returns
        -------
        bool
            True if the path is a valid GARPOS survey directory, False otherwise.
        """
        test_dir = cls(survey_dir=path.parent)
        test_dir.location = path

        test_default_obsfile = test_dir.location / GARPOS_DEFAULT_OBS_FILE
        test_default_settings = test_dir.location / GARPOS_DEFAULT_SETTINGS_FILE

        if test_default_obsfile.exists() and test_default_settings.exists():
            return True
            
        return False
    
    @classmethod
    def load_from_path(cls, path: Path | S3Path) -> "GARPOSSurveyDir":
        """Load a GARPOSSurveyDir instance from an existing directory path.

        Parameters
        ----------
        path : Path | S3Path
            The path to the GARPOS survey directory.

        Returns
        -------
        GARPOSSurveyDir
            The loaded GARPOSSurveyDir instance.
        """
        if not cls.is_garpos_directory(path):
            raise ValueError(f"The path {path} is not a valid GARPOS survey directory.")

        survey_dir = cls(survey_dir=path.parent)
        survey_dir.location = path

        default_obsfile_path = path / GARPOS_DEFAULT_OBS_FILE
        if default_obsfile_path.exists():
            survey_dir.default_obsfile = default_obsfile_path

        default_settings_path = path / GARPOS_DEFAULT_SETTINGS_FILE
        if default_settings_path.exists():
            survey_dir.default_settings = default_settings_path

        log_directory_path = path / "logs"
        if log_directory_path.exists():
            survey_dir.log_directory = log_directory_path

        results_dir_path = path / GARPOS_RESULTS_DIR
        if results_dir_path.exists():
            survey_dir.results_dir = results_dir_path

        svp_file_path = path / SVP_FILE_NAME
        if svp_file_path.exists():
            survey_dir.svp_file = svp_file_path

        return survey_dir

class TileDBDir(_Base):
    """
    Represents a directory structure for TileDB arrays.
    """

    # Optional directory paths, if not provided, will be auto-generated
    location: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The TileDB directory path"
    )
    shot_data: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The shotdata TileDB path"
    )
    shot_data_pre: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The preprocessed shotdata TileDB path"
    )
    kin_position_data: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The kinematic position TileDB path"
    )
    gnss_obs_data: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The GNSS observation TileDB path"
    )
    gnss_obs_data_secondary: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The secondary GNSS observation TileDB path"
    )
    imu_position_data: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The IMU position TileDB path"
    )
    acoustic_data: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The acoustic TileDB path"
    )

    station: Union[Path, S3Path] = Field(..., description="The station directory path")

    def build(self):
            
        """Creates the directory structure for the TileDB arrays."""
        if not self.location:
            self.location = self.station / TILEDB_DIR
            if Environment.working_environment() == WorkingEnvironment.LOCAL:
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

    def to_s3(self) -> None:
        # Convert all Path attributes to S3Path
        for field_name, field_value in self.__dict__.items():
            if isinstance(field_value, Path):
                field_value = str(field_value)
                if 's3:/' in field_value and 's3://' not in field_value:
                    new_path = field_value.replace("s3:/", "s3://")
                    s3_path = S3Path(new_path)
                elif 's3://' in str(field_value):
                    s3_path = S3Path(field_value)
                else:
                    s3_path = S3Path(f"s3://{str(field_value)}")

                setattr(self, field_name, s3_path)

    @classmethod
    def load_from_path(cls, path: Path | S3Path) -> "TileDBDir":
        """Load a TileDBDir instance from an existing directory path.

        Parameters
        ----------
        path : Path | S3Path
            The path to the TileDB directory.

        Returns
        -------
        TileDBDir
            The loaded TileDBDir instance.
        """
        name = str(path.name)
        station = path.parent

        tiledb_dir = cls(station=station)
        tiledb_dir.location = path

        shot_data_path = path / SHOTDATA_TDB
        if shot_data_path.exists():
            tiledb_dir.shot_data = shot_data_path

        shot_data_pre_path = path / SHOTDATA_PRE_TDB
        if shot_data_pre_path.exists():
            tiledb_dir.shot_data_pre = shot_data_pre_path

        kin_position_data_path = path / KIN_TDB
        if kin_position_data_path.exists():
            tiledb_dir.kin_position_data = kin_position_data_path

        gnss_obs_data_path = path / GNSS_OBS_TDB
        if gnss_obs_data_path.exists():
            tiledb_dir.gnss_obs_data = gnss_obs_data_path

        gnss_obs_data_secondary_path = path / GNSS_OBS_SECONDARY_TDB
        if gnss_obs_data_secondary_path.exists():
            tiledb_dir.gnss_obs_data_secondary = gnss_obs_data_secondary_path

        imu_position_data_path = path / IMU_POSITION_TDB
        if imu_position_data_path.exists():
            tiledb_dir.imu_position_data = imu_position_data_path

        acoustic_data_path = path / ACOUSTIC_TDB
        if acoustic_data_path.exists():
            tiledb_dir.acoustic_data = acoustic_data_path

        return tiledb_dir

class SurveyDir(_Base):
    """
    Represents a survey directory structure.
    """

    location: Optional[Path|S3Path] = Field(
        default=None, description="The survey directory path"
    )
    shotdata: Optional[Path|S3Path] = Field(default=None, description="The shotdata file path")
    kinpositiondata: Optional[Path|S3Path] = Field(
        default=None, description="The kinematic position file path"
    )
    imupositiondata: Optional[Path|S3Path] = Field(
        default=None, description="The IMU position file path"
    )
    metadata: Optional[Path|S3Path] = Field(
        default=None, description="The survey metadata file path"
    )
    garpos: Optional[GARPOSSurveyDir] = Field(
        default=None, description="GARPOS data directory path"
    )

    name: str = Field(..., description="The survey name")
    campaign: Path|S3Path = Field(..., description="The campaign directory path")

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

    @classmethod
    def is_survey_directory(cls,path: Path| S3Path) -> bool:
        """Check if the given path is a valid survey directory.

        A valid survey directory contains a GARPOS subdirectory.

        Parameters
        ----------
        path : Path | S3Path
            The path to check.

        Returns
        -------
        bool
            True if the path is a valid survey directory, False otherwise.
        """
        test_dir = cls(name=str(path.name),campaign=path.parent)
        test_dir.location = path
        test_garpos_dir = test_dir.location / "GARPOS"
        shotdata_files = list(test_dir.location.glob("*.csv"))

        if test_garpos_dir.exists() or len(shotdata_files) > 0:
            return True
            
        return False

    @classmethod
    def load_from_path(cls, path: Path | S3Path) -> "SurveyDir":
        """Load a SurveyDir instance from an existing directory path.

        Parameters
        ----------
        path : Path | S3Path
            The path to the survey directory.

        Returns
        -------
        SurveyDir
            The loaded SurveyDir instance.
        """
        if not cls.is_survey_directory(path):
            raise ValueError(f"The path {path} is not a valid survey directory.")

        name = str(path.name)
        campaign = path.parent

        survey_dir = cls(name=name, campaign=campaign)
        survey_dir.location = path

        metadata_path = path / SURVEY_METADATA_FILE
        if metadata_path.exists():
            survey_dir.metadata = metadata_path

        garpos_dir = path / "GARPOS"
        if garpos_dir.exists():
            survey_dir.garpos = GARPOSSurveyDir.load_from_path(garpos_dir)

        shotdata_files = list(path.glob("*.csv"))
        for shotdata_file in shotdata_files:
            if "shotdata" in shotdata_file.name.lower() and "filtered" not in shotdata_file.name.lower():
                survey_dir.shotdata = shotdata_file
  

        return survey_dir

class CampaignDir(_Base):
    """
    Represents a campaign directory structure.
    """

    # Optional directory paths, if not provided, will be auto-generated
    location: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The campaign directory path"
    )
    raw: Optional[Union[Path, S3Path]] = Field(default=None, description="Raw data directory path")
    processed: Optional[Union[Path, S3Path]] = Field(
        default=None, description="Processed data directory path"
    )
    intermediate: Optional[Union[Path, S3Path]] = Field(
        default=None, description="Intermediate data directory path"
    )
    surveys: Optional[dict[str, SurveyDir]] = Field(
        default={}, description="Surveys in the campaign"
    )
    log_directory: Optional[Union[Path, S3Path]] = Field(
        default=None, description="Logs directory path"
    )
    qc: Optional[Union[Path, S3Path]] = Field(
        default=None, description="Quality control directory path"
    )
    metadata_directory: Optional[Union[Path, S3Path]] = Field(
        default=None, description="Metadata directory path"
    )
    campaign_metadata: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The campaign metadata file path"
    )
    rinex_metadata: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The RINEX metadata file path"
    )
    svp_file: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The sound velocity profile file path"
    )
    # Fields needed to auto-generate paths
    station: Union[Path, S3Path] = Field(..., description="The station directory path")
    name: str

    def build(self):
        """Creates the directory structure for the campaign."""
        if not self.location:
            self.location = self.station / self.name
            self.location.mkdir(parents=True, exist_ok=True)

        if not self.metadata_directory:
            self.metadata_directory = self.location / "metadata"
            self.metadata_directory.mkdir(parents=True, exist_ok=True)

        if not self.campaign_metadata:
            self.campaign_metadata = self.metadata_directory / CAMPAIGN_METADATA_FILE

        if not self.raw:
            self.raw = self.location / RAW_DATA_DIR
            if Environment.working_environment() == WorkingEnvironment.LOCAL:
                self.raw.mkdir(parents=True, exist_ok=True)

        if not self.processed:
            self.processed = self.location / PROCESSED_DATA_DIR
            self.processed.mkdir(parents=True, exist_ok=True) 

        if not self.intermediate:
            self.intermediate = self.location / INTERMEDIATE_DATA_DIR
            if Environment.working_environment() == WorkingEnvironment.LOCAL:
                self.intermediate.mkdir(parents=True, exist_ok=True)

        if not self.log_directory:
            self.log_directory = self.location / LOGS_DIR
            if Environment.working_environment() == WorkingEnvironment.LOCAL:
                self.log_directory.mkdir(parents=True, exist_ok=True)

        if not self.qc:
            self.qc = self.location / QC_DIR
            if Environment.working_environment() == WorkingEnvironment.LOCAL:
                self.qc.mkdir(parents=True, exist_ok=True)

        if not self.svp_file:
            self.svp_file = self.processed / SVP_FILE_NAME


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

    @classmethod
    def is_campaign_directory(cls,path: Path| S3Path) -> bool:
        """Check if the given path is a valid campaign directory.

        A valid campaign directory contains subdirectories for surveys.

        Parameters
        ----------
        path : Path | S3Path
            The path to check.

        Returns
        -------
        bool
            True if the path is a valid campaign directory, False otherwise.
        """
        test_dir = cls(name=str(path.name),station=path.parent)
        test_dir.location = path
        test_raw_dir = test_dir.location / RAW_DATA_DIR
        test_processed_dir = test_dir.location / PROCESSED_DATA_DIR
        test_intermediate_dir = test_dir.location / INTERMEDIATE_DATA_DIR
        test_logs_dir = test_dir.location / LOGS_DIR
        test_qc_dir = test_dir.location / QC_DIR

        if any(dir.exists() for dir in [
            test_raw_dir,test_processed_dir,test_intermediate_dir,test_logs_dir,test_qc_dir]) is True:
            return True
        return False
    
    @classmethod
    def load_from_path(cls, path: Path | S3Path) -> "CampaignDir":
        """Load a CampaignDir instance from an existing directory path.

        Parameters
        ----------
        path : Path | S3Path
            The path to the campaign directory.

        Returns
        -------
        CampaignDir
            The loaded CampaignDir instance.
        """
        if not cls.is_campaign_directory(path):
            raise ValueError(f"The path {path} is not a valid campaign directory.")

        name = str(path.name)
        station = path.parent

        campaign_dir = cls(name=name, station=station)
        campaign_dir.location = path

        raw_path = path / RAW_DATA_DIR
        if raw_path.exists():
            campaign_dir.raw = raw_path

        processed_path = path / PROCESSED_DATA_DIR
        if processed_path.exists():
            campaign_dir.processed = processed_path

        intermediate_path = path / INTERMEDIATE_DATA_DIR
        if intermediate_path.exists():
            campaign_dir.intermediate = intermediate_path

        log_directory_path = path / LOGS_DIR
        if log_directory_path.exists():
            campaign_dir.log_directory = log_directory_path

        qc_path = path / QC_DIR
        if qc_path.exists():
            campaign_dir.qc = qc_path

        svp_file_path = path / PROCESSED_DATA_DIR / SVP_FILE_NAME
        if svp_file_path.exists():
            campaign_dir.svp_file = svp_file_path
        
        # Load surveys
        for campaign_subdir in path.iterdir():
            if SurveyDir.is_survey_directory(campaign_subdir):
                survey_dir = SurveyDir.load_from_path(campaign_subdir)
                campaign_dir.surveys[survey_dir.name] = survey_dir

        return campaign_dir


class StationDir(_Base):
    """
    Represents a station directory structure.
    """

    # Optional location and stations
    campaigns: Optional[dict[str, CampaignDir]] = Field(
        default={}, description="Campaigns in the station"
    )
    location: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The station directory path"
    )
    tiledb_directory: Optional[TileDBDir] = Field(
        default=None, description="The TileDB directory path"
    )
    metadata_directory: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The metadata directory path"
    )
    site_metadata: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The site metadata file path"
    )
    # Fields needed to auto-generate paths
    name: str = Field(..., description="The station name")
    network: Union[Path, S3Path] = Field(..., description="The network directory path")

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
    
    @classmethod
    def is_station_directory(cls,path: Path| S3Path) -> bool:
        """Check if the given path is a valid station directory.

        A valid station directory contains subdirectories for campaigns.

        Parameters
        ----------
        path : Path | S3Path
            The path to check.

        Returns
        -------
        bool
            True if the path is a valid station directory, False otherwise.
        """
        name = str(path.name)
        if len(name) != 4 or not name.isupper():
            return False
        test_dir = cls(name=name,network=path.parent)
        test_dir.location = path
        if Environment.working_environment() == WorkingEnvironment.LOCAL:
            test_tdb_dir = test_dir.location / TILEDB_DIR
            if not test_tdb_dir.exists():
                return False
        return True
    
    @classmethod
    def load_from_path(cls, path: Path | S3Path) -> "StationDir":
        """Load a StationDir instance from an existing directory path.

        Parameters
        ----------
        path : Path | S3Path
            The path to the station directory.

        Returns
        -------
        StationDir
            The loaded StationDir instance.
        """
        if not cls.is_station_directory(path):
            raise ValueError(f"The path {path} is not a valid station directory.")

        name = str(path.name)
        network = path.parent

        station_dir = cls(name=name, network=network)
        station_dir.location = path

        tiledb_dir_path = path / TILEDB_DIR
        if tiledb_dir_path.exists():
            station_dir.tiledb_directory = TileDBDir.load_from_path(tiledb_dir_path)

        metadata_directory_path = path / "metadata"
        if metadata_directory_path.exists():
            station_dir.metadata_directory = metadata_directory_path

        site_metadata_path = metadata_directory_path / "site_metadata.json"
        if site_metadata_path.exists():
            station_dir.site_metadata = site_metadata_path
        
        # Load campaigns
        for station_subdir in path.iterdir():
            if CampaignDir.is_campaign_directory(station_subdir):
                campaign_dir = CampaignDir.load_from_path(station_subdir)
                station_dir.campaigns[campaign_dir.name] = campaign_dir

        return station_dir


class NetworkDir(_Base):
    """
    Represents a network directory structure.
    """

    # Optional location and stations
    stations: Optional[dict[str, StationDir]] = Field(
        default={}, description="Stations in the network"
    )
    location: Optional[Union[Path, S3Path]] = Field(
        default=None, description="The network directory path"
    )

    name: str = Field(..., description="The network name")
    main_directory: Union[Path, S3Path] = Field(..., description="The main directory path")

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

    @classmethod
    def is_network_directory(cls,path: Path| S3Path) -> bool:
        """Check if the given path is a valid network directory.

        A valid network directory contains subdirectories for stations.

        Parameters
        ----------
        path : Path | S3Path
            The path to check.

        Returns
        -------
        bool
            True if the path is a valid network directory, False otherwise.
        """
        test_dir = cls(name=str(path.name),main_directory=path.parent)
        test_dir.location = path
        station_dirs = test_dir.location.glob("[A-Z][A-Z][A-Z][0-9]")
        if any(StationDir.is_station_directory(d) for d in station_dirs):
            return True
        return False

    @classmethod
    def load_from_path(cls, path: Path | S3Path) -> "NetworkDir":
        """Load a NetworkDir instance from an existing directory path.  

        Parameters
        ----------
        path : Path | S3Path
            The path to the network directory.

        Returns
        -------
        NetworkDir
            The loaded NetworkDir instance.
        """
        if not cls.is_network_directory(path):
            raise ValueError(f"The path {path} is not a valid network directory.")

        name = str(path.name)
        main_directory = path.parent

        network_dir = cls(name=name, main_directory=main_directory)
        network_dir.location = path

        # Load station directories
        for station_subdir in path.iterdir():
            if StationDir.is_station_directory(station_subdir):
                station_dir = StationDir.load_from_path(station_subdir)
                network_dir.stations[station_dir.name] = station_dir

        return network_dir
