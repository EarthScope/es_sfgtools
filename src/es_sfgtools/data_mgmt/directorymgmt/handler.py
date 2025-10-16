"""
This module defines a set of Pydantic models for managing a directory structure for geodesy data processing.

The models define a hierarchical structure of directories and files, starting from a main directory,
and branching into networks, stations, campaigns, and various data and results directories.

:class DirectoryHandler: The main class that manages the entire directory structure.
:class NetworkDir: Represents a network of stations.
:class StationDir: Represents a single station with multiple campaigns.
:class CampaignDir: Represents a data collection campaign.
:class TileDBDir: Manages TileDB arrays for a station.
:class GARPOSCampaignDir: Manages GARPOS-specific data and results.
:class GARPOSSurveyDir: Represents a single GARPOS survey.
"""
import datetime
import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, PrivateAttr

from .schemas import (
    _Base,NetworkDir,StationDir,CampaignDir,SurveyDir,TileDBDir,GARPOSSurveyDir
)
from .config import (
    ASSET_CATALOG,
    PRIDE_DIR,
)

class DirectoryHandler(_Base):
    """
    The main class for managing the directory structure.
    """
    _filepath:str = PrivateAttr("directoryCatalog.json")

    filepath: Optional[Path] = Field(default=None, description="Path to the directory structure JSON file")

    asset_catalog_db_path: Optional[Path] = Field(default=None, description="Path to the asset catalog database")

    networks: Optional[dict[str, NetworkDir]] = {}
    pride_directory: Optional[Path] = Field(default=None, description="The PRIDE PPPAR binary directory path")

    location: Path = Field(
     description="The main directory path"
    )

    def save(self):
        """Saves the directory structure to a JSON file."""
        with open(self.filepath, "w") as file:
            json_dict = json.loads(self.model_dump_json())
            json.dump(json_dict, file, indent=4)
    

    @classmethod
    def load(cls, path: str | Path) -> "DirectoryHandler":
        """Loads the directory structure from a JSON file.

        Parameters
        ----------
        path : Union[str, Path]
            The path to the JSON file.

        Returns
        -------
        DirectoryHandler
            A DirectoryHandler object.
        """
        with open(path, "r") as file:
            raw_data = file.read()
        directory_handler = cls.model_validate_json(raw_data)
        directory_handler.filepath = path
        return directory_handler

    def add_network(self, name: str) -> NetworkDir:
        """Adds a new network to the directory structure.

        Parameters
        ----------
        name : str
            The name of the network to add.

        Returns
        -------
        NetworkDir
            The newly created or existing NetworkDir object.
        """
        if name in self.networks:
            print(f"Network {name} already exists.")
            return self.networks[name]
        new_network = NetworkDir(name=name,main_directory=self.location)
        new_network.build()
        self.networks[name] = new_network
        return self.networks[name]

    def __getitem__(self, key: str) -> Optional[NetworkDir]:
        """Gets a network by name.

        Parameters
        ----------
        key : str
            The name of the network.

        Returns
        -------
        Optional[NetworkDir]
            The NetworkDir object if found, None otherwise.
        """
        try:
            return self.networks[key]
        except KeyError:
            print(f"Network {key} not found.")
            return None

    def build(self):
        """Creates the main directory structure."""
        if not self.filepath:
            self.filepath = self.location / self._filepath
            if self.filepath.exists():
                loaded = DirectoryHandler.load(self.filepath)
                for key, value in loaded.__dict__.items():
                    setattr(self, key, value)

        if not self.pride_directory:
            self.pride_directory = self.location / PRIDE_DIR
            self.pride_directory.mkdir(parents=True, exist_ok=True)

        if not self.asset_catalog_db_path:
            self.asset_catalog_db_path = self.location / ASSET_CATALOG
            if not self.asset_catalog_db_path.exists():
                self.asset_catalog_db_path.touch()

    def build_station_directory(self,network_name:str,station_name:str=None,campaign_name:str=None,survey_name:str=None) -> Optional[tuple[NetworkDir,StationDir,CampaignDir,SurveyDir]]:
        """Builds a station directory, and optionally a campaign directory.

        Parameters
        ----------
        network_name : str
            The name of the network.
        station_name : str, optional
            The name of the station, by default None.
        campaign_name : str, optional
            The name of the campaign, by default None.
        survey_name : str, optional
            The name of the survey, by default None.

        Returns
        -------
        Optional[tuple[NetworkDir, StationDir, CampaignDir, SurveyDir]]
            A tuple containing the created directory objects, or None if the
            directory was not built successfully.
        """
        if station_name and not network_name:
            print("Station name provided without network name.")
            return None, None, None, None
        if campaign_name and not station_name:
            print("Campaign name provided without station name.")
            return None, None, None, None
        if survey_name and not campaign_name:
            print("Survey name provided without campaign name.")
            return None, None, None, None
        
        networkDir: NetworkDir = None
        stationDir: StationDir = None
        campaignDir: CampaignDir = None
        surveyDir: SurveyDir = None

        if not (networkDir:= self.networks.get(network_name)):
            networkDir: NetworkDir = self.add_network(name=network_name)
        
        if station_name:
            if not (stationDir := networkDir.stations.get(station_name)):
                stationDir: StationDir = networkDir.add_station(name=station_name)
            
            if campaign_name:
                if not (campaignDir := stationDir.campaigns.get(campaign_name)):
                    campaignDir:CampaignDir = stationDir.add_campaign(name=campaign_name)
                
                if survey_name:
                    if not (surveyDir := campaignDir.surveys.get(survey_name)):
                        surveyDir:SurveyDir = campaignDir.add_survey(name=survey_name)

        return networkDir, stationDir, campaignDir, surveyDir
