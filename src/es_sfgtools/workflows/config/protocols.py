from typing import Callable, Concatenate, Optional, Protocol,TypeVar, ParamSpec
from functools import wraps
from abc import ABC
from pathlib import Path

from es_sfgtools.data_mgmt.directorymgmt.handler import DirectoryHandler, CampaignDir, StationDir, SurveyDir, NetworkDir, TileDBDir
from es_sfgtools.data_mgmt.assetcatalog.handler import PreProcessCatalogHandler
from es_sfgtools.data_models.metadata import Site,Campaign,Survey

P = ParamSpec("P")
R = TypeVar("R")

class HasNetworkStationCampaign(Protocol):
    """A protocol for classes that have network, station, and campaign attributes."""
    current_network_name: Optional[str]
    current_station_name: Optional[str]
    current_campaign_name: Optional[str]


def validate_network_station_campaign(
    func: Callable[Concatenate[HasNetworkStationCampaign, P], R],
) -> Callable[Concatenate[HasNetworkStationCampaign, P], R]:
    @wraps(func)
    def wrapper(
        self: HasNetworkStationCampaign, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        if self.current_network_name is None:
            raise ValueError("Network name not set, use change_working_station")
        if self.current_station_name is None:
            raise ValueError("Station name not set, use change_working_station")
        if self.current_campaign_name is None:
            raise ValueError("campaign name not set, use change_working_station")
        return func(self, *args, **kwargs)

    return wrapper


class WorkflowABC(ABC):
    """Abstract base class for workflow implementations."""
    mid_process_workflow: bool = False

    def __init__(self,
                 directory:Path=None,
                 asset_catalog:Optional[PreProcessCatalogHandler]=None,
                 directory_handler:Optional[DirectoryHandler]=None,
                 station_metadata:Optional[Site]=None):

        if directory is None and directory_handler is None:
            raise ValueError("Either directory or directory_handler must be provided")

        if directory_handler is None:
            directory_handler = DirectoryHandler(location=directory)
            directory_handler.build()

        if asset_catalog is None:
            asset_catalog = PreProcessCatalogHandler(db_path=directory_handler.asset_catalog_db_path)
        if directory is None:
            directory = directory_handler.location

        self.directory = directory
        self.directory_handler = directory_handler
        self.asset_catalog = asset_catalog

        # Network tracking attributes
        self.current_network_name: Optional[str] = None
        self.current_network_dir: Optional[NetworkDir] = None

        # Station tracking attributes
        self.current_station_name: Optional[str] = None
        self.current_station_dir: Optional[StationDir] = None
        self.current_station_metadata: Optional[Site] = station_metadata

        # Campaign tracking attributes
        self.current_campaign_name: Optional[str] = None
        self.current_campaign_dir: Optional[CampaignDir] = None
        self.current_campaign_metadata: Optional[Campaign] = None

        # Survey tracking attributes
        self.current_survey_name: Optional[str] = None
        self.current_survey_dir: Optional[SurveyDir] = None
        self.current_survey_metadata: Optional[Survey] = None

    def _reset_survey(self) -> None:
        """Resets the current survey."""
        self.current_survey_name = None
        self.current_survey_metadata = None
        self.current_survey_dir = None

    def _reset_campaign(self) -> None:
        """Resets the current campaign."""
        self.current_campaign_name = None
        self.current_campaign_metadata = None
        self.current_campaign_dir = None
        self._reset_survey()

    def _reset_station(self) -> None:
        """Resets the current station."""
        self.current_station_name = None
        self.current_station_dir = None
        self.current_station_metadata = None
        self._reset_campaign()

    def _reset_network(self) -> None:
        """Resets the current network."""
        self.current_network_name = None
        self.current_network_dir = None
        self._reset_station()

    def set_network(self, network_id: str):
        """Sets the current network.

        Parameters
        ----------
        network_id : str
            The ID of the network to set.

        Raises
        ------
        ValueError
            If the network is not found in the site metadata.
        """
        self._reset_network()


        self.current_network_name = network_id

        if (
            current_network_dir := self.directory_handler.networks.get(
                self.current_network_name, None
            )
        ) is None:
            current_network_dir = self.directory_handler.add_network(
                name=self.current_network_name
            )
        self.current_network_dir = current_network_dir

    def set_station(self, station_id: str):
        """Sets the current station.

        Parameters
        ----------
        station_id : str
            The ID of the station to set.

        Raises
        ------
        ValueError
            If the station is not found in the site metadata.
        """

        self._reset_station()

        self.current_station_name = station_id
    
        if (
            current_station_dir := self.current_network_dir.stations.get(
                self.current_station_name, None
            )
        ) is None:
            current_station_dir = self.current_network_dir.add_station(
                name=self.current_station_name
            )
        
        self.current_station_dir = current_station_dir

        if self.mid_process_workflow:
            # Load site metadata for mid-process workflows
            assert self.current_station_dir.site_metadata.exists(), f"Site metadata file not found for station {station_id}, cannot proceed with mid-process workflow."
            self.current_station_metadata = Site.from_json(
                self.current_station_dir.site_metadata
            )

    def set_campaign(self, campaign_id: str):
        """Sets the current campaign.

        Parameters
        ----------
        campaign_id : str
            The ID of the campaign to set.

        Raises
        ------
        ValueError
            If the campaign is not found in the site metadata.
        """
        self._reset_campaign()

        # Set current campaign attributes
        if self.mid_process_workflow:
            for campaign in self.current_station_metadata.campaigns:
                if campaign.name == campaign_id:
                    self.current_campaign_metadata = campaign
                    self.current_campaign_name = campaign.name
                    break
            if self.current_campaign_metadata is None:
                raise ValueError(f"Campaign {campaign_id} not found in site metadata.")
        else:
            self.current_campaign_name = campaign_id
        if (
            current_campaign_dir := self.current_station_dir.campaigns.get(
                self.current_campaign_metadata.name, None
            )
        ) is None:
            current_campaign_dir = self.current_station_dir.add_campaign(
                name=campaign_id
            )
        self.current_campaign_dir = current_campaign_dir

    def set_network_station_campaign(
        self, network_id: str, station_id: str, campaign_id: str
    ):
        """Sets the current network, station, and campaign.

        Parameters
        ----------
        network : str
            The ID of the network to set.
        station : str
            The ID of the station to set.
        campaign : str
            The ID of the campaign to set.
        """
        assert isinstance(network_id, str), "network_id must be a string"
        assert isinstance(station_id, str), "station_id must be a string"
        assert isinstance(campaign_id, str), "campaign_id must be a string"

        self.set_network(network_id=network_id)
        self.set_station(station_id=station_id)
        self.set_campaign(campaign_id=campaign_id)

    @validate_network_station_campaign
    def set_survey(self, survey_id: str):
        """Sets the current survey.

        Parameters
        ----------
        survey_id : str
            The ID of the survey to set.

        Raises
        ------
        ValueError
            If the survey is not found in the current campaign.
        """
        assert self.mid_process_workflow, "set_survey is only available in mid-process workflows"
        assert isinstance(survey_id, str), "survey_id must be a string"

        self._reset_survey()

        # Set current survey attributes
        for survey in self.current_campaign_metadata.surveys:
            if survey.id == survey_id:
                self.current_survey_metadata = survey
                break
        if self.current_survey_metadata is None:
            raise ValueError(
                f"Survey {survey_id} not found in campaign {self.current_campaign_metadata.name}."
            )

        if (
            current_survey_dir := self.current_campaign_dir.surveys.get(survey_id, None)
        ) is None:
            current_survey_dir = self.current_campaign_dir.add_survey(name=survey_id)
        self.current_survey_dir = current_survey_dir
