from typing import Callable, Concatenate, Optional, Protocol,TypeVar, ParamSpec
from functools import wraps

from es_sfgtools.data_mgmt.directorymgmt.handler import DirectoryHandler, CampaignDir, StationDir, SurveyDir, NetworkDir, TileDBDir
from es_sfgtools.data_mgmt.assetcatalog.handler import PreProcessCatalogHandler
from es_sfgtools.data_models.metadata import Site,Campaign,Survey

class PreProcessIngestProtocol(Protocol):
    asset_catalog: PreProcessCatalogHandler
    directory_handler: DirectoryHandler
    current_campaign_name: str
    current_station_name: str
    current_network_name: str
    current_network_dir: NetworkDir
    current_campaign_dir: CampaignDir
    current_station_dir: StationDir
    current_station_meta: Site

    def set_network(self, network_id: str) -> None: ...
    def _reset_network(self) -> None: ...

    def set_station(self, station_id: str) -> None: ...
    def _reset_station(self) -> None: ...

    def set_campaign(self, campaign_id: str) -> None: ...
    def _reset_campaign(self) -> None: ...

    def set_network_station_campaign(self, network_id: str, station_id: str, campaign_id: str) -> None: ...


class MidProcessIngestProtocol(PreProcessIngestProtocol):
    current_campaign: Campaign
    current_survey_name: str
    current_survey_dir: SurveyDir
    current_survey: Survey

    def set_survey(self, survey: str) -> None: ...
    def _reset_survey(self) -> None: ...


P = ParamSpec("P")
R = TypeVar("R")


class HasNetworkStationCampaign(Protocol):
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
