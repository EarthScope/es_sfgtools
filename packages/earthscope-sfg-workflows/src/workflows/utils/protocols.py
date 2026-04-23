from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Concatenate, ParamSpec, Protocol, TypeVar

from ...data_mgmt.assetcatalog.handler import PreProcessCatalogHandler
from ...data_mgmt.directorymgmt import (
    CampaignDir,
    NetworkDir,
    StationDir,
    SurveyDir,
)
from ...data_mgmt.directorymgmt.handler import DirectoryHandler
from ...data_models.metadata import Campaign, Site, Survey

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class WorkflowContext:
    """Value-object that tracks the current network/station/campaign/survey context.

    Replaces the 15+ individual attributes previously scattered across WorkflowABC.
    Resetting a higher level automatically clears all dependent lower levels.
    """

    network_name: str | None = None
    network_dir: NetworkDir | None = None

    station_name: str | None = None
    station_dir: StationDir | None = None
    station_metadata: Site | None = None

    campaign_name: str | None = None
    campaign_dir: CampaignDir | None = None
    campaign_metadata: Campaign | None = None

    survey_name: str | None = None
    survey_dir: SurveyDir | None = None
    survey_metadata: Survey | None = None

    def reset_survey(self) -> None:
        self.survey_name = None
        self.survey_dir = None
        self.survey_metadata = None

    def reset_campaign(self) -> None:
        self.campaign_name = None
        self.campaign_dir = None
        self.campaign_metadata = None
        self.reset_survey()

    def reset_station(self) -> None:
        self.station_name = None
        self.station_dir = None
        self.station_metadata = None
        self.reset_campaign()

    def reset_network(self) -> None:
        self.network_name = None
        self.network_dir = None
        self.reset_station()

    @property
    def has_network_station_campaign(self) -> bool:
        return all([self.network_name, self.station_name, self.campaign_name])


class HasNetworkStationCampaign(Protocol):
    """A protocol for classes that have network, station, and campaign attributes."""

    current_network_name: str | None
    current_station_name: str | None
    current_campaign_name: str | None


def validate_network_station_campaign(
    func: Callable[Concatenate[HasNetworkStationCampaign, P], R],
) -> Callable[Concatenate[HasNetworkStationCampaign, P], R]:
    @wraps(func)
    def wrapper(self: HasNetworkStationCampaign, *args: P.args, **kwargs: P.kwargs) -> R:
        if self.current_network_name is None:
            raise ValueError("Network name not set, use change_working_station")
        if self.current_station_name is None:
            raise ValueError("Station name not set, use change_working_station")
        if self.current_campaign_name is None:
            raise ValueError("campaign name not set, use change_working_station")
        return func(self, *args, **kwargs)

    return wrapper

def validate_network_station(
    func: Callable[Concatenate[HasNetworkStationCampaign, P], R],
) -> Callable[Concatenate[HasNetworkStationCampaign, P], R]:
    @wraps(func)
    def wrapper(self: HasNetworkStationCampaign, *args: P.args, **kwargs: P.kwargs) -> R:
        if self.current_network_name is None:
            raise ValueError("Network name not set, use change_working_station")
        if self.current_station_name is None:
            raise ValueError("Station name not set, use change_working_station")
        return func(self, *args, **kwargs)

    return wrapper

class WorkflowABC(ABC):
    """
    Abstract base class for seafloor geodesy workflow implementations.

    This class provides a standardized framework for managing hierarchical contexts
    in seafloor geodesy data processing workflows. It handles the organization of
    data across four hierarchical levels: network → station → campaign → survey.

    The class supports two operational modes:

    1. **Setup workflows** (mid_process_workflow=False): For initial data organization
       and directory structure creation
    2. **Mid-process workflows** (mid_process_workflow=True): For scientific data
       processing with metadata validation and loading

    Attributes
    ----------
    mid_process_workflow : bool, default False
        Flag indicating whether this is a mid-process workflow that requires
        metadata validation and loading.
    directory : Path
        Root directory for the workflow data structure.
    directory_handler : DirectoryHandler
        Handler for managing the hierarchical directory structure.
    asset_catalog : PreProcessCatalogHandler
        Handler for managing data asset catalogs and metadata.
    current_network_name : str or None
        Name of the currently active network.
    current_network_dir : NetworkDir or None
        Directory handler for the current network.
    current_station_name : str or None
        Name of the currently active station.
    current_station_dir : StationDir or None
        Directory handler for the current station.
    current_station_metadata : Site or None
        Metadata object for the current station (loaded in mid-process workflows).
    current_campaign_name : str or None
        Name of the currently active campaign.
    current_campaign_dir : CampaignDir or None
        Directory handler for the current campaign.
    current_campaign_metadata : Campaign or None
        Metadata object for the current campaign (loaded in mid-process workflows).
    current_survey_name : str or None
        Name of the currently active survey.
    current_survey_dir : SurveyDir or None
        Directory handler for the current survey.
    current_survey_metadata : Survey or None
        Metadata object for the current survey (loaded in mid-process workflows).

    Notes
    -----
    The hierarchical context system ensures data organization consistency across
    different workflow implementations. Each level must be set in order:
    network → station → campaign → survey.

    Context switching at any level automatically resets all dependent lower levels
    to maintain data integrity and prevent cross-contamination between different
    organizational contexts.

    See Also
    --------
    Workspace : Unified config and directory management
    PreProcessCatalogHandler : Asset catalog management
    Site : Station metadata model
    Campaign : Campaign metadata model
    Survey : Survey metadata model
    """

    mid_process_workflow: bool = False

    def __init__(
        self,
        directory: Path | str = None,
        s3_sync_bucket: str | None = None,
        asset_catalog: PreProcessCatalogHandler | None = None,
        station_metadata: Site | None = None,
    ):
        """Initialize the workflow with a directory handler and asset catalog.

        Parameters
        ----------
        directory : Path | str, optional
            Root path of the data tree. Auto-detected from environment when omitted.
        s3_sync_bucket : str, optional
            S3 bucket name/URI for sync operations.
        asset_catalog : PreProcessCatalogHandler, optional
            Pre-configured asset catalog. Created automatically when omitted.
        station_metadata : Site, optional
            Pre-loaded station metadata for mid-process workflows.
        """
        if directory is None:
            import os
            directory = os.environ.get("MAIN_DIRECTORY", ".")

        self.directory_handler: DirectoryHandler = DirectoryHandler.load_from_path(directory)
        if self.directory_handler is None:
            self.directory_handler = DirectoryHandler(location=Path(directory))
            self.directory_handler.build()

        self.directory: Path = self.directory_handler.location
        self.s3_sync_bucket: str | None = s3_sync_bucket

        if asset_catalog is None:
            asset_catalog = PreProcessCatalogHandler(
                db_path=self.directory_handler.asset_catalog_db_path
            )

        self.asset_catalog = asset_catalog

        # Consolidated hierarchical context
        self.ctx = WorkflowContext(station_metadata=station_metadata)

    # ---- Backward-compatible property accessors ----
    # These delegate to self.ctx so existing code using self.current_*
    # keeps working without changes in downstream files.

    @property
    def current_network_name(self) -> str | None:
        return self.ctx.network_name

    @current_network_name.setter
    def current_network_name(self, value: str | None):
        self.ctx.network_name = value

    @property
    def current_network_dir(self) -> NetworkDir | None:
        return self.ctx.network_dir

    @current_network_dir.setter
    def current_network_dir(self, value: NetworkDir | None):
        self.ctx.network_dir = value

    @property
    def current_station_name(self) -> str | None:
        return self.ctx.station_name

    @current_station_name.setter
    def current_station_name(self, value: str | None):
        self.ctx.station_name = value

    @property
    def current_station_dir(self) -> StationDir | None:
        return self.ctx.station_dir

    @current_station_dir.setter
    def current_station_dir(self, value: StationDir | None):
        self.ctx.station_dir = value

    @property
    def current_station_metadata(self) -> Site | None:
        return self.ctx.station_metadata

    @current_station_metadata.setter
    def current_station_metadata(self, value: Site | None):
        self.ctx.station_metadata = value

    @property
    def current_campaign_name(self) -> str | None:
        return self.ctx.campaign_name

    @current_campaign_name.setter
    def current_campaign_name(self, value: str | None):
        self.ctx.campaign_name = value

    @property
    def current_campaign_dir(self) -> CampaignDir | None:
        return self.ctx.campaign_dir

    @current_campaign_dir.setter
    def current_campaign_dir(self, value: CampaignDir | None):
        self.ctx.campaign_dir = value

    @property
    def current_campaign_metadata(self) -> Campaign | None:
        return self.ctx.campaign_metadata

    @current_campaign_metadata.setter
    def current_campaign_metadata(self, value: Campaign | None):
        self.ctx.campaign_metadata = value

    @property
    def current_survey_name(self) -> str | None:
        return self.ctx.survey_name

    @current_survey_name.setter
    def current_survey_name(self, value: str | None):
        self.ctx.survey_name = value

    @property
    def current_survey_dir(self) -> SurveyDir | None:
        return self.ctx.survey_dir

    @current_survey_dir.setter
    def current_survey_dir(self, value: SurveyDir | None):
        self.ctx.survey_dir = value

    @property
    def current_survey_metadata(self) -> Survey | None:
        return self.ctx.survey_metadata

    @current_survey_metadata.setter
    def current_survey_metadata(self, value: Survey | None):
        self.ctx.survey_metadata = value

    # ---- Context reset methods (delegate to WorkflowContext) ----

    def _reset_survey(self) -> None:
        """Reset the survey-level context to None."""
        self.ctx.reset_survey()

    def _reset_campaign(self) -> None:
        """Reset the campaign-level context and all dependent survey context."""
        self.ctx.reset_campaign()

    def _reset_station(self) -> None:
        """Reset the station-level context and all dependent contexts."""
        self.ctx.reset_station()

    def _reset_network(self) -> None:
        """Reset the network-level context and all dependent contexts."""
        self.ctx.reset_network()

    def set_network(self, network_id: str):
        """
        Set the current network context and reset all dependent contexts.

        Establishes the network-level context for workflow operations. The network
        represents the top level of the organizational hierarchy and typically
        corresponds to a geographic region, institutional boundary, or collection
        of related seafloor geodesy sites.

        This method resets all existing contexts and initializes the network-level
        directory structure, creating the network directory if it doesn't exist.

        Parameters
        ----------
        network_id : str
            The identifier for the network to activate. This should match the
            expected directory name in the data structure and serve as a unique
            identifier for the network.

        Raises
        ------
        ValueError
            If the network directory cannot be created or accessed through the
            directory handler.

        Notes
        -----
        The method performs these operations in sequence:

        1. Reset all contexts (network, station, campaign, survey) via _reset_network()
        2. Set current_network_name to the provided network_id
        3. Retrieve existing NetworkDir or create new one via workspace
        4. Store the NetworkDir in current_network_dir

        The network directory is created automatically if it doesn't exist, making
        this method suitable for both setup and mid-process workflows. All dependent
        contexts (station, campaign, survey) are cleared when the network changes
        to ensure proper hierarchical isolation.

        This is typically the first method called in a workflow sequence, followed
        by set_station(), set_campaign(), and optionally set_survey().

        See Also
        --------
        set_station : Next step in context hierarchy
        set_network_station_campaign : Convenience method to set multiple contexts
        NetworkDir : Network directory management class
        """
        self._reset_network()

        self.current_network_name = network_id

        if (
            current_network_dir := self.directory_handler.networks.get(self.current_network_name, None)
        ) is None:
            current_network_dir = self.directory_handler.add_network(name=self.current_network_name)
        self.current_network_dir = current_network_dir

    def set_station(self, station_id: str):
        """
        Set the current station context and reset all dependent contexts.

        Establishes the station-level context within the current network. The station
        represents a specific seafloor geodesy site with associated transponders,
        benchmarks, and measurement infrastructure.

        For mid-process workflows (mid_process_workflow=True), this method also loads
        and validates the station metadata file, which contains critical information
        about transponder positions, site configuration, and processing parameters.

        Parameters
        ----------
        station_id : str
            The identifier for the station to activate. This should match both
            the directory name in the data structure and the station identifier
            in the site metadata files.

        Raises
        ------
        AssertionError
            If mid_process_workflow=True and the site metadata file is not found
            for the specified station. The metadata file is required for scientific
            processing operations.

        ValueError
            If the station directory cannot be created or accessed through the
            directory handler.

        Notes
        -----
        The method performs these operations in sequence:

        1. Reset station, campaign, and survey contexts via _reset_station()
        2. Set current_station_name to the provided station_id
        3. Retrieve or create the StationDir object from current_network_dir
        4. Store the StationDir in current_station_dir
        5. If mid_process_workflow=True:
           - Verify site metadata file exists
           - Load Site metadata from JSON file
           - Store in current_station_metadata

        The site metadata loading in mid-process workflows provides access to
        transponder configurations, campaign definitions, and other scientific
        parameters required for data processing and analysis.

        See Also
        --------
        set_network : Must be called before this method
        set_campaign : Next step in context hierarchy
        set_network_station_campaign : Convenience method to set multiple contexts
        Site.from_json : Used internally to load station metadata
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
        self.current_station_dir.build()

        if self.current_station_dir.site_metadata.exists():
            self.current_station_metadata = Site.from_json(self.current_station_dir.site_metadata)

        if self.mid_process_workflow:
            # Load site metadata for mid-process workflows
            assert self.current_station_metadata is not None, (
                f"Site metadata file not found for station {station_id}, cannot proceed with mid-process workflow."
            )

    def set_campaign(self, campaign_id: str):
        """
        Set the current campaign context and reset survey-level context.

        Establishes the campaign-level context within the current station. A campaign
        represents a specific deployment or measurement period, typically lasting
        several days to months, during which multiple surveys are conducted.

        For mid-process workflows, this method validates that the campaign exists
        in the station metadata and loads the corresponding Campaign object with
        deployment details and survey configurations.

        Parameters
        ----------
        campaign_id : str
            The identifier for the campaign to activate. This should match both
            the directory name in the data structure and the campaign identifier
            in the station metadata campaigns list.

        Raises
        ------
        ValueError
            If mid_process_workflow=True and the campaign is not found in the current
            station metadata campaigns list, or if the campaign directory cannot be
            created or accessed.

        AssertionError
            If set_station() has not been called first to establish the station
            context, or if mid_process_workflow=True but station metadata is not
            available.

        Notes
        -----
        The method performs these operations in sequence:

        1. Reset campaign and survey contexts via _reset_campaign()
        2. If mid_process_workflow=True:
           - Search station metadata for matching campaign
           - Load Campaign object into current_campaign_metadata
           - Set current_campaign_name from metadata
        3. If not mid_process_workflow:
           - Set current_campaign_name directly from campaign_id
        4. Retrieve or create CampaignDir object from current_station_dir
        5. Store the CampaignDir in current_campaign_dir

        Campaign validation in mid-process workflows ensures that only properly
        configured campaigns are activated, preventing processing errors downstream.
        The campaign metadata includes deployment dates, instrument configurations,
        and survey parameters specific to that measurement period.

        See Also
        --------
        set_station : Must be called before this method
        set_survey : Next step in context hierarchy (mid-process workflows only)
        set_network_station_campaign : Convenience method to set multiple contexts
        Campaign : Campaign metadata model used for validation and loading
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
                self.current_campaign_name, None
            )
        ) is None:
            current_campaign_dir = self.current_station_dir.add_campaign(
                name=campaign_id
            )
        self.current_campaign_dir = current_campaign_dir
        self.current_campaign_dir.build()

    def set_network_station_campaign(
        self,
        network_id: str,
        station_id: str | None = None,
        campaign_id: str | None = None,
    ):
        """
        Set the current network, and optionally station and campaign contexts.

        Convenience method that establishes hierarchical context up to whichever
        level is provided. Omitted levels are left unchanged (or cleared if a
        higher-level context changes).

        Parameters
        ----------
        network_id : str
            The identifier for the network to activate.
        station_id : str, optional
            The identifier for the station to activate. If None, only the
            network context is set.
        campaign_id : str, optional
            The identifier for the campaign to activate. If None, campaign
            context is not set. Ignored when station_id is also None.

        Raises
        ------
        AssertionError
            If any provided parameter is not a string.

        ValueError
            If any of the individual context setting operations fail.

        See Also
        --------
        set_network : Set only the network context
        set_station : Set only the station context
        set_campaign : Set only the campaign context
        """
        assert isinstance(network_id, str), "network_id must be a string"
        assert station_id is None or isinstance(station_id, str), "station_id must be a string or None"
        assert campaign_id is None or isinstance(campaign_id, str), "campaign_id must be a string or None"

        if network_id != self.current_network_name:
            self.set_network(network_id=network_id)
        if station_id is not None and station_id != self.current_station_name:
            self.set_station(station_id=station_id)
        if campaign_id is not None and campaign_id != self.current_campaign_name:
            self.set_campaign(campaign_id=campaign_id)

    @validate_network_station_campaign
    def set_survey(self, survey_id: str):
        """
        Set the current survey context within the current campaign.

        Establishes the survey-level context, which represents an individual
        measurement session within a campaign. Surveys are the finest-grained
        organizational unit and contain the actual observational data and
        processing results.

        This method is only available for mid-process workflows as it requires
        access to campaign metadata to validate survey existence and load
        survey-specific configuration parameters.

        Parameters
        ----------
        survey_id : str
            The identifier for the survey to activate. This should match the
            survey ID defined in the current campaign's metadata and correspond
            to the survey directory name.

        Raises
        ------
        AssertionError
            If mid_process_workflow=False (survey context only available for
            mid-process workflows), or if survey_id is not a string.

        ValueError
            If the survey is not found in the current campaign metadata, or if
            the survey directory cannot be created or accessed.

        Notes
        -----
        The method performs these operations in sequence:

        1. Validate mid_process_workflow=True and survey_id is string
        2. Reset survey context via _reset_survey()
        3. Search current campaign metadata for matching survey
        4. Load Survey object into current_survey_metadata
        5. Retrieve or create SurveyDir object from current_campaign_dir
        6. Store the SurveyDir in current_survey_dir

        The decorator @validate_network_station_campaign ensures that network,
        station, and campaign contexts are properly established before this
        method can be called.

        Survey metadata includes measurement timestamps, instrument configurations,
        data quality parameters, and processing settings specific to that
        measurement session.

        See Also
        --------
        set_network_station_campaign : Must establish these contexts first
        validate_network_station_campaign : Decorator ensuring proper context
        Survey : Survey metadata model used for validation and loading
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

        if (current_survey_dir := self.current_campaign_dir.surveys.get(survey_id, None)) is None:
            current_survey_dir = self.current_campaign_dir.add_survey(name=survey_id)
        self.current_survey_dir = current_survey_dir
        self.current_survey_dir.build()
