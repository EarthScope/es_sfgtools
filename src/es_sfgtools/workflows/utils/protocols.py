from typing import Callable, Concatenate, Optional, Protocol,TypeVar, ParamSpec
from functools import wraps
from abc import ABC
from pathlib import Path

from es_sfgtools.config import Environment, WorkingEnvironment
from es_sfgtools.data_mgmt.directorymgmt import DirectoryHandler, CampaignDir, SurveyDir, NetworkDir, TileDBDir, StationDir
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
    DirectoryHandler : Core directory management functionality
    PreProcessCatalogHandler : Asset catalog management
    Site : Station metadata model
    Campaign : Campaign metadata model
    Survey : Survey metadata model
    """
    mid_process_workflow: bool = False

    def __init__(self,
                 directory:Path=None,
                 asset_catalog:Optional[PreProcessCatalogHandler]=None,
                 directory_handler:Optional[DirectoryHandler]=None,
                 station_metadata:Optional[Site]=None):
        """
        Initialize the workflow with directory structure and handlers.

        Sets up the basic infrastructure for workflow operations including directory
        management, asset catalogs, and hierarchical context tracking. All context
        attributes are initialized to None and must be set using the appropriate
        context methods.

        Parameters
        ----------
        directory : Path, optional
            Root directory path for the workflow. If not provided, must supply
            directory_handler. Used to initialize DirectoryHandler if needed.
        asset_catalog : PreProcessCatalogHandler, optional
            Pre-configured asset catalog handler. If not provided, will be created
            automatically using the directory handler's asset catalog database path.
        directory_handler : DirectoryHandler, optional
            Pre-configured directory handler. If not provided, will be created from
            the directory parameter and built automatically.
        station_metadata : Site, optional
            Pre-loaded station metadata for workflows that start with known station
            context. Typically used in mid-process workflows.

        Raises
        ------
        ValueError
            If both directory and directory_handler are None. At least one must be
            provided to establish the workflow's file system context.

        Notes
        -----
        The initialization process follows this sequence:

        1. Validate that either directory or directory_handler is provided
        2. Create DirectoryHandler if not supplied and build directory structure
        3. Create PreProcessCatalogHandler if not supplied
        4. Store all handlers and directory references
        5. Initialize all hierarchical context attributes to None

        All hierarchical context attributes (network, station, campaign, survey) are
        set to None during initialization. Use the set_* methods to establish the
        working context before performing workflow operations.

        The directory handler is automatically built during initialization if created
        from a directory parameter, ensuring the full directory structure exists
        before workflow operations begin.
        """

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
        """
        Reset the survey-level context to None.

        Clears the current survey context by setting survey name, directory handler,
        and metadata to None. This method is called automatically when higher-level
        contexts (campaign, station, or network) are changed to prevent orphaned
        survey contexts.

        Notes
        -----
        This is a private method used internally by the context management system.
        It ensures that survey context is properly cleared when parent contexts
        change, maintaining the hierarchical integrity of the workflow state.

        The method clears:
        - current_survey_name
        - current_survey_metadata  
        - current_survey_dir

        See Also
        --------
        _reset_campaign : Calls this method when resetting campaign context
        _reset_station : Calls this method via _reset_campaign
        _reset_network : Calls this method via _reset_station
        """
        self.current_survey_name = None
        self.current_survey_metadata = None
        self.current_survey_dir = None

    def _reset_campaign(self) -> None:
        """
        Reset the campaign-level context and all dependent survey context.

        Clears the current campaign context and automatically resets the survey
        context via _reset_survey(). This ensures that when a campaign changes,
        no orphaned survey context remains from the previous campaign.

        Notes
        -----
        This is a private method used internally by the context management system.
        It maintains the hierarchical integrity by cascading the reset operation
        to all dependent contexts.

        The method clears:
        - current_campaign_name
        - current_campaign_metadata
        - current_campaign_dir
        - All survey-level context (via _reset_survey)

        This method is called automatically when:
        - Station context changes (via _reset_station)
        - Network context changes (via _reset_network)
        - Campaign context is explicitly changed (via set_campaign)

        See Also
        --------
        _reset_survey : Called by this method to clear survey context
        _reset_station : Calls this method when resetting station context
        _reset_network : Calls this method via _reset_station
        """
        self.current_campaign_name = None
        self.current_campaign_metadata = None
        self.current_campaign_dir = None
        self._reset_survey()

    def _reset_station(self) -> None:
        """
        Reset the station-level context and all dependent campaign and survey contexts.

        Clears the current station context and automatically resets all dependent
        contexts (campaign and survey) via _reset_campaign(). This ensures that
        when a station changes, no orphaned lower-level context remains from the
        previous station.

        Notes
        -----
        This is a private method used internally by the context management system.
        It maintains the hierarchical integrity by cascading the reset operation
        to all dependent contexts in the proper order.

        The method clears:
        - current_station_name
        - current_station_dir
        - current_station_metadata
        - All campaign-level context (via _reset_campaign)
        - All survey-level context (via _reset_campaign → _reset_survey)

        This method is called automatically when:
        - Network context changes (via _reset_network)
        - Station context is explicitly changed (via set_station)

        The station metadata is particularly important in mid-process workflows
        as it contains critical information about transponder configurations,
        site parameters, and campaign definitions required for scientific processing.

        See Also
        --------
        _reset_campaign : Called by this method to clear campaign/survey context
        _reset_network : Calls this method when resetting network context
        set_station : Calls this method before setting new station context
        """
        self.current_station_name = None
        self.current_station_dir = None
        self.current_station_metadata = None
        self._reset_campaign()

    def _reset_network(self) -> None:
        """
        Reset the network-level context and all dependent contexts.

        Clears the current network context and automatically resets all dependent
        contexts (station, campaign, and survey) via _reset_station(). This ensures
        complete context isolation when switching between different networks.

        Notes
        -----
        This is a private method used internally by the context management system.
        It represents the top level of the hierarchical reset cascade and ensures
        that all lower-level contexts are properly cleared when the network changes.

        The method clears:
        - current_network_name
        - current_network_dir
        - All station-level context (via _reset_station)
        - All campaign-level context (via _reset_station → _reset_campaign)  
        - All survey-level context (via _reset_station → _reset_campaign → _reset_survey)

        This method is called automatically when:
        - Network context is explicitly changed (via set_network)

        The network represents the highest level of organization in the seafloor
        geodesy data hierarchy and typically corresponds to a geographic region
        or institutional boundary containing multiple seafloor sites.

        See Also
        --------
        _reset_station : Called by this method to clear all station-dependent context
        set_network : Calls this method before setting new network context
        """
        self.current_network_name = None
        self.current_network_dir = None
        self._reset_station()

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
        3. Retrieve existing NetworkDir or create new one via directory_handler
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
            current_network_dir := self.directory_handler.networks.get(
                self.current_network_name, None
            )
        ) is None:
            current_network_dir = self.directory_handler.add_network(
                name=self.current_network_name
            )
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
            self.current_station_metadata = Site.from_json(
                self.current_station_dir.site_metadata
            )

        if self.mid_process_workflow:
            # Load site metadata for mid-process workflows
            assert self.current_station_metadata is not None, f"Site metadata file not found for station {station_id}, cannot proceed with mid-process workflow."
        

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
        self, network_id: str, station_id: str, campaign_id: str
    ):
        """
        Set the current network, station, and campaign contexts in sequence.

        Convenience method that establishes the complete hierarchical context
        (network → station → campaign) in a single call. This is equivalent to
        calling set_network(), set_station(), and set_campaign() in sequence,
        but with parameter validation to ensure all are strings.

        Parameters
        ----------
        network_id : str
            The identifier for the network to activate. This should match the
            expected directory name and serve as a unique network identifier.
        station_id : str
            The identifier for the station to activate. This should match both
            the directory name and station identifier in metadata files.
        campaign_id : str
            The identifier for the campaign to activate. This should match both
            the directory name and campaign identifier in station metadata.

        Raises
        ------
        AssertionError
            If any of the input parameters are not strings.

        ValueError
            If any of the individual context setting operations fail. Error
            messages will indicate which specific context failed to set.

        Notes
        -----
        The method performs parameter validation before calling the individual
        context setting methods:

        1. Validate all parameters are strings
        2. Call set_network(network_id)
        3. Call set_station(station_id)  
        4. Call set_campaign(campaign_id)

        This method is particularly useful for workflows that need to establish
        a known hierarchical context at startup, such as processing workflows
        that operate on specific campaign data.

        All the same rules and behaviors of the individual set_* methods apply,
        including metadata loading for mid-process workflows and directory
        creation for setup workflows.

        See Also
        --------
        set_network : First step in the sequence
        set_station : Second step in the sequence
        set_campaign : Third step in the sequence
        """
        assert isinstance(network_id, str), "network_id must be a string"
        assert isinstance(station_id, str), "station_id must be a string"
        assert isinstance(campaign_id, str), "campaign_id must be a string"

        if network_id != self.current_network_name:
            self.set_network(network_id=network_id)
        if station_id != self.current_station_name:
            self.set_station(station_id=station_id)
        if campaign_id != self.current_campaign_name:
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

        if (
            current_survey_dir := self.current_campaign_dir.surveys.get(survey_id, None)
        ) is None:
            current_survey_dir = self.current_campaign_dir.add_survey(name=survey_id)
        self.current_survey_dir = current_survey_dir
        self.current_survey_dir.build()