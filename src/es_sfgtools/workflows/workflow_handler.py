from pathlib import Path
from typing import (
    Callable,
    Concatenate,
    List,
    Literal,
    Optional,
    ParamSpec,
    Protocol,
    Tuple,
    TypeVar,
    Union,
)

from es_sfgtools.data_mgmt.assetcatalog.handler import PreProcessCatalogHandler
from es_sfgtools.data_mgmt.utils import validate_network_station_campaign
from es_sfgtools.data_models.metadata.campaign import Campaign, Survey
import seaborn
from tqdm.auto import tqdm

from es_sfgtools.data_mgmt.config import DEFAULT_FILE_TYPES_TO_DOWNLOAD, REMOTE_TYPE

from es_sfgtools.data_mgmt.directorymgmt.handler import (
    CampaignDir,
    DirectoryHandler,
    NetworkDir,
    StationDir,
    SurveyDir,
)
from es_sfgtools.data_mgmt.assetcatalog.schemas import AssetEntry, AssetType
from es_sfgtools.data_mgmt.post_processing import (
    IntermediateDataProcessor,
)
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.logging import change_all_logger_dirs
from es_sfgtools.modeling.garpos_tools.garpos_handler import GarposHandler
from es_sfgtools.workflows.preprocess_ingest.pipelines.sv3_pipeline import SV3Pipeline
from es_sfgtools.workflows.preprocess_ingest.pipelines import exceptions as pipeline_exceptions

from es_sfgtools.workflows.preprocess_ingest.pipelines.config import (
    SV3PipelineConfig,
    PrideCLIConfig,
    NovatelConfig,
    RinexConfig,
    DFOP00Config,
    PositionUpdateConfig,
)

from es_sfgtools.data_mgmt.ingestion.archive_pull import (
    download_file_from_archive,
    list_campaign_files,
    load_site_metadata,
)
from es_sfgtools.utils.model_update import validate_and_merge_config

from es_sfgtools.workflows.preprocess_ingest.pipelines.sv3_pipeline import SV3Pipeline
from es_sfgtools.workflows.config.protocols import validate_network_station_campaign, MidProcessIngestProtocol
from es_sfgtools.workflows.preprocess_ingest.data_handler import DataHandler


pipeline_jobs = Literal[
    "all",
    "process_novatel",
    "build_rinex",
    "run_pride",
    "process_kinematic",
    "process_dfop00",
    "refine_shotdata",
    "process_svp",
]


class WorkflowHandler(MidProcessIngestProtocol):
    """
    Handles data operations including searching, adding, downloading, and processing.
    """

    def __init__(
        self,
        directory: Path | str,
    ) -> None:
        """Initializes the DataHandler, setting up directories and the processing catalog.

        Parameters
        ----------
        directory : Union[Path, str]
            The root directory for data storage and operations.
        """

        self.current_network_name: Optional[str] = None
        self.current_network_dir: Optional[NetworkDir] = None

        self.current_station_name: Optional[str] = None
        self.current_station_dir: Optional[StationDir] = None
        self.current_station_metadata: Optional[Site] = None

        self.current_campaign_name: Optional[str] = None
        self.current_campaign: Optional[Campaign] = None
        self.current_campaign_dir: Optional[CampaignDir] = None

        self.current_survey_name: Optional[str] = None
        self.current_survey_dir: Optional[SurveyDir] = None
        self.current_survey: Optional[Survey] = None

        self.directory_handler: DirectoryHandler = DirectoryHandler(directory=directory)
        self.data_handler = DataHandler(directory=directory)
        self.asset_catalog: PreProcessCatalogHandler = self.data_handler.asset_catalog

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

        # Set current network attributes
        for network_name in self.current_station_meta.networks:
            if network_name == network_id:
                self.current_network = network_name
                break
        if self.current_network is None:
            raise ValueError(f"Network {network_id} not found in site metadata.")

        if (
            current_network_dir := self.directory_handler.networks.get(
                self.current_network, None
            )
        ) is None:
            current_network_dir = self.directory_handler.add_network(
                name=self.current_network
            )
        self.current_network_dir = current_network_dir

    def _reset_network(self) -> None:
        """Resets the current network."""
        self.current_network_name = None
        self.current_network_dir = None
        self._reset_station()

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

        # Set current station attributes
        for station_name in self.current_station_meta.names:
            if station_name == station_id:
                self.current_station = station_name
                break
        if self.current_station is None:
            raise ValueError(f"Station {station_id} not found in site metadata.")

        if (
            current_station_dir := self.current_network_dir.stations.get(
                self.current_station, None
            )
        ) is None:
            current_station_dir = self.current_network_dir.add_station(
                name=self.current_station
            )
        self.current_station_dir = current_station_dir

    def _reset_station(self) -> None:
        """Resets the current station."""
        self.current_station_name = None
        self.current_station_dir = None
        self.current_station_meta = None

        self._reset_campaign()

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

        for campaign in self.current_station_meta.campaigns:
            if campaign.name == campaign_id:
                self.current_campaign = campaign
                self.current_campaign_name = campaign.name
                break
        if self.current_campaign is None:
            raise ValueError(f"Campaign {campaign_id} not found in site metadata.")

        if (
            current_campaign_dir := self.current_station_dir.campaigns.get(
                self.current_campaign.name, None
            )
        ) is None:
            current_campaign_dir = self.current_station_dir.add_campaign(
                name=campaign_id
            )
        self.current_campaign_dir = current_campaign_dir

    def _reset_campaign(self) -> None:
        """Resets the current campaign."""
        self.current_campaign_name = None
        self.current_campaign = None
        self.current_campaign_dir = None

        self._reset_survey()

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

        if self.current_station_meta is None:
    
            message = f"No site metadata found for {self.current_network} {self.current_station}. Some processing steps may fail."
            logger.logwarn(message)
            raise Warning(message)


    @validate_network_station_campaign
    def ingest_add_local_data(self, directory_path: Path) -> None:
        """Scans a directory for data files and adds them to the catalog.

        Parameters
        ----------
        directory_path : Path
            The path to the directory to scan.
        """

        self.data_handler.discover_data_and_add_files(directory_path=directory_path)

    @validate_network_station_campaign
    def ingest_catalog_archive_data(self) -> None:
        """
        Updates the data catalog with the s3 uri's for data hosted in Earthscope's remote archive for the current network, station, and campaign.

        Notes
        -----
        This method does not download any data files. It only updates the catalog with remote file paths. See `ingest_download_archive_data` to download files.

        """
        self.data_handler.update_catalog_from_archive()

    @validate_network_station_campaign
    def ingest_download_archive_data(self,file_types:Optional[List[AssetType] | List[str]]=DEFAULT_FILE_TYPES_TO_DOWNLOAD) -> None:
        """
        Downloads data files from the Earthscope archive based on the current catalog entries. 

        Notes
        -----
        This method requires that the catalog has been populated with remote file paths using `ingest_catalog_archive_data`.
        """
        self.data_handler.download_data()

    @validate_network_station_campaign
    def preprocess_get_pipeline_sv3(
        self,
        primary_config: Optional[
            Union[
                SV3PipelineConfig,
                PrideCLIConfig,
                NovatelConfig,
                RinexConfig,
                DFOP00Config,
                PositionUpdateConfig,
                dict,
            ]
        ] = None,
        secondary_config: Optional[
            Union[
                SV3PipelineConfig,
                PrideCLIConfig,
                NovatelConfig,
                RinexConfig,
                DFOP00Config,
                PositionUpdateConfig,
                dict,
            ]
        ] = None,
    ) -> SV3Pipeline:
        """Creates and configures an SV3 processing pipeline.

        Parameters
        ----------
        primary_config : Optional[Union[SV3PipelineConfig, PrideCLIConfig, NovatelConfig, RinexConfig, DFOP00Config, PositionUpdateConfig, dict]], optional
            Optional primary configuration for the pipeline.
        secondary_config : Optional[Union[SV3PipelineConfig, PrideCLIConfig, NovatelConfig, RinexConfig, DFOP00Config, PositionUpdateConfig, dict]], optional
            Optional secondary configuration for the pipeline.

        Returns
        -------
        SV3Pipeline
            Configured SV3Pipeline instance.

        Raises
        ------
        AssertionError
            If current network, station, or campaign is not set.
        ValueError
            If configuration validation fails.

        See Also
        --------
        es_sfgtools.pipelines.sv3_pipeline.SV3Pipeline : The pipeline class used for processing.
        """

        base_config = SV3PipelineConfig()
        base_config_updated = base_config.model_copy()
        # Merge primary config if provided, overwriting defaults. Also check for misspelled keys
        if primary_config is not None:
            if isinstance(
                primary_config,
                (
                    SV3PipelineConfig,
                    PrideCLIConfig,
                    NovatelConfig,
                    RinexConfig,
                    DFOP00Config,
                    PositionUpdateConfig,
                ),
            ):
                primary_config = primary_config.model_dump()

            base_config_updated = validate_and_merge_config(
                base_class=base_config, override_config=primary_config
            )

        # Merge secondary config if provided, overwriting primary and defaults. Also check for misspelled keys
        if secondary_config is not None:
            if isinstance(
                secondary_config,
                (
                    SV3PipelineConfig,
                    PrideCLIConfig,
                    NovatelConfig,
                    RinexConfig,
                    DFOP00Config,
                    PositionUpdateConfig,
                ),
            ):
                secondary_config = secondary_config.model_dump()
            base_config_updated = validate_and_merge_config(
                base_class=base_config_updated, override_config=secondary_config
            )

        pipeline = SV3Pipeline(
            directory_handler=self.data_handler.directory_handler, config=base_config
        )
        pipeline.set_network_station_campaign(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign,
        )
        return pipeline

    @validate_network_station_campaign
    def preprocess_run_pipeline_sv3(
        self,
        job: Literal[
            "all",
            "process_novatel",
            "build_rinex",
            "run_pride",
            "process_kinematic",
            "process_dfop00",
            "refine_shotdata",
            "process_svp",
        ] = "all",
        primary_config: Optional[
            Union[
                SV3PipelineConfig,
                PrideCLIConfig,
                NovatelConfig,
                RinexConfig,
                DFOP00Config,
                PositionUpdateConfig,
                dict,
            ]
        ] = None,
        secondary_config: Optional[
            Union[
                SV3PipelineConfig,
                PrideCLIConfig,
                NovatelConfig,
                RinexConfig,
                DFOP00Config,
                PositionUpdateConfig,
                dict,
            ]
        ] = None,
    ) -> None:
        """Runs the SV3 processing pipeline with optional configuration overrides.

        This method creates and configures an :class:`~es_sfgtools.pipelines.sv3_pipeline.SV3Pipeline`
        instance using the :attr:`data_handler` to access the directory structure and catalog.

        Parameters
        ----------
        job : Literal["all", "process_novatel", "build_rinex", "run_pride", "process_kinematic", "process_dfop00", "refine_shotdata", "process_svp"], optional
            The specific job to run within the pipeline, by default "all".
        primary_config : Optional[Union[SV3PipelineConfig, dict]], optional
            Primary configuration to override defaults.
        secondary_config : Optional[Union[SV3PipelineConfig, dict]], optional
            Secondary configuration to override primary and defaults.

        Raises
        ------
        AssertionError
            If job is not in valid pipeline jobs.
        ValueError
            If configuration validation fails.

        See Also
        --------
        preprocess_get_pipeline_sv3 : Method that creates the pipeline instance.
        es_sfgtools.pipelines.sv3_pipeline.SV3Pipeline : The pipeline class used.
        es_sfgtools.data_mgmt.data_handler.DataHandler : Data management dependency.

        Examples
        --------
        # Run the sv3 pipeline with custom Novatel processing configuration
        >>> workflow = WorkflowHandler("/path/to/data")
        >>> workflow.change_working_station("network", "station", "campaign")
        >>> workflow.preprocess_run_pipeline_sv3(
        ...     job="process_novatel",
        ...     primary_config={"novatel_config": {"n_processes": 8}}
        ... )
        """
        assert job in ['all', 'process_novatel', 'build_rinex', 'run_pride', 'process_kinematic', 'process_dfop00', 'refine_shotdata', 'process_svp'], f"Job must be one of {pipeline_jobs}"

        pipeline: SV3Pipeline = self.preprocess_get_pipeline_sv3(
            primary_config=primary_config, secondary_config=secondary_config
        )
        match job:

            case "all":
                pipeline.run_pipeline()

            case "process_novatel":
                assert isinstance(
                    primary_config,
                    (type(None), dict, SV3PipelineConfig, NovatelConfig),
                ), "Primary config must be provided and be of type None, dict, SV3PipelineConfig, or NovatelConfig when running process_novatel"
                assert isinstance(
                    secondary_config,
                    (type(None), dict, SV3PipelineConfig, NovatelConfig),
                ), "Secondary config must be of type None, dict, SV3PipelineConfig, or NovatelConfig when running process_novatel"
                try:
                    pipeline.pre_process_novatel()
                except Exception as e:
                    logger.logerr(f"Novatel processing failed: {e}")
                    raise e

            case "build_rinex":
                assert isinstance(
                    primary_config, (type(None), dict, SV3PipelineConfig, RinexConfig)
                ), "Primary config must be provided and be of type None, dict, SV3PipelineConfig, or RinexConfig when running build_rinex"
                assert isinstance(
                    secondary_config, (type(None), dict, SV3PipelineConfig, RinexConfig)
                ), "Secondary config must be of type None, dict, SV3PipelineConfig, or RinexConfig when running build_rinex"
                try:
                    pipeline.get_rinex_files()
                except Exception as e:
                    logger.logerr(f"RINEX file generation failed: {e}")
                    raise e

            case "run_pride":
                assert isinstance(
                    primary_config, (type(None), dict, SV3PipelineConfig, PrideCLIConfig)
                ), "Primary config must be provided and be of type None, dict, SV3PipelineConfig, or PrideCLIConfig when running run_pride"
                assert isinstance(
                    secondary_config,
                    (type(None), dict, SV3PipelineConfig, PrideCLIConfig),
                ), "Secondary config must be of type None, dict, SV3PipelineConfig, or PrideCLIConfig when running run_pride"
                try:
                    pipeline.process_rinex()
                except Exception as e:
                    logger.logerr(f"PRIDE-PPP processing failed: {e}")
                    raise e

            case "process_kinematic":
                assert isinstance(
                    primary_config, (type(None), dict, RinexConfig)
                ), "Primary config must be provided and be of type None, dict, or RinexConfig when running process_kinematic"
                assert isinstance(
                    secondary_config, (type(None), dict, RinexConfig)
                ), "Secondary config must be of type None, dict, or RinexConfig when running process_kinematic"
                try:
                    pipeline.process_kin()
                except Exception as e:
                    logger.logerr(f"Kinematic processing failed: {e}")
                    raise e

            case "process_dfop00":
                assert isinstance(
                    primary_config, (type(None), dict, DFOP00Config)
                ), "Primary config must be provided and be of type None, dict, or DFOP00Config when running process_dfop00"
                assert isinstance(
                    secondary_config, (type(None), dict, DFOP00Config)
                ), "Secondary config must be of type None, dict, or DFOP00Config when running process_dfop00"
                try:
                    pipeline.process_dfop00()
                except Exception as e:
                    logger.logerr(f"DFOP00 processing failed: {e}")
                    raise e

            case "refine_shotdata":
                assert isinstance(
                    primary_config, (type(None), dict, PositionUpdateConfig)
                ), "Primary config must be provided and be of type None, dict, or PositionUpdateConfig when running refine_shotdata"
                assert isinstance(
                    secondary_config, (type(None), dict, PositionUpdateConfig)
                ), "Secondary config must be of type None, dict, or PositionUpdateConfig when running refine_shotdata"
                try:
                    pipeline.update_shotdata()
                except Exception as e:
                    logger.logerr(f"Shotdata refinement failed: {e}")
                    raise e

            case "process_svp":
                assert isinstance(
                    primary_config, (type(None), dict, SV3PipelineConfig)
                ), "Primary config must be provided and be of type None, dict, or SV3PipelineConfig when running process_svp"
                assert isinstance(
                    secondary_config, (type(None), dict, SV3PipelineConfig)
                ), "Secondary config must be of type None, dict, or SV3PipelineConfig when running process_svp"
                try:
                    pipeline.process_svp()
                except Exception as e:
                    logger.logerr(f"SVP processing failed: {e}")
                    raise e

            case _:
                pipeline.run_pipeline()

    @validate_network_station_campaign
    def midprocess_get_sitemeta(
        self, site_metadata: Optional[Union[Site, str]] = None
    ) -> Site:
        """Loads and returns the site metadata for the current station. Sets the currentSiteMetaData attribute.

        Parameters
        ----------
        site_metadata : Optional[Union[Site, str]], optional
            Optional site metadata or path to metadata file. If not provided, it will be loaded if available.

        Returns
        -------
        Site
            The site metadata.

        Raises
        ------
        ValueError
            If site metadata cannot be loaded or is not provided.
        """
        siteMeta: Union[Site, None] = self.data_handler.get_site_metadata(
            site_metadata=site_metadata
        )
        if siteMeta is None:
            raise ValueError("Site metadata not loaded or provided, cannot proceed")
        self.currentSiteMetaData = siteMeta
        return self.currentSiteMetaData

    @validate_network_station_campaign
    def midprocess_get_processor(
        self, site_metadata: Optional[Union[Site, str]] = None
    ) -> IntermediateDataProcessor:
        """Returns an instance of the IntermediateDataProcessor for the current station.

        Returns
        -------
        IntermediateDataProcessor
            An instance of IntermediateDataProcessor.

        Raises
        ------
        ValueError
            If site metadata is not loaded.
        """
        # Ensure site metadata is loaded
        self.midprocess_get_sitemeta(site_metadata=site_metadata)

        dataPostProcessor = IntermediateDataProcessor(
            site=self.currentSiteMetaData,
            directory_handler=self.data_handler.directory_handler,
        )
        dataPostProcessor.set_network(network_id=self.current_network)
        dataPostProcessor.set_station(station_id=self.current_station)
        dataPostProcessor.set_campaign(campaign_id=self.current_campaign)

        return dataPostProcessor

    @validate_network_station_campaign
    def midprocess_parse_surveys(
        self,
        site_metadata: Optional[Union[Site, str]] = None,
        override: bool = False,
        write_intermediate: bool = False,
        survey_id: Optional[str] = None,
    ) -> None:
        """Parses survey data for the current station.

        Parameters
        ----------
        site_metadata : Optional[Union[Site, str]], optional
            Optional site metadata or path to metadata file. If not provided, it will be loaded if available.
        override : bool, optional
            If True, re-parses existing data, by default False.
        write_intermediate : bool, optional
            If True, writes intermediate files to disk, by default False.
        survey_id : Optional[str], optional
            Optional survey identifier to process. If None, processes all surveys, by default None.

        Raises
        ------
        ValueError
            If site metadata is not loaded.
        """
        dataPostProcessor: IntermediateDataProcessor = self.midprocess_get_processor(site_metadata=site_metadata)
        dataPostProcessor.parse_surveys(
            survey_id=survey_id,
            override=override,
            write_intermediate=write_intermediate,
        )

    @validate_network_station_campaign
    def midprocess_prep_garpos(
        self,
        site_metadata: Optional[Union[Site, str]] = None,
        survey_id: Optional[str] = None,
        custom_filters: Optional[dict] = None,
        override: bool = False,
        write_intermediate: bool = False,
    ) -> None:
        """Prepares data for GARPOS processing.

        Parameters
        ----------
        site_metadata : Optional[Union[Site, str]], optional
            Optional site metadata or path to metadata file. If not provided, it will be loaded if available.
        survey_id : Optional[str], optional
            Optional survey identifier to process. If None, processes all surveys, by default None.
        custom_filters : dict, optional
            Custom filter settings for shot data preparation, by default None.
        override : bool, optional
            If True, re-prepares existing data, by default False.
        write_intermediate : bool, optional
            If True, writes intermediate files, by default False.

        Raises
        ------
        ValueError
            If site metadata is not loaded.
        """
        dataPostProcessor: IntermediateDataProcessor = self.midprocess_get_processor(
            site_metadata=site_metadata
        )

        dataPostProcessor.parse_surveys(
            override=override,
            write_intermediate=write_intermediate,
        )
        dataPostProcessor.prepare_shotdata_garpos(
            survey_id=survey_id,
            custom_filters=custom_filters,
            overwrite=override,
        )

    @validate_network_station_campaign
    def modeling_get_garpos_handler(self) -> GarposHandler:
        """Returns an instance of the GarposHandler for the current station.

        Returns
        -------
        GarposHandler
            An instance of GarposHandler.

        Raises
        ------
        ValueError
            If site metadata is not loaded.
        """
        if self.currentSiteMetaData is None:
            raise ValueError("Site metadata not loaded, cannot get GarposHandler")

        gp_handler = GarposHandler(
            directory_handler=self.data_handler.directory_handler,
            site=self.currentSiteMetaData,
        )
        gp_handler.set_network_station_campaign(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign,
        )
        return gp_handler

    @validate_network_station_campaign
    def modeling_run_garpos(
        self,
        survey_id: Optional[str] = None,
        run_id: str = "Test",
        iterations: int = 1,
        override: bool = False,
        custom_settings: Optional[dict] = None,
    ) -> None:
        """Runs GARPOS processing for the current station.

        Parameters
        ----------
        survey_id : Optional[str], optional
            Optional survey identifier to process. If None, processes all surveys, by default None.
        run_id : str
            Identifier for the GARPOS run.
        iterations : int, optional
            Number of GARPOS iterations to perform, by default 1.
        site_metadata : Optional[Union[Site, str]], optional
            Optional site metadata or path to metadata file. If not provided, it will be loaded if available.
        override : bool, optional
            If True, re-runs GARPOS even if results exist, by default False.
        custom_settings : Optional[dict], optional
            Custom settings to override GARPOS defaults, by default None.

        Raises
        ------
        ValueError
            If site metadata is not loaded.
        """
        gp_handler = self.modeling_get_garpos_handler()
        gp_handler.run_garpos(
            survey_id=survey_id,
            run_id=run_id,
            iterations=iterations,
            override=override,
            custom_settings=custom_settings,
        )

    @validate_network_station_campaign
    def modeling_plot_garpos_results(
        self,
        survey_id: Optional[str] = None,
        run_id: str = "Test",
        residuals_filter: Optional[float] = 10,
        save_fig: bool = True,
        show_fig: bool = False,
    ) -> None:
        """Plots the time series results for a given survey.

        Parameters
        ----------
        survey_id : str, optional
            ID of the survey to plot results for, by default None.
        run_id : int or str, optional
            The run ID of the survey results to plot, by default 0.
        res_filter : float, optional
            The residual filter value to filter outrageous values (m), by
            default 10.
        save_fig : bool, optional
            If True, save the figure, by default True.
        show_fig : bool, optional
            If True, display the figure, by default False.
        """
        gp_handler = self.modeling_get_garpos_handler()
        gp_handler.plot_ts_results(
            survey_id=survey_id,
            run_id=run_id,
            res_filter=residuals_filter,
            savefig=save_fig,
            showfig=show_fig,
        )
