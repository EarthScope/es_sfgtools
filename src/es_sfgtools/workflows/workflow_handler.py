from pathlib import Path
from typing import (
    List,
    Literal,
    Optional,
    Union,
)


from es_sfgtools.config.file_config import DEFAULT_FILE_TYPES_TO_DOWNLOAD
from es_sfgtools.modeling.garpos_tools.schemas import InversionParams
from es_sfgtools.workflows.pipelines.qc_pipeline import QCPipeline

from es_sfgtools.data_mgmt.assetcatalog.schemas import AssetEntry, AssetType
from es_sfgtools.workflows.midprocess.mid_processing import IntermediateDataProcessor
from es_sfgtools.workflows.modeling.garpos_handler import GarposHandler

from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.logging import change_all_logger_dirs

from es_sfgtools.workflows.pipelines.sv3_pipeline import SV3Pipeline
from es_sfgtools.workflows.pipelines import exceptions as pipeline_exceptions

from es_sfgtools.workflows.pipelines.config import (
    SV3PipelineConfig,
    PrideCLIConfig,
    NovatelConfig,
    RinexConfig,
    DFOP00Config,
    PositionUpdateConfig,
)


from es_sfgtools.utils.model_update import validate_and_merge_config


from es_sfgtools.workflows.utils.protocols import WorkflowABC, validate_network_station_campaign
from es_sfgtools.workflows.preprocess_ingest.data_handler import DataHandler
from es_sfgtools.config.env_config import ( Environment, WorkingEnvironment)

pipeline_jobs = [
    "all",
    "process_novatel",
    "build_rinex",
    "run_pride",
    "process_kinematic",
    "process_dfop00",
    "refine_shotdata",
    "process_svp",
]


class WorkflowHandler(WorkflowABC):
    """
    Handles data operations including searching, adding, downloading, and processing.
    
    This class extends WorkflowABC to provide comprehensive workflow management
    capabilities including data ingestion, processing pipelines, and analysis
    tools for seafloor geodesy workflows.
    """

    def __init__(
        self,
        directory: Path | str = None,
    ) -> None:
        """Initializes the WorkflowHandler with directory structure and handlers.

        Sets up the workflow infrastructure and creates a DataHandler instance
        for data operations.

        Parameters
        ----------
        directory : Path | str
            The root directory for data storage and operations.
        """
        Environment.load_working_environment()
        if directory is None:
            assert Environment.working_environment() == WorkingEnvironment.GEOLAB, "Directory must be provided unless in GEOLAB environment"
            directory = Environment.main_directory_GEOLAB()

        # Create DataHandler instance for data operations
        self.data_handler = DataHandler(directory=directory)

        # Initialize parent WorkflowABC with directory
        super().__init__(directory=directory)

    def set_network_station_campaign(
        self, network_id: str, station_id: str, campaign_id: str
    ):
        """Sets the current network, station, and campaign.

        Delegates to DataHandler which handles both its own setup and parent
        context switching. Then syncs WorkflowHandler-specific state.

        Parameters
        ----------
        network_id : str
            The ID of the network to set.
        station_id : str
            The ID of the station to set.
        campaign_id : str
            The ID of the campaign to set.
        """
        # DataHandler handles both its setup AND parent context switching
        self.data_handler.set_network_station_campaign(network_id, station_id, campaign_id)

        # Sync WorkflowHandler state from DataHandler
        for key,value in self.data_handler.__dict__.items():
            if value is not None and hasattr(self,key):
                setattr(self,key,value)
                logger.logdebug(f"WorkflowHandler state updated: {key} = {value}")

        self._geolab_s3_synced = False

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
        self.data_handler.download_data(file_types=file_types)

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
            directory_handler=self.data_handler.directory_handler, config=base_config_updated
        )
        pipeline.set_network_station_campaign(
            network_id=self.current_network_name,
            station_id=self.current_station_name,
            campaign_id=self.current_campaign_name,
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
        assert job in pipeline_jobs, f"Job must be one of {pipeline_jobs}"

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
        """Loads and returns the site metadata for the current station. Sets the current_station_metadata attribute.

        1. If site_metadata is None, attempts to load from data_handler's current_station_metadata.
        2. If site_metadata is a string or Path, loads the site metadata from the file.
        3. If site_metadata is already a Site instance, uses it directly.

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
        if site_metadata is None:
            if self.data_handler.current_station_metadata is not None:
                site_metadata = self.data_handler.current_station_metadata
            else:
                site_metadata: Union[Site, None] = self.data_handler.get_site_metadata(
                    site_metadata=site_metadata
                )

        elif isinstance(site_metadata, (str, Path)):
            site_metadata = Site.from_json(site_metadata)

        else:
            assert isinstance(site_metadata, Site), "site_metadata must be of type Site if not a str or Path"   

        if site_metadata is None:
            raise ValueError("Site metadata not loaded or provided, cannot proceed")

        self.current_station_metadata = site_metadata
        return self.current_station_metadata

    @validate_network_station_campaign
    def midprocess_get_processor(
        self, site_metadata: Optional[Union[Site, str]] = None, override_metadata_require: bool = False
    ) -> IntermediateDataProcessor:
        """Returns an instance of the IntermediateDataProcessor for the current station.

        Parameters
        ----------
        site_metadata : Optional[Union[Site, str]], optional
            Optional site metadata or path to metadata file. If not provided, it will be loaded if available.
        override_metadata_require : bool, optional
            If True, bypasses the requirement for loaded site metadata, by default False.

        Returns
        -------
        IntermediateDataProcessor
            An instance of IntermediateDataProcessor.

        Raises
        ------
        ValueError
            If site metadata is not loaded and override_metadata_require is False.
        """
        if not override_metadata_require:
            # Ensure site metadata is loaded
            self.midprocess_get_sitemeta(site_metadata=site_metadata)

        if self.current_station_metadata is None:
            raise ValueError("Station metadata must be loaded before initializing IntermediateDataProcessor.")
        dataPostProcessor = IntermediateDataProcessor(
            station_metadata=self.current_station_metadata,
            directory_handler=self.data_handler.directory_handler,
        )
        dataPostProcessor.mid_process_workflow = not override_metadata_require
        dataPostProcessor.set_network(network_id=self.current_network_name)
        dataPostProcessor.set_station(station_id=self.current_station_name)
        dataPostProcessor.set_campaign(campaign_id=self.current_campaign_name)

        return dataPostProcessor

    @validate_network_station_campaign
    def midprocess_parse_surveys(
        self,
        site_metadata: Optional[Union[Site, str]] = None,
        override: bool = False,
        write_intermediate: bool = False,
        survey_id: Optional[str] = None,
    ) -> IntermediateDataProcessor:
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
        if Environment.working_environment() == WorkingEnvironment.GEOLAB:
            self.data_handler.geolab_get_s3(overwrite=override)
            for key, value in self.data_handler.__dict__.items():
                if value is not None and hasattr(self, key):
                    setattr(self, key, value)
                    logger.logdebug(f"WorkflowHandler state updated: {key} = {value}")

        dataPostProcessor: IntermediateDataProcessor = self.midprocess_get_processor(site_metadata=site_metadata)
        dataPostProcessor.parse_surveys(
            survey_id=survey_id,
            override=override,
            write_intermediate=write_intermediate,
        )
        return dataPostProcessor

    @validate_network_station_campaign
    def midprocess_prep_garpos(
        self,
        site_metadata: Optional[Union[Site, str]] = None,
        survey_id: Optional[str] = None,
        custom_filters: Optional[dict] = None,
        override: bool = False,
        override_survey_parsing: bool = False,
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
        dataPostProcessor: IntermediateDataProcessor = self.midprocess_parse_surveys(
            site_metadata=site_metadata,
            override=override_survey_parsing,
            write_intermediate=write_intermediate,
            survey_id=survey_id,
        )
        dataPostProcessor.prepare_shotdata_garpos(
            survey_id=survey_id,
            custom_filters=custom_filters,
            overwrite=override,
        )

    @validate_network_station_campaign
    def midprocess_upload_s3(
        self, overwrite: bool = False, override_metadata_require: bool = False
    ) -> None:
        """Uploads intermediate processed data to S3 for the current station.
        Parameters
        ----------
        overwrite : bool, optional
            If True, overwrites existing data on S3, by default False.
        override_metadata_require : bool, optional
            If True, bypasses the requirement for loaded site metadata, by default False.

        Raises
        ------
        ValueError
            If site metadata is not loaded and ``override_metadata_require`` is False.
        """
        dataPostProcessor: IntermediateDataProcessor = self.midprocess_get_processor(self.current_station_metadata, override_metadata_require=override_metadata_require)
        dataPostProcessor.midprocess_sync_s3(overwrite=overwrite)

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
        if self.current_station_metadata is None:
            raise ValueError("Site metadata not loaded, cannot get GarposHandler")

        gp_handler = GarposHandler(
            directory_handler=self.data_handler.directory_handler,
            station_metadata=self.current_station_metadata,
        )
        gp_handler.set_network_station_campaign(
            network_id=self.current_network_name,
            station_id=self.current_station_name,
            campaign_id=self.current_campaign_name,
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
    def modeling_plot_shotdata_replies_per_transponder(
        self,
        survey_id: Optional[str] = None,
        save_fig: bool = True,
        show_fig: bool = False,
    ) -> None:
        """Plots the shot data replies per transponder for a given survey.

        Parameters
        ----------
        survey_id : str, optional
            ID of the survey to plot results for, by default None.
        save_fig : bool, optional
            If True, save the figure, by default True.
        show_fig : bool, optional
            If True, display the figure, by default False.
        """
        gp_handler = self.modeling_get_garpos_handler()
        gp_handler.plot_shotdata_replies_per_transponder(
            savefig=save_fig,
            showfig=show_fig,
        )

    @validate_network_station_campaign
    def modeling_plot_flagged_residuals(
        self,
        survey_id: Optional[str] = None,
        run_id: str = "Test",
        save_fig: bool = True,
        show_fig: bool = False,
    ) -> None:
        """Plots the flagged residuals for a given survey.

        Parameters
        ----------
        survey_id : str, optional
            ID of the survey to plot results for, by default None.
        run_id : int or str, optional
            The run ID of the survey results to plot, by default 0.
        save_fig : bool, optional
            If True, save the figure, by default True.
        show_fig : bool, optional
            If True, display the figure, by default False.
        """
        gp_handler = self.modeling_get_garpos_handler()
        gp_handler.plot_residuals_per_transponder_before_and_after(
            survey_id=survey_id,
            run_id=run_id,
            savefig=save_fig,
            showfig=show_fig,
        )

    @validate_network_station_campaign
    def modeling_plot_garpos_residuals(
        self,
        survey_id: Optional[str] = None,
        run_id: str = "Test",
        subplots: bool = True,
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
        gp_handler.plot_remaining_residuals_per_transponder(
            survey_id=survey_id,
            run_id=run_id,
            subplots=subplots,
            savefig=save_fig,
            showfig=show_fig,
        )

    @validate_network_station_campaign
    def qc_get_pipeline(self, config: dict = {}) -> "QCPipeline":
        """Get a configured QCPipeline instance.

        Parameters
        ----------
        config : dict, optional
            A dictionary of configuration options, by default {}

        Returns
        -------
        QCPipeline
            A configured QCPipeline instance.
        """

        qc_pipeline:QCPipeline = QCPipeline(
            directory_handler=self.directory_handler,
            asset_catalog=self.asset_catalog,
            config=config,
        )
        qc_pipeline.set_network_station_campaign(
            network_id=self.current_network_name,
            station_id=self.current_station_name,
            campaign_id=self.current_campaign_name,
        )
        return qc_pipeline

    @validate_network_station_campaign
    def qc_process_and_model(self,
                             site_metadata: Optional[Union[Site, str]] = None,
                             run_id:str|int = 0,
                             iterations:int = 1,
                             garpos_settings: Optional[dict | InversionParams] = None,
                             garpos_override: bool = False,
                             pre_process_config: dict = {},
                             ) -> None:
        """Process QC files and run GARPOS modeling.

        Parameters
        ----------
        pre_process_config : dict, optional
            A dictionary of configuration options, by default {}
        run_id : str or int, optional
            Identifier for the GARPOS run, by default 0.
        iterations : int, optional
            Number of GARPOS iterations to perform, by default 1.
        garpos_settings : Optional[dict | InversionParams], optional
            Custom settings to override GARPOS defaults, by default None.
        garpos_override : bool, optional
            If True, re-runs GARPOS even if results exist, by default False.
        
        Raises
        ------
        ValueError
            If site metadata is not provided and cannot be loaded.
        """
        # Get and run the QC pipeline
        qc_pipeline: QCPipeline = self.qc_get_pipeline(config=pre_process_config)
        qc_pipeline.process_qc_files()

        # Get the intermediate data processor and parse QC surveys
        try:
            qc_mid_processor = self.midprocess_get_processor(site_metadata=site_metadata)
        except ValueError as e:
            raise e # for visibility
        
        gp_dir_list = qc_mid_processor.parse_surveys_qc(
            shotdata_uri=qc_pipeline.shotDataTDB.uri
        )

        # Get the GARPOS handler and run GARPOS
        qc_garpos_handler = self.modeling_get_garpos_handler()
        qc_garpos_handler.current_campaign_dir.location = (
            qc_garpos_handler.current_campaign_dir.qc
        )
        qc_garpos_handler.current_campaign_dir.build()
        qc_garpos_handler.run_garpos(surveys=gp_dir_list,
                                     run_id=run_id,
                                     iterations=iterations,
                                     override=garpos_override,
                                     custom_settings=garpos_settings,
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
