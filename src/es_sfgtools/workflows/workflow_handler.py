"""Contains the DataHandler class for handling data operations."""

import concurrent.futures
import os
import threading
import warnings
from functools import wraps
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

import boto3
import seaborn
from tqdm.auto import tqdm
import json

from es_sfgtools.data_mgmt.assetcatalog.catalog import PreProcessCatalog
from es_sfgtools.data_mgmt.constants import DEFAULT_FILE_TYPES_TO_DOWNLOAD, REMOTE_TYPE
from es_sfgtools.data_mgmt.ingestion.datadiscovery import (
    get_file_type_local,
    get_file_type_remote,
    scrape_directory_local,
)
from es_sfgtools.data_mgmt.directorymgmt.directory_handler import (
    CampaignDir,
    DirectoryHandler,
    NetworkDir,
    StationDir,
    SurveyDir,
)
from es_sfgtools.data_mgmt.assetcatalog.file_schemas import AssetEntry, AssetType
from es_sfgtools.data_mgmt.post_processing import (
    IntermediateDataProcessor,
)
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.logging import change_all_logger_dirs
from es_sfgtools.modeling.garpos_tools.garpos_handler import GarposHandler
from es_sfgtools.pipelines.sv3_pipeline import SV3Pipeline
from es_sfgtools.pipelines import exceptions as pipeline_exceptions

from es_sfgtools.pipelines.config import (
    SV3PipelineConfig,
    PrideCLIConfig,
    NovatelConfig,
    RinexConfig,
    DFOP00Config,
    PositionUpdateConfig,
)
from es_sfgtools.tiledb_tools.tiledb_schemas import (
    TDBAcousticArray,
    TDBGNSSObsArray,
    TDBIMUPositionArray,
    TDBKinPositionArray,
    TDBShotDataArray,
)
from es_sfgtools.data_mgmt.ingestion.archive_pull import (
    download_file_from_archive,
    list_campaign_files,
    load_site_metadata,
)
from es_sfgtools.utils.model_update import validate_and_merge_config

from es_sfgtools.data_mgmt.data_handler import check_network_station_campaign, DataHandler

seaborn.set_theme(style="whitegrid")

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


class WorkflowHandler:
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

        self.current_network: Optional[str] = None
        self.current_station: Optional[str] = None
        self.current_campaign: Optional[str] = None

        self.currentNetworkDir: Optional[NetworkDir] = None
        self.currentStationDir: Optional[StationDir] = None
        self.currentCampaignDir: Optional[CampaignDir] = None
        self.currentSurveyDir: Optional[SurveyDir] = None

        self.currentSiteMetaData: Optional[Site] = None

        self.data_handler = DataHandler(directory=directory)


    def change_working_station(
        self,
        network: str,
        station: str,
        campaign: str,
    ):
        """Changes the operational context to a specific network, station, and campaign.

        Parameters
        ----------
        network : str
            The network identifier.
        station : str
            The station identifier.
        campaign : str
            The campaign identifier.

        Raises
        ------
        AssertionError
            If the station,campaign, or network is not a non-empty string.
        Warning
            If site metadata is not found for the specified network and station.
        """
        assert (
            isinstance(network, str) and network is not None
        ), "Network must be a non-empty string"
        assert (
            isinstance(station, str) and station is not None
        ), "Station must be a non-empty string"
        assert (
            isinstance(campaign, str) and campaign is not None
        ), "Campaign must be a non-empty string"

        self.data_handler.change_working_station(
            network=network,
            station=station,
            campaign=campaign,
        )
        self.current_network = self.data_handler.current_network
        self.current_station = self.data_handler.current_station
        self.current_campaign = self.data_handler.current_campaign
        self.currentNetworkDir = self.data_handler.currentNetworkDir
        self.currentStationDir = self.data_handler.currentStationDir
        self.currentCampaignDir = self.data_handler.currentCampaignDir
        self.currentSiteMetaData = self.data_handler.currentSiteMetaData

        if self.currentSiteMetaData is None:
            message = f"No site metadata found for {self.current_network} {self.current_station}. Some processing steps may fail."
            logger.logwarn(message)
            raise Warning(message)

    @check_network_station_campaign
    def ingest_add_local_data(self, directory_path: Path) -> None:
        """Scans a directory for data files and adds them to the catalog.

        Parameters
        ----------
        directory_path : Path
            The path to the directory to scan.
        """

        self.data_handler.discover_data_and_add_files(directory_path=directory_path)

    @check_network_station_campaign
    def ingest_get_archive_data(self) -> None:
        """
        Downloads and catalogs data from the remote archive for the current network, station, and campaign.
        """
        self.data_handler.update_catalog_from_archive()

    @check_network_station_campaign
    def ingest_download_archive_data(self):
        """
        Downloads data files from the Earthscope archive based on the current catalog entries.
        """
        self.data_handler.download_data()

    @check_network_station_campaign
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
        pipeline.setNetworkStationCampaign(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign,
        )
        return pipeline

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

    @check_network_station_campaign
    def midprocess_get_sitemeta(
        self, site_metadata: Optional[Union[Site, str]] = None
    ) -> Site:
        """Loads and returns the site metadata for the current station.

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

    @check_network_station_campaign
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
        dataPostProcessor.setNetwork(network_id=self.current_network)
        dataPostProcessor.setStation(station_id=self.current_station)
        dataPostProcessor.setCampaign(campaign_id=self.current_campaign)

        return dataPostProcessor

    @check_network_station_campaign
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
            If True, writes intermediate files, by default False.

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

    @check_network_station_campaign
    def midprocess_prep_garpos(
        self,
        site_metadata: Optional[Union[Site, str]] = None,
        survey_id: Optional[str] = None,
        custom_filters: dict = None,
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

    def print_logs(self, log: Literal["base", "gnss", "process"]):
        """Prints the specified log to the console.

        Parameters
        ----------
        log : Literal["base", "gnss", "process"]
            The type of log to print.

        Raises
        ------
        ValueError
            If the specified log type is not recognized.
        """
        if log == "base":
            logger.route_to_console()
        elif log == "gnss":
            pass  # GNSS logger not implemented yet
        elif log == "process":
            pass  # Process logger not implemented yet
        else:
            raise ValueError(
                f"Log type {log} not recognized. Must be one of ['base','gnss','process']"
            )

    @check_network_station_campaign
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
        gp_handler.setNetworkStationCampaign(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign,
        )
        return gp_handler
    
    @check_network_station_campaign
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