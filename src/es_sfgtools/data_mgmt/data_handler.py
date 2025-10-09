""" Contains the DataHandler class for handling data operations. """
import os
import warnings
import concurrent.futures
import json
import threading
from functools import wraps
from pathlib import Path
from typing import Callable, List, Literal, Optional, Tuple, Union, Callable, ParamSpec, TypeVar, Concatenate


import boto3
import matplotlib.pyplot as plt
import seaborn
from tqdm.auto import tqdm

from es_sfgtools.data_mgmt.catalog import PreProcessCatalog
from es_sfgtools.data_mgmt.constants import (DEFAULT_FILE_TYPES_TO_DOWNLOAD,
                                           REMOTE_TYPE)
from es_sfgtools.data_mgmt.datadiscovery import (get_file_type_local,
                                               get_file_type_remote,
                                               scrape_directory_local)
from es_sfgtools.data_mgmt.directory_handler import (CampaignDir,
                                                   DirectoryHandler, NetworkDir,
                                                   StationDir, SurveyDir)
from es_sfgtools.data_mgmt.file_schemas import AssetEntry, AssetType
from es_sfgtools.data_mgmt.post_processing import IntermediateDataProcessor,DEFAULT_FILTER_CONFIG
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.logging import change_all_logger_dirs
from es_sfgtools.modeling.garpos_tools.garpos_handler import GarposHandler
from es_sfgtools.pipelines.sv3_pipeline import SV3Pipeline, SV3PipelineConfig
from es_sfgtools.tiledb_tools.tiledb_schemas import (TDBAcousticArray,
                                                   TDBGNSSObsArray,
                                                   TDBIMUPositionArray,
                                                   TDBKinPositionArray,
                                                   TDBShotDataArray)
from es_sfgtools.utils.archive_pull import (download_file_from_archive,
                                          list_campaign_files,
                                          list_campaign_files_by_type,load_site_metadata)


seaborn.set_theme(style="whitegrid")


P = ParamSpec("P")
R = TypeVar("R")

class HasNetworkStationCampaign(Protocol):
    current_network: Optional[str]
    current_station: Optional[str]
    current_campaign: Optional[str]


def check_network_station_campaign(
    func: Callable[Concatenate[HasNetworkStationCampaign, P], R],
) -> Callable[Concatenate[HasNetworkStationCampaign, P], R]:
    @wraps(func)
    def wrapper(
        self: HasNetworkStationCampaign, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        if self.current_network is None:
            raise ValueError("Network name not set, use change_working_station")
        if self.current_station is None:
            raise ValueError("Station name not set, use change_working_station")
        if self.current_campaign is None:
            raise ValueError("campaign name not set, use change_working_station")
        return func(self, *args, **kwargs)

    return wrapper


class DataHandler:
    """
    Handles data operations including searching, adding, downloading, and processing.
    """

    def __init__(
        self,
        directory: Path | str,
    ) -> None:
        """
        Initializes the DataHandler, setting up directories and the processing catalog.

        :param directory: The root directory for data storage and operations.
        :type directory: Union[Path, str]
        """

        self.current_network: Optional[str] = None
        self.current_station: Optional[str] = None
        self.current_campaign: Optional[str] = None

        self.currentNetworkDir: Optional[NetworkDir] = None
        self.currentStationDir: Optional[StationDir] = None
        self.currentCampaignDir: Optional[CampaignDir] = None
        self.currentSurveyDir: Optional[SurveyDir] = None

        self.currentSiteMetaData: Optional[Site] = None

        # Create the directory structures
        self.main_directory = Path(directory)
        self.directory_handler = DirectoryHandler(location=self.main_directory)
        self.directory_handler.build()

        logger.set_dir(self.main_directory)

        self.catalog = PreProcessCatalog(self.directory_handler.asset_catalog_db_path)

    def _build_station_dir_structure(self, network: str, station: str, campaign: str):
        """
        Constructs the necessary directory structure for a given station and campaign.

        This includes directories for raw data, intermediate files, GARPOS processing,
        logs, and quality control.

        :param network: The name of the network.
        :type network: str
        :param station: The name of the station.
        :type station: str
        :param campaign: The name of the campaign.
        :type campaign: str
        """

        networkDir, stationDir, campaignDir, _ = self.directory_handler.build_station_directory(
            network_name=network, station_name=station, campaign_name=campaign
        )
        self.currentNetworkDir = networkDir
        self.currentStationDir = stationDir
        self.currentCampaignDir = campaignDir

        # Change the logger directory to the campaign log directory
        log_dir = self.currentCampaignDir.log_directory
        change_all_logger_dirs(log_dir)
        os.environ["LOG_FILE_PATH"] = str(log_dir)
        # Log to the new log
        logger.loginfo(f"Built directory structure for {network} {station} {campaign}")

    @check_network_station_campaign
    def _build_tileDB_arrays(self):
        """
        Initializes and consolidates TileDB arrays for the current station.

        This includes arrays for acoustic data, kinematic positions, IMU positions,
        shot data, and GNSS observables.
        """
        logger.loginfo(f"Creating TileDB arrays for {self.current_station}")

        acoustic_tdb_uri = self.currentStationDir.tiledb_directory.acoustic_data
        self.acoustic_tdb = TDBAcousticArray(acoustic_tdb_uri)

        kin_position_tdb_uri = self.directory_handler[self.current_network][self.current_station].tiledb_directory.kin_position_data
        self.kin_position_tdb = TDBKinPositionArray(kin_position_tdb_uri)

        imu_position_tdb_uri = self.currentStationDir.tiledb_directory.imu_position_data
        self.imu_position_tdb = TDBIMUPositionArray(imu_position_tdb_uri)

        shotdata_tdb_uri = self.currentStationDir.tiledb_directory.shot_data

        self.shotdata_tdb = TDBShotDataArray(shotdata_tdb_uri)
        # Use a pre-array for dfo processing, self.shotdata_tdb is where we store the updated version
        shotdata_tdb_uri_pre = self.currentStationDir.tiledb_directory.shot_data_pre
        self.shotdata_tdb_pre = TDBShotDataArray(shotdata_tdb_uri_pre)

        # this is the primary GNSS observables (10hz NOV770 collected on USB3 for SV3, 5hz bcnovatel for SV2)
        gnss_obs_tdb_uri = self.currentStationDir.tiledb_directory.gnss_obs_data
        self.gnss_obs_tdb = TDBGNSSObsArray(
            gnss_obs_tdb_uri
        )  # golang binaries will be used to interact with this array

        # this is the secondary GNSS observables (5hz NOV000 collected on USB2 for SV3)
        # can choose to use this instead of the primary GNSS observables if desired
        gnss_obs_secondary_tdb_uri = self.currentStationDir.tiledb_directory.gnss_obs_data_secondary
        self.gnss_obs_secondary_tdb = TDBGNSSObsArray(
            gnss_obs_secondary_tdb_uri
        )

        logger.loginfo(f"Consolidating existing TileDB arrays for {self.current_station}")
        self.acoustic_tdb.consolidate()
        self.kin_position_tdb.consolidate()
        self.imu_position_tdb.consolidate()
        self.shotdata_tdb.consolidate()
        self.gnss_obs_tdb.consolidate()
        self.gnss_obs_secondary_tdb.consolidate()

    def change_working_station(
        self,
        network: str,
        station: str,
        campaign: str,
        site_metadata: Optional[Union[Site,Path,str]] = None,

    ):
        """
        Changes the operational context to a specific network, station, and campaign.

        :param network: The network identifier.
        :type network: str
        :param station: The station identifier.
        :type station: str
        :param campaign: The campaign identifier.
        :type campaign: str
        :param site_metadata: Optional site metadata. If not provided, it will be loaded if available.
        :type site_metadata: Optional[Site], optional

        """
        assert isinstance(network, str) and network is not None, "Network must be a non-empty string"
        assert isinstance(station, str) and station is not None, "Station must be a non-empty string"
        assert isinstance(campaign, str) and campaign is not None, "Campaign must be a non-empty string"

        assert site_metadata is None or isinstance(site_metadata, (Site, Path, str)), "Site metadata must be a Site, Path, or str"

        getSiteMeta = False
        if (self.current_network != network or self.current_station != station or self.currentSiteMetaData is None):
            getSiteMeta = True

        # Set class attributes & create the directory structure
        self.current_station = station
        self.current_network = network
        self.current_campaign = campaign

        # Build the campaign directory structure and TileDB arrays, this changes the logger directory as well
        self._build_station_dir_structure(network, station, campaign)
        self._build_tileDB_arrays()

        if getSiteMeta or site_metadata is not None:
            # Load site metadata
            self.currentSiteMetaData = self.get_site_metadata(site_metadata=site_metadata)

        logger.loginfo(f"Changed working station to {network} {station} {campaign}")

    @check_network_station_campaign
    def get_dtype_counts(self):
        """
        Retrieves the counts of different data types for the current operational context.

        :returns: A dictionary mapping data types to their counts.
        :rtype: Dict[str, int]
        """
        return self.catalog.get_dtype_counts(
            network=self.current_network, station=self.current_station, campaign=self.current_campaign
        )

    @check_network_station_campaign
    def discover_data_and_add_files(self, directory_path: Path) -> None:
        """
        Scans a directory for data files and adds them to the catalog.

        :param directory_path: The path to the directory to scan.
        :type directory_path: Path
        """

        files: List[Path] = scrape_directory_local(directory_path)
        if len(files) == 0:
            logger.logerr(
                f"No files found in {directory_path}, ensure the directory is correct."
            )
            return

        logger.loginfo(f"Found {len(files)} files in {directory_path}")

        self.add_data_to_catalog(files)

    @check_network_station_campaign
    def add_data_to_catalog(self, local_filepaths: List[Path]):
        """
        Adds a list of local files to the data catalog.

        :param local_filepaths: A list of paths to the files to add.
        :type local_filepaths: List[Path]
        """

        file_data_list = []
        for file_path in local_filepaths:
            if not file_path.exists():
                logger.logerr(f"File {str(file_path)} does not exist")
                continue
            file_type, _size = get_file_type_local(file_path)
            if file_type is not None:
                symlinked_path = self.currentCampaignDir.raw / file_path.name
                # symlink to the raw directory
                try:
                    file_path.symlink_to(symlinked_path, target_is_directory=False)
                except FileExistsError:
                    pass
                file_data = AssetEntry(
                    local_path=file_path,
                    type=file_type,
                    network=self.current_network,
                    station=self.current_station,
                    campaign=self.current_campaign,
                )
                file_data_list.append(file_data)

        # Add each file (AssetEntry) to the catalog
        count = len(file_data_list)
        uploadCount = 0
        for file_assest in file_data_list:
            if self.catalog.add_entry(file_assest):
                uploadCount += 1

        logger.loginfo(f"Added {uploadCount} out of {count} files to the catalog")

    @check_network_station_campaign
    def add_data_remote(
        self,
        remote_filepaths: List[str],
        remote_type: Union[REMOTE_TYPE, str] = REMOTE_TYPE.HTTP,
    ) -> None:
        """
        Adds remote data files to the catalog.

        :param remote_filepaths: A list of remote file paths.
        :type remote_filepaths: List[str]
        :param remote_type: The type of the remote storage. Defaults to REMOTE_TYPE.HTTP.
        :type remote_type: Union[REMOTE_TYPE, str]
        :raises ValueError: If the specified remote type is not recognized.
        """

        # Check that the remote type is valid, default is HTTP
        if isinstance(remote_type, str):
            try:
                remote_type = REMOTE_TYPE(remote_type)
            except:
                raise ValueError(
                    f"Remote type {remote_type} must be one of {REMOTE_TYPE.__members__.keys()}"
                )

        # Create an AssetEntry for each file and append to a list
        file_data_list = []
        not_recognized = []
        for file in remote_filepaths:
            # Get the file type, If the file type is not recognized, it returns None
            file_type = get_file_type_remote(file)

            if file_type is None:  # If the file type is not recognized, skip it
                logger.logdebug(f"File type not recognized for {file}")
                not_recognized.append(file)
                continue

            if not self.catalog.remote_file_exist(
                network=self.current_network,
                station=self.current_station,
                campaign=self.current_campaign,
                type=file_type,
                remote_path=file,
            ):
                file_data = AssetEntry(
                    remote_path=file,
                    remote_type=remote_type,
                    type=file_type,
                    network=self.current_network,
                    station=self.current_station,
                    campaign=self.current_campaign,
                )
                file_data_list.append(file_data)
            else:
                # Count the file as already existing in the catalog
                logger.logdebug(
                    f"File {file} already exists in the catalog and has a local path"
                )

        # Add each file (AssetEntry) to the catalog
        file_count = len(file_data_list)
        uploadCount = 0
        for file_assest in file_data_list:
            if self.catalog.add_entry(file_assest):
                uploadCount += 1

        already_existed_in_catalog = file_count - uploadCount

        logger.loginfo(f"{len(not_recognized)} files not recognized and skipped")
        logger.loginfo(
            f"{already_existed_in_catalog} files already exist in the catalog"
        )
        logger.loginfo(f"Added {uploadCount} out of {file_count} files to the catalog")

    def download_data(
        self,
        file_types: List[AssetType] | List[str] | str = DEFAULT_FILE_TYPES_TO_DOWNLOAD,
        override: bool = False,
    ):
        """
        Downloads files of specified types from remote storage.

        :param file_types: The types of files to download.
        :type file_types: Union[List[AssetType], List[str], str], optional
        :param override: If True, redownloads files even if they exist locally. Defaults to False.
        :type override: bool, optional
        :raises ValueError: If a specified file type is not recognized.
        """

        # Grab assests from the catalog that match the network, station, campaign, and file type
        if not isinstance(file_types, list):
            file_types = [file_types]

        # Convert all string file_types to lowercase
        file_types = [ft.lower() if isinstance(ft, str) else ft for ft in file_types]

        # Remove duplicates
        file_types = list(set(file_types))

        # Check that the file types are valid, default is all file types
        for type in file_types:
            if isinstance(type, str):
                try:
                    file_types[file_types.index(type)] = AssetType(type)
                except:
                    raise ValueError(
                        f"File type {type} must be one of {AssetType.__members__.keys()}"
                    )

        # Pull files from the catalog by type
        for type in file_types:
            assets = self.catalog.get_assets(
                network=self.current_network,
                station=self.current_station,
                campaign=self.current_campaign,
                type=type,
            )

            if len(assets) == 0:
                logger.logerr(f"No matching data of type {type.value} found in catalog")
                continue

            # Find files that we need to download based on the catalog output. If override is True, download all files.
            if override:
                assets_to_download = assets
            else:
                assets_to_download = []
                for file_asset in assets:
                    if file_asset.local_path is None:
                        assets_to_download.append(file_asset)
                    else:
                        # Check to see if the file exists locally anyway
                        if not file_asset.local_path.exists():
                            assets_to_download.append(file_asset)

            if len(assets_to_download) == 0:
                logger.loginfo(f"No new {type.value} files to download")

            # split the entries into s3 and http
            s3_assets = [
                file
                for file in assets_to_download
                if file.remote_type == REMOTE_TYPE.S3.value
            ]
            http_assets = [
                file
                for file in assets_to_download
                if file.remote_type == REMOTE_TYPE.HTTP.value
            ]

            # Download Files from either S3 or HTTP
            if len(s3_assets) > 0:
                with threading.Lock():
                    client = boto3.client("s3")
                self._download_S3_files(s3_assets=s3_assets)
                for file in s3_assets:
                    if file.local_path is not None:
                        self.catalog.update_local_path(file.id, file.local_path)

            if len(http_assets) > 0:
                self.download_HTTP_files(http_assets=http_assets, file_type=type)

    def _download_S3_files(self, s3_assets: List[AssetEntry]):
        """
        Downloads files from S3 and updates the catalog with local paths.

        :param s3_assets: A list of S3 assets to download.
        :type s3_assets: List[AssetEntry]
        """

        s3_entries_processed = []
        for file in s3_assets:
            _path = Path(file.remote_path)
            s3_entries_processed.append(
                {"bucket": (bucket := _path.root), "prefix": _path.relative_to(bucket)}
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            local_path_results = executor.map(
                self._S3_download_file, s3_entries_processed
            )
            for local_downloaded_path, file_asset in zip(local_path_results, s3_assets):
                if local_downloaded_path is not None:
                    # Update the local path in the AssetEntry
                    file_asset.local_path = str(local_downloaded_path)
                    # Update catalog with local path
                    self.catalog.update_local_path(
                        id=file_asset.id, local_path=file_asset.local_path
                    )

    def _S3_download_file(
        self, client: boto3.client, bucket: str, prefix: str
    ) -> Path | None:
        """
        Downloads a single file from an S3 bucket.

        :param client: The Boto3 S3 client.
        :type client: boto3.client
        :param bucket: The S3 bucket name.
        :type bucket: str
        :param prefix: The S3 object key.
        :type prefix: str
        :returns: The local path of the downloaded file, or None if the download fails.
        :rtype: Optional[Path]
        """

        local_path = self.currentCampaignDir.raw / Path(prefix).name

        try:
            logger.logdebug(f"Downloading {prefix} to {local_path}")
            client.download_file(
                Bucket=bucket, Key=str(prefix), Filename=str(local_path)
            )
            logger.logdebug(f"Downloaded {str(prefix)} to {str(local_path)}")

        except Exception as e:
            logger.logerr(
                f"Error downloading {prefix} from {bucket }\n {e} \n HINT: $ aws sso login"
            )
            local_path = None

        finally:
            return local_path

    def download_HTTP_files(
        self, http_assets: List[AssetEntry], file_type: AssetType = None
    ):
        """
        Downloads files from an HTTP server and updates the catalog.

        :param http_assets: A list of HTTP assets to download.
        :type http_assets: List[AssetEntry]
        :param file_type: The type of file being downloaded. Defaults to None.
        :type file_type: Optional[AssetType], optional
        """

        for file_asset in tqdm(
            http_assets, desc=f"Downloading {file_type.value} files"
        ):
            if (
                local_path := self._HTTP_download_file(file_asset.remote_path)
            ) is not None:
                # Update the local path in the AssetEntry
                file_asset.local_path = str(local_path)
                # Update catalog with local path
                self.catalog.update_local_path(
                    id=file_asset.id, local_path=file_asset.local_path
                )

    def _HTTP_download_file(self, remote_url: Path) -> Path:
        """
        Downloads a single file from an HTTP URL.

        :param remote_url: The URL of the file to download.
        :type remote_url: Path
        :returns: The local path of the downloaded file, or None if the download fails.
        :rtype: Optional[Path]
        """
        try:
            local_path = self.currentCampaignDir.raw / Path(remote_url).name
            download_file_from_archive(url=remote_url, dest_dir=local_path.parent)

            if not local_path.exists():
                raise Exception

            logger.logdebug(f"Downloaded {str(remote_url)} to {str(local_path)}")

        except Exception as e:
            logger.logerr(
                f"Error downloading {str(remote_url)} \n {e}"
                + "\n HINT: Check authentication credentials"
            )
            local_path = None

        finally:
            return local_path

    @check_network_station_campaign
    def update_catalog_from_archive(self):
        """
        Updates the catalog with remote file paths from the data archive.
        """
        logger.loginfo(
            f"Updating catalog with remote paths of available data for {self.current_network} {self.current_station} {self.current_campaign}"
        )
        remote_filepaths = list_campaign_files(
            network=self.current_network, station=self.current_station, campaign=self.current_campaign
        )
        self.add_data_remote(
            remote_filepaths=remote_filepaths, remote_type=REMOTE_TYPE.HTTP
        )

    @check_network_station_campaign
    def get_site_metadata(self, site_metadata: Optional[Union[Site,Path]] = None) -> Optional[Site]:

        site_meta_write_dest = self.currentStationDir.site_metadata
        site_meta_read_dest = None
        site = None

        sources = [site_metadata, site_meta_write_dest]
        if site_metadata is None:
            sources = sources[::-1]  # reverse the list to prioritize the station directory file

        for source in sources:

            if isinstance(source, str):
                source = Path(source)

            if isinstance(source, Site):
                site = source
                # Write the site metadata to the station directory
                with open(site_meta_write_dest, "w") as f:
                    f.write(site.model_dump_json(indent=4))
                site_meta_read_dest = site_meta_write_dest
                logger.loginfo(
                    f"Using provided site metadata and wrote to {site_meta_write_dest}"
                )
                break

            elif isinstance(source, Path) and source.exists():
                try:
                    site = Site.from_json(source)
                    site_meta_read_dest = source
                    break
                except Exception as e:
                    response = f"Error loading site metadata from {source}: {e}"
                    warnings.warn(response)
                    logger.logerr(response)

            elif source is None:

                try:
                    site = load_site_metadata(
                        network=self.current_network, station=self.current_station
                    )
                    with open(site_meta_write_dest, "w") as f:
                        f.write(site.model_dump_json(indent=4))
                    site_meta_read_dest = site_meta_write_dest
                    logger.loginfo(
                        f"Downloaded site metadata from the ES archive to {site_meta_write_dest}"
                    )
                    break
                except Exception as e:
                    site = None
                    response = f"Error loading site metadata from the ES archive: {e}"
                    warnings.warn(response)
                    logger.logerr(response)

        if site is not None:

            if site_meta_read_dest != site_meta_write_dest:
                # Write the site metadata to the station directory
                with open(site_meta_write_dest, "w") as f:
                    f.write(site.model_dump_json(indent=4))
                logger.loginfo(f"Wrote site metadata to {site_meta_write_dest}")

        else:
            response = f"Warning: No site metadata found for {self.current_network} {self.current_station}. Some functionality may be limited."
            warnings.warn(response)
            logger.logwarn(response)

        return site

    @check_network_station_campaign
    def get_pipeline_sv3(self) -> Tuple[SV3Pipeline, SV3PipelineConfig]:
        """
        Initializes and returns an SV3 processing pipeline and its configuration.

        :returns: A tuple containing the pipeline and its config.
        :rtype: Tuple[SV3Pipeline, SV3PipelineConfig]
        """

        config = SV3PipelineConfig()
        pipeline = SV3Pipeline(
           directory_handler=self.directory_handler, config=config
        )
        pipeline.setNetworkStationCampaign(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign
        )
        return pipeline, config


    def print_logs(self, log: Literal["base", "gnss", "process"]):
        """
        Prints the specified log to the console.

        :param log: The type of log to print.
        :type log: Literal["base", "gnss", "process"]
        :raises ValueError: If the specified log type is not recognized.
        """
        if log == "base":
            logger.route_to_console()
        elif log == "gnss":
            pass # GNSS logger not implemented yet
        elif log == "process":
            pass # Process logger not implemented yet
        else:
            raise ValueError(
                f"Log type {log} not recognized. Must be one of ['base','gnss','process']"
            )

    def parse_surveys(self,override:bool=False,write_intermediate:bool=False):
        """
        Parses survey data for a given site.

        :param override: If True, re-parses existing data. Defaults to False.
        :type override: bool, optional
        :param write_intermediate: If True, writes intermediate files. Defaults to False.
        :type write_intermediate: bool, optional
        :raises ValueError: If site metadata is not loaded.
        """
        if self.currentSiteMetaData is None:
            raise ValueError("Site metadata not loaded, cannot parse surveys")
        
        dataPostProcessor = IntermediateDataProcessor(
            site=self.currentSiteMetaData,
            directory_handler=self.directory_handler,
        )
        dataPostProcessor.parse_surveys(
            network=self.current_network,
            station=self.current_station,
            override=override,
            write_intermediate=write_intermediate,
        )

    @check_network_station_campaign
    def prep_garpos(self,
                    custom_filter:dict = None,
                    shotdata_filter_config:dict = DEFAULT_FILTER_CONFIG,
                    override:bool=False,
                    write_intermediate:bool=False):
        """
        Prepares data for GARPOS processing.

        :param custom_filter: Custom filter settings for shot data preparation. Defaults to None.
        :type custom_filter: dict, optional
        :param shotdata_filter_config: Configuration for shot data filtering. Defaults to DEFAULT_FILTER_CONFIG.
        :type shotdata_filter_config: dict, optional
        :param override: If True, re-prepares existing data. Defaults to False.
        :type override: bool, optional
        :param write_intermediate: If True, writes intermediate files. Defaults to False.
        :type write_intermediate: bool, optional
        """
        dataPostProcessor: IntermediateDataProcessor = self.getIntermediateDataProcessor()

        dataPostProcessor.parse_surveys(
            override=override,
            write_intermediate=write_intermediate,
        )
        dataPostProcessor.prepare_shotdata_garpos(
            custom_filters=custom_filter,
            shotdata_filter_config=shotdata_filter_config,
            overwrite=override,
        )

    @check_network_station_campaign
    def getIntermediateDataProcessor(self)->IntermediateDataProcessor:
        """
        Returns an instance of the IntermediateDataProcessor for the current station.

        :returns: An instance of IntermediateDataProcessor.
        :rtype: IntermediateDataProcessor
        :raises ValueError: If site metadata is not loaded.
        """
        if self.currentSiteMetaData is None:
            raise ValueError("Site metadata not loaded, cannot get IntermediateDataProcessor")
        
        dataPostProcessor = IntermediateDataProcessor(
            site=self.currentSiteMetaData,
            directory_handler=self.directory_handler,
        )
        dataPostProcessor.setNetwork(network_id=self.current_network)
        dataPostProcessor.setStation(station_id=self.current_station)
        dataPostProcessor.setCampaign(campaign_id=self.current_campaign)

        return dataPostProcessor
    
    @check_network_station_campaign
    def getGARPOSHandler(self)->GarposHandler:
        """
        Returns an instance of the GarposHandler for the current station.

        :returns: An instance of GarposHandler.
        :rtype: GarposHandler
        :raises ValueError: If site metadata is not loaded.
        """
        if self.currentSiteMetaData is None:
            raise ValueError("Site metadata not loaded, cannot get GarposHandler")
        
        gp_handler = GarposHandler(
            directory_handler=self.directory_handler,
            site=self.currentSiteMetaData,
        )
        gp_handler.setNetworkStationCampaign(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign,
        )
        return gp_handler
