"""Contains the DataHandler class for handling data operations."""

import concurrent.futures
import os
import threading
import warnings
from pathlib import Path
from typing import (

    List,

    Optional,

    Union,
)

import boto3
from tqdm.auto import tqdm
import json

from es_sfgtools.data_mgmt.assetcatalog.handler import PreProcessCatalogHandler
from es_sfgtools.config.file_config import (
    REMOTE_TYPE,
    DEFAULT_FILE_TYPES_TO_DOWNLOAD,
    AssetType,
)

from es_sfgtools.data_mgmt.ingestion.datadiscovery import (
    get_file_type_local,
    get_file_type_remote,
    scrape_directory_local,
)
from es_sfgtools.data_mgmt.directorymgmt.handler import (
    CampaignDir,
    DirectoryHandler,
    NetworkDir,
    StationDir,
)
from es_sfgtools.data_mgmt.assetcatalog.schemas import AssetEntry

from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.logging import change_all_logger_dirs
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
from es_sfgtools.workflows.utils.protocols import WorkflowABC,validate_network_station_campaign

from es_sfgtools.config import Environment, WorkingEnvironment


class DataHandler(WorkflowABC):
    """
    Handles data operations including searching, adding, downloading, and processing.
    """
    mid_process_workflow: bool = False

    def __init__(
        self,
        directory: Path | str,
    ) -> None:
        """
        Initializes the DataHandler, setting up directories and the processing catalog.

        Parameters
        ----------
        directory : Path or str
            The root directory for data storage and operations.
        """
        super().__init__(directory=directory)

        self.acoustic_tdb: Optional[TDBAcousticArray] = None
        self.kin_position_tdb: Optional[TDBKinPositionArray] = None
        self.imu_position_tdb: Optional[TDBIMUPositionArray] = None
        self.shotdata_tdb: Optional[TDBShotDataArray] = None
        self.shotdata_tdb_pre: Optional[TDBShotDataArray] = None
        self.gnss_obs_tdb: Optional[TDBGNSSObsArray] = None
        self.gnss_obs_secondary_tdb: Optional[TDBGNSSObsArray] = None
        self.s3_directory_handler: Optional[DirectoryHandler] = None

    def _build_station_dir_structure(self, network_id: str, station_id: str, campaign_id: str):
        """
        Constructs the necessary directory structure for a given station and campaign.

        This includes directories for raw data, intermediate files, GARPOS processing,
        logs, and quality control.

        Parameters
        ----------
        network : str
            The name of the network.
        station : str
            The name of the station.
        campaign : str
            The name of the campaign.
        """

        # Change the logger directory to the campaign log directory
        log_dir = self.current_campaign_dir.log_directory
        change_all_logger_dirs(log_dir)
        os.environ["LOG_FILE_PATH"] = str(log_dir)
        # Log to the new log
        logger.loginfo(f"Built directory structure for {network_id} {station_id} {campaign_id}")

    def _ensure_tiledb_array(self, array_attr_name: str, array_class, uri_path):
        """
        Helper method to ensure a TileDB array is properly initialized.

        Parameters
        ----------
        array_attr_name : str
            The attribute name on this instance to store the array.
        array_class : type
            The TileDB array class to instantiate.
        uri_path : Path
            The URI path for the TileDB array.
        """
        current_array = getattr(self, array_attr_name, None)
        if current_array is None or uri_path != current_array.uri:
            setattr(self, array_attr_name, array_class(uri_path))

    def _build_tileDB_arrays(self) -> None:
        """
        Initializes and consolidates TileDB arrays for the current station.

        This includes arrays for acoustic data, kinematic positions, IMU positions,
        shot data, and GNSS observables.
        """
        logger.loginfo(f"Creating TileDB arrays for {self.current_station_name}")

        tiledb_dir = self.current_station_dir.tiledb_directory
        if 's3:/' in str(tiledb_dir.location):
            tiledb_dir.to_s3()

        # Use standardized pattern for all TileDB arrays
        self._ensure_tiledb_array('acoustic_tdb', TDBAcousticArray, tiledb_dir.acoustic_data)
        self._ensure_tiledb_array('kin_position_tdb', TDBKinPositionArray, tiledb_dir.kin_position_data)
        self._ensure_tiledb_array('imu_position_tdb', TDBIMUPositionArray, tiledb_dir.imu_position_data)
        self._ensure_tiledb_array('shotdata_tdb', TDBShotDataArray, tiledb_dir.shot_data)

        # Use a pre-array for dfo processing, self.shotdata_tdb is where we store the updated version
        self._ensure_tiledb_array('shotdata_tdb_pre', TDBShotDataArray, tiledb_dir.shot_data_pre)

        # Primary GNSS observables (10hz NOV770 collected on USB3 for SV3, 5hz bcnovatel for SV2)
        self._ensure_tiledb_array('gnss_obs_tdb', TDBGNSSObsArray, tiledb_dir.gnss_obs_data)

        # Secondary GNSS observables (5hz NOV000 collected on USB2 for SV3)
        self._ensure_tiledb_array('gnss_obs_secondary_tdb', TDBGNSSObsArray, tiledb_dir.gnss_obs_data_secondary)

        logger.loginfo(
            f"Consolidating existing TileDB arrays for {self.current_station_name}"
        )
        self.acoustic_tdb.consolidate()
        self.kin_position_tdb.consolidate()
        self.imu_position_tdb.consolidate()
        self.shotdata_tdb.consolidate()
        self.gnss_obs_tdb.consolidate()
        self.gnss_obs_secondary_tdb.consolidate()

    def set_network_station_campaign(
        self,
        network_id: str,
        station_id: str,
        campaign_id: str,
    ):
        """
        Changes the operational context to a specific network, station, and campaign.

        Overrides the parent method to add DataHandler-specific setup including
        TileDB array initialization and logging configuration.

        Parameters
        ----------
        network_id : str
            The network identifier.
        station_id : str
            The station identifier.
        campaign_id : str
            The campaign identifier.
        """
        # Call parent method to handle context switching
        super().set_network_station_campaign(network_id, station_id, campaign_id)

        # Build the campaign directory structure and TileDB arrays, this changes the logger directory as well
        log_dir = self.current_campaign_dir.log_directory
        change_all_logger_dirs(log_dir)
        os.environ["LOG_FILE_PATH"] = str(log_dir)

        if Environment.working_environment() == WorkingEnvironment.LOCAL:
            self._build_tileDB_arrays()

        logger.loginfo(f"Changed working station to {network_id} {station_id} {campaign_id}")

    def set_network_station_campaign_with_metadata(
        self,
        network_id: str,
        station_id: str,
        campaign_id: str,
        site_metadata: Optional[Union[Site, Path, str]] = None,
    ):
        """
        Changes the operational context and loads specific site metadata.

        This method extends set_network_station_campaign() by allowing custom
        site metadata to be loaded for the station context.

        Parameters
        ----------
        network_id : str
            The network identifier.
        station_id : str
            The station identifier.
        campaign_id : str
            The campaign identifier.
        site_metadata : Site, Path, str, optional
            Optional site metadata. If not provided, it will be loaded if available.
        """
        # First set the context
        self.set_network_station_campaign(network_id, station_id, campaign_id)

        # Then load metadata if provided or if none exists
        if site_metadata is not None or self.current_station_metadata is None:
            self.current_station_metadata = self.get_site_metadata(
                site_metadata=site_metadata
            )

    @validate_network_station_campaign
    def get_dtype_counts(self):
        """
        Retrieves the counts of different data types for the current operational context.

        Returns
        -------
        dict of {str : int}
            A dictionary mapping data types to their counts.
        """
        return self.asset_catalog.get_dtype_counts(
            network=self.current_network_name,
            station=self.current_station_name,
            campaign=self.current_campaign_name,
        )

    @validate_network_station_campaign
    def discover_data_and_add_files(self, directory_path: Path) -> None:
        """
        Scans a directory for data files and adds them to the catalog.

        Parameters
        ----------
        directory_path : Path
            The path to the directory to scan.
        """

        files: List[Path] = scrape_directory_local(directory_path)
        if not isinstance(files, list) or len(files) == 0:
            logger.logerr(
                f"No files found in {directory_path}, ensure the directory is correct."
            )
            return

        logger.loginfo(f"Found {len(files)} files in {directory_path}")

        self.add_data_to_catalog(files)

    @validate_network_station_campaign
    def add_data_to_catalog(self, local_filepaths: List[Path]):
        """
        Adds a list of local files to the data catalog.

        Parameters
        ----------
        local_filepaths : list of Path
            A list of paths to the files to add.
        """

        file_data_list = []
        for file_path in local_filepaths:
            if not file_path.exists():
                logger.logerr(f"File {str(file_path)} does not exist")
                continue
            file_type, _size = get_file_type_local(file_path)
            if file_type is not None:
                # Add check to see if the parent directory is the raw directory, if so, just use the file path
                if file_path.parent == self.current_campaign_dir.raw:
                    symlinked_path = file_path
                else:
                    symlinked_path = self.current_campaign_dir.raw / file_path.name
                # symlink to the raw directory
                if symlinked_path != file_path:
                    try:
                        file_path.symlink_to(symlinked_path, target_is_directory=False)
                    except FileExistsError:
                        pass
                file_data = AssetEntry(
                    local_path=file_path,
                    type=file_type,
                    network=self.current_network_name,
                    station=self.current_station_name,
                    campaign=self.current_campaign_name,
                )
                file_data_list.append(file_data)

        # Add each file (AssetEntry) to the catalog
        count = len(file_data_list)
        uploadCount = 0
        for file_assest in file_data_list:
            if self.asset_catalog.add_entry(file_assest):
                uploadCount += 1

        logger.loginfo(f"Added {uploadCount} out of {count} files to the catalog")

    @validate_network_station_campaign
    def add_data_remote(
        self,
        remote_filepaths: List[str],
        remote_type: Union[REMOTE_TYPE, str] = REMOTE_TYPE.HTTP,
    ) -> None:
        """
        Adds remote data files to the catalog.

        Parameters
        ----------
        remote_filepaths : list of str
            A list of remote file paths.
        remote_type : REMOTE_TYPE or str, default REMOTE_TYPE.HTTP
            The type of the remote storage.

        Raises
        ------
        ValueError
            If the specified remote type is not recognized.
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

            if not self.asset_catalog.remote_file_exist(
                network=self.current_network_name,
                station=self.current_station_name,
                campaign=self.current_campaign_name,
                type=file_type,
                remote_path=file,
            ):
                file_data = AssetEntry(
                    remote_path=file,
                    remote_type=remote_type,
                    type=file_type,
                    network=self.current_network_name,
                    station=self.current_station_name,
                    campaign=self.current_campaign_name,
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
            if self.asset_catalog.add_entry(file_assest):
                uploadCount += 1

        already_existed_in_catalog = file_count - uploadCount

        logger.loginfo(f"{len(not_recognized)} files not recognized and skipped")
        logger.loginfo(
            f"{already_existed_in_catalog} files already exist in the catalog"
        )
        logger.loginfo(f"Added {uploadCount} out of {file_count} files to the catalog")

    def download_data(
        self,
        file_types: Union[List[AssetType], List[str], str] = DEFAULT_FILE_TYPES_TO_DOWNLOAD,
        override: bool = False,
    ):
        """
        Downloads files of specified types from remote storage.

        Parameters
        ----------
        file_types : list of AssetType, list of str, or str, default DEFAULT_FILE_TYPES_TO_DOWNLOAD
            The types of files to download.
        override : bool, default False
            If True, redownloads files even if they exist locally.

        Raises
        ------
        ValueError
            If a specified file type is not recognized.
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
            assets = self.asset_catalog.get_assets(
                network=self.current_network_name,
                station=self.current_station_name,
                campaign=self.current_campaign_name,
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
                    if file_asset.local_path is None or not Path(file_asset.local_path).exists():
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
                        self.asset_catalog.update_local_path(file.id, file.local_path)

            if len(http_assets) > 0:
                self.download_HTTP_files(http_assets=http_assets, file_type=type)

    def _download_S3_files(self, s3_assets: List[AssetEntry]):
        """
        Downloads files from S3 and updates the catalog with local paths.

        Parameters
        ----------
        s3_assets : list of AssetEntry
            A list of S3 assets to download.
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
                    self.asset_catalog.update_local_path(
                        id=file_asset.id, local_path=file_asset.local_path
                    )

    def _S3_download_file(
        self, client: boto3.client, bucket: str, prefix: str
    ) -> Optional[Path]:
        """
        Downloads a single file from an S3 bucket.

        Parameters
        ----------
        client : boto3.client
            The Boto3 S3 client.
        bucket : str
            The S3 bucket name.
        prefix : str
            The S3 object key.

        Returns
        -------
        Path or None
            The local path of the downloaded file, or None if the download fails.
        """

        local_path = self.current_campaign_dir.raw / Path(prefix).name

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
        self, http_assets: List[AssetEntry], file_type: Optional[AssetType] = None
    ):
        """
        Downloads files from an HTTP server and updates the catalog.

        Parameters
        ----------
        http_assets : list of AssetEntry
            A list of HTTP assets to download.
        file_type : AssetType, optional
            The type of file being downloaded.
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
                self.asset_catalog.update_local_path(
                    id=file_asset.id, local_path=file_asset.local_path
                )

    def _HTTP_download_file(self, remote_url: Path) -> Path:
        """
        Downloads a single file from an HTTP URL.

        Parameters
        ----------
        remote_url : Path
            The URL of the file to download.

        Returns
        -------
        Path or None
            The local path of the downloaded file, or None if the download fails.
        """
        try:
            local_path = self.current_campaign_dir.raw / Path(remote_url).name
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

    @validate_network_station_campaign
    def update_catalog_from_archive(self) -> None:
        """
        Updates the catalog with remote file paths from the data archive.
        """
        logger.loginfo(
            f"Updating catalog with remote paths of available data for {self.current_network_name} {self.current_station_name} {self.current_campaign_name}"
        )
        remote_filepaths = list_campaign_files(
            network=self.current_network_name,
            station=self.current_station_name,
            campaign=self.current_campaign_name,
        )
        self.add_data_remote(
            remote_filepaths=remote_filepaths, remote_type=REMOTE_TYPE.HTTP
        )

    @validate_network_station_campaign
    def get_site_metadata(
        self, site_metadata: Optional[Union[Site, Path]] = None
    ) -> Optional[Site]:
        """
        Loads or validates site metadata for the current station.

        Parameters
        ----------
        site_metadata : Site, Path, or None, optional
            Site metadata to use. If None, attempts to load from station directory or archive.

        Returns
        -------
        Site or None
            The loaded site metadata, or None if not found.
        """

        site_meta_write_dest = self.current_station_dir.site_metadata
        site_meta_read_dest = None
        site = None

        sources = [site_metadata, site_meta_write_dest]
        if site_metadata is None:
            sources = sources[
                ::-1
            ]  # reverse the list to prioritize the station directory file

        for source in sources:

            if isinstance(source, str):
                source = Path(source)

            if isinstance(source, Site):
                site = source
                # Write the site metadata to the station directory
                with open(site_meta_write_dest, "w") as f:
                    json.dump(site.model_dump(mode='json'), f, indent=4)
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
                        network=self.current_network_name, station=self.current_station_name
                    )
                    with open(site_meta_write_dest, "w") as f:
                        json.dump(site.model_dump(mode='json'), f, indent=4)

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
                    json.dump(site.model_dump(), f, indent=4)
                logger.loginfo(f"Wrote site metadata to {site_meta_write_dest}")

        else:
            response = f"Warning: No site metadata found for {self.current_network_name} {self.current_station_name}. Some functionality may be limited."
            warnings.warn(response)
            logger.logwarn(response)

        return site

    def geolab_get_s3(self, overwrite: bool = False):
        """
        Synchronize seafloor geodesy data from S3 storage to local GeoLab environment.
        
        This method downloads and synchronizes data files from AWS S3 to the local
        GeoLab environment for the currently selected network and station. It handles
        both metadata files and campaign data, creating the necessary local directory
        structure and maintaining catalog consistency.
        
        The synchronization process:
        1. Validates GeoLab environment and S3 bucket configuration
        2. Loads or creates an S3 directory catalog
        3. Downloads station metadata files from S3 to local storage
        4. Downloads campaign data files from S3 to local storage
        5. Updates local and remote directory catalogs
        
        Args:
            overwrite (bool, optional): If True, re-downloads files even if they 
                already exist locally. If False, only downloads missing files.
                Defaults to False.
        
        Raises:
            AssertionError: If not running in GEOLAB environment
            ValueError: If S3 bucket configuration is missing or invalid
            
        Note:
            - Only processes data for the currently set network and station context
            - Requires valid AWS credentials and S3 bucket access
            - Creates local directory structure to match S3 organization
            - Maintains both local and remote directory catalogs for consistency
        """
        
        # =================================================================
        # ENVIRONMENT AND CONFIGURATION VALIDATION
        # =================================================================
        
        # Ensure we're running in the correct environment for S3 operations
        assert Environment.working_environment() == WorkingEnvironment.GEOLAB, \
            "S3 sync is only available in the GEOLAB environment."
        
        # Get the configured S3 bucket for data synchronization
        try:
            s3_bucket = Environment.s3_sync_bucket()
        except ValueError as e:
            # S3 bucket not configured - skip synchronization
            logger.logwarn(f"S3 synchronization skipped: {e}")
            return
        
        # =================================================================
        # S3 DIRECTORY CATALOG MANAGEMENT
        # =================================================================
        
        # Check if we have a cached remote catalog file
        if self.s3_directory_handler is None or overwrite:
            self.s3_directory_handler = DirectoryHandler.load_from_path(s3_bucket)
        if self.s3_directory_handler is None:
            raise ValueError(f"Failed to load or create S3 directory catalog from bucket: {s3_bucket}")

        # =================================================================
        # DATA SYNCHRONIZATION PROCESS
        # =================================================================
        
        # Iterate through all networks in the S3 directory structure
        for network_name, network_dir in self.s3_directory_handler.networks.items():
            # Only process the currently selected network (skip others)
            if network_name != self.current_network_name:
                continue
            
            # Create or get the corresponding local network directory
            local_network_dir = self.directory_handler.add_network(network_name)

            # Process all stations within the current network
            for station_name, remote_station_dir in network_dir.stations.items():
                # Only process the currently selected station (skip others)
                if station_name != self.current_station_name:
                    continue
                
                # Create or get the corresponding local station directory
                local_station_dir = local_network_dir.add_station(station_name)
                
                # Synchronize TileDB directory reference (array storage location)
                local_station_dir.tiledb_directory = remote_station_dir.tiledb_directory
                
                # =================================================================
                # CAMPAIGN DATA SYNCHRONIZATION
                # =================================================================
                
                # Process all campaigns within the current station
                for campaign_id, remote_campaign_dir in remote_station_dir.campaigns.items():
                    # Create or get the corresponding local campaign directory
                    local_campaign_dir = local_station_dir.add_campaign(campaign_id)
                    
                    # Download all files within this campaign from S3
                    for file in remote_campaign_dir.location.rglob("*"):
                        # Calculate the relative path within the campaign directory
                        relative_path = file.relative_to(remote_campaign_dir.location)
                        # Construct the corresponding local file path
                        local_file_path = local_campaign_dir.location / relative_path
                        
                        try:
                            # Download file if it doesn't exist locally or if overwriting
                            if not local_file_path.exists() or overwrite:
                                # Ensure local directory structure exists
                                local_file_path.parent.mkdir(
                                    parents=True, exist_ok=True
                                )
                                # Download the file from S3 to local storage
                                file.download_to(local_file_path)
                        except Exception as e:
                            # Log download failures but continue with other files
                            logger.logerr(f"Failed to download campaign file {file} to local: {e}")
        
        # =================================================================
        # CATALOG PERSISTENCE
        # =================================================================
        
        # Save the updated local directory catalog to disk
        # This ensures the local catalog reflects all downloaded files
        self.directory_handler.save()