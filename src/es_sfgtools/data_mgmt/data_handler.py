""" Contains the DataHandler class for handling data operations. """
import os
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
from typing import List, Callable, Union, Generator, Tuple, LiteralString, Optional, Dict, Literal
import logging
import boto3
import matplotlib.pyplot as plt
from tqdm.auto import tqdm 
from functools import partial
import concurrent.futures
import threading
from functools import wraps
import json
from datetime import date
import seaborn 
seaborn.set_theme(style="whitegrid")

from es_sfgtools.data_models.metadata import MetaDataCatalog as Catalog
from es_sfgtools.data_models.metadata import NetworkData,StationData
from es_sfgtools.utils.archive_pull import download_file_from_archive, list_campaign_files, list_campaign_files_by_type
from es_sfgtools.logging import ProcessLogger as logger, change_all_logger_dirs
from es_sfgtools.data_mgmt.file_schemas import AssetEntry, AssetType
from es_sfgtools.tiledb_tools.tiledb_schemas import TDBAcousticArray,TDBKinPositionArray,TDBIMUPositionArray,TDBShotDataArray,TDBGNSSObsArray
from es_sfgtools.data_mgmt.catalog import PreProcessCatalog
from es_sfgtools.pipelines.sv3_pipeline import SV3Pipeline, SV3PipelineConfig
from es_sfgtools.novatel_tools.utils import get_metadata,get_metadatav2
from es_sfgtools.data_mgmt.constants import REMOTE_TYPE, FILE_TYPES
from es_sfgtools.data_mgmt.datadiscovery import scrape_directory_local, get_file_type_local, get_file_type_remote
from es_sfgtools.modeling.garpos_tools.garpos_handler import GarposHandler
from es_sfgtools.data_mgmt.constants import DEFAULT_FILE_TYPES_TO_DOWNLOAD

from es_sfgtools.data_mgmt.directory_handler import DirectoryHandler

def check_network_station_campaign(func: Callable):
    """ Wrapper to check if network, station, and campaign are set before running a function. """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.current_network is None:
            raise ValueError("Network name not set, use change_working_station")

        if self.current_station is None:
            raise ValueError("Station name not set, use change_working_station")

        if self.current_campaign is None:
            raise ValueError("campaign name not set, use change_working_station")

        return func(self, *args, **kwargs)
    return wrapper


class CatalogHandler:
    def __init__(self, file_path: Union[str, Path], name: str = None, catalog: Catalog = None):
        """
        Initialize the CatalogHandler with a file path to persist the catalog.

        Args:
            file_path (Union[str, Path]): Path to the .json file for the catalog.
        """
        self.file_path = Path(file_path)
        if catalog is not None:
            self.catalog = catalog
        else:
            self.catalog = self._load_catalog(name=name)

    def _load_catalog(self,name:str=None) -> Catalog:
        """
        Load the catalog from the .json file or create a new one if the file doesn't exist.

        Returns:
            Catalog: The loaded or newly created catalog.
        """
        if self.file_path.exists() and self.file_path.__sizeof__() > 0:
            with open(self.file_path, "r") as file:
                data = json.load(file)
            return Catalog.load_data(data, name=name)
        else:
            return Catalog(name=name, type="Data", networks={})

    def save(self):
        """
        Save the current state of the catalog to the .json file.
        """
        with open(self.file_path, "w") as file:
            json.dump(self.catalog.model_dump(), file, indent=2)

    def add_network(self, network_name: str):
        """
        Add a new network to the catalog.

        Args:
            network_name (str): The name of the network to add.
        """
        if network_name in self.catalog.networks:
            return
        self.catalog.networks[network_name] = NetworkData(
            name=network_name, stations={}
        )
        self.save()

    def add_station(self, network_name: str, station_name: str, station_data: dict|StationData):
        """
        Add a new station to a network.

        Args:
            network_name (str): The name of the network to add the station to.
            station_name (str): The name of the station to add.
            station_data (dict): The station data to add.
        """
        if network_name not in self.catalog.networks:
            self.add_network(network_name)
        network = self.catalog.networks[network_name]      
        station_data = station_data if isinstance(station_data, StationData) else StationData(**station_data)
        network.stations[station_name] = station_data
        self.save()

    def update_station(self, network_name: str, station_name: str, updated_data: dict):
        """
        Update the data for an existing station.

        Args:
            network_name (str): The name of the network containing the station.
            station_name (str): The name of the station to update.
            updated_data (dict): The updated station data.
        """
        if network_name not in self.catalog.networks:
            raise ValueError(f"Network '{network_name}' does not exist.")
        network = self.catalog.networks[network_name]
        if station_name not in network.stations:
            raise ValueError(
                f"Station '{station_name}' does not exist in network '{network_name}'."
            )
        for key, value in updated_data.items():
            setattr(network.stations[station_name], key, value)
        self.save()


class DataHandler:
    """
    A class to handle data operations such as searching for, adding, downloading and processing data.
    """

    def __init__(
        self,
        directory: Path | str,
        data_catalog: Catalog = None,
    ) -> None:
        """
        Initialize the DataHandler object.

        Args:
            directory (Path | str): The directory path to store files under.

        """

        self.current_network: Optional[str] = None
        self.current_station: Optional[str] = None
        self.current_campaign: Optional[str] = None

        # Create the directory structures
        self.main_directory = Path(directory)
        self.directory_handler = DirectoryHandler(location=self.main_directory)
        self.directory_handler.build()

        logger.set_dir(self.main_directory)

        # Create the main and pride directory
        # self.pride_dir = self.main_directory / "Pride"
        # self.pride_dir.mkdir(exist_ok=True, parents=True)

        # Create the catalog

        self.catalog = PreProcessCatalog(self.directory_handler.asset_catalog_db_path)

        self.data_catalog_path = self.main_directory / "data_catalog.json"
        self.data_catalog = CatalogHandler(
            self.data_catalog_path, name="Data Catalog", catalog=data_catalog
        )

    def _build_station_dir_structure(self, network: str, station: str, campaign: str):
        """
        Build the directory structure for a station.
        Format is as follows:
            - [SFG Data Directory]/
                - <network>/
                    - <station>/
                        - <campaign>/
                            - raw/
                            - intermediate/
                            - GARPOS/
                                - survey 1
                                - survey 2
                            - logs/
                            - qc/
                        - TileDB/

                - Pride/
        """

        # # Create the network/station directory structure
        # self.station_dir = self.main_directory / network / station
        # self.station_dir.mkdir(parents=True, exist_ok=True)

        # # Create the Data directory structure (network/station/Data)
        # campaign_data_dir = self.station_dir / campaign
        # campaign_data_dir.mkdir(exist_ok=True)

        # # Set up loggers under the campaign directory
        # self.campaign_log_dir = campaign_data_dir / "logs"
        # self.campaign_log_dir.mkdir(parents=True, exist_ok=True)

        # # Create the TileDB directory structure (network/station/TileDB)
        # self.tileb_dir = self.station_dir / "TileDB"
        # self.tileb_dir.mkdir(exist_ok=True)

        # # Create the raw, intermediate, and processed directories (network/station/Data/raw) and store as class attributes
        # self.raw_dir = campaign_data_dir / "raw"
        # self.raw_dir.mkdir(exist_ok=True)

        # self.inter_dir = campaign_data_dir / "intermediate"
        # self.inter_dir.mkdir(exist_ok=True)

        # self.garpos_dir = campaign_data_dir / "GARPOS"
        # self.garpos_dir.mkdir(exist_ok=True)

        # self.qc_dir = campaign_data_dir / "qc"
        # self.qc_dir.mkdir(exist_ok=True)

        self.directory_handler.build_station_directory(
            network_name=network, station_name=station, campaign_name=campaign
        )

        # Change the logger directory to the campaign log directory
        log_dir = self.directory_handler[network][station][campaign].log_directory
        change_all_logger_dirs(log_dir)
        os.environ["LOG_FILE_PATH"] = str(log_dir)
        # Log to the new log
        logger.loginfo(f"Built directory structure for {network} {station} {campaign}")

    @check_network_station_campaign
    def _build_tileDB_arrays(self):
        """
        Build the TileDB arrays for the current station. TileDB directory is /network/station/TileDB.
        """
        logger.loginfo(f"Creating TileDB arrays for {self.current_station}")

        self.directory_handler[self.current_network][self.current_station].tiledb_directory.build()

        acoustic_tdb_uri = self.directory_handler[self.current_network][self.current_station].tiledb_directory.acoustic_data
        self.acoustic_tdb = TDBAcousticArray(acoustic_tdb_uri)


        kin_position_tdb_uri = self.directory_handler[self.current_network][self.current_station].tiledb_directory.kin_position_data
        self.kin_position_tdb = TDBKinPositionArray(kin_position_tdb_uri)

        imu_position_tdb_uri = self.directory_handler[self.current_network][self.current_station].tiledb_directory.imu_position_data
        self.imu_position_tdb = TDBIMUPositionArray(imu_position_tdb_uri)
        
        shotdata_tdb_uri = self.directory_handler[self.current_network][self.current_station].tiledb_directory.shot_data
           
        self.shotdata_tdb = TDBShotDataArray(shotdata_tdb_uri)
        # Use a pre-array for dfo processing, self.shotdata_tdb is where we store the updated version
        shotdata_tdb_uri_pre = self.directory_handler[self.current_network][self.current_station].tiledb_directory.shot_data_pre
        self.shotdata_tdb_pre = TDBShotDataArray(shotdata_tdb_uri_pre)
        
        #this is the primary GNSS observables (10hz NOV770 collected on USB3 for SV3, 5hz bcnovatel for SV2)
        gnss_obs_tdb_uri = self.directory_handler[self.current_network][self.current_station].tiledb_directory.gnss_obs_data
        self.gnss_obs_tdb = TDBGNSSObsArray(
            gnss_obs_tdb_uri
        )  # golang binaries will be used to interact with this array
        
        # this is the secondary GNSS observables (5hz NOV000 collected on USB2 for SV3)
        # can choose to use this instead of the primary GNSS observables if desired
        gnss_obs_secondary_tdb_uri = self.directory_handler[self.current_network][self.current_station].tiledb_directory.gnss_obs_data_secondary
        self.gnss_obs_secondary_tdb = TDBGNSSObsArray(
            gnss_obs_secondary_tdb_uri
        )
        

        self.data_catalog.add_station(
            network_name=self.current_network,
            station_name=self.current_station,
            station_data=StationData(
                name=self.current_station,
                shotdata=str(shotdata_tdb_uri),
                kinpositiondata=str(kin_position_tdb_uri),
                gnssobsdata=str(gnss_obs_tdb_uri),
                gnssobsdata_secondary=str(gnss_obs_secondary_tdb_uri),
                imupositiondata=str(imu_position_tdb_uri),
                acousticdata=str(acoustic_tdb_uri),
                shotdata_pre=str(self.shotdata_tdb_pre.uri),
            ),
        )
        logger.loginfo(f"Consolidating existing TileDB arrays for {self.current_station}")
        self.acoustic_tdb.consolidate()
        self.kin_position_tdb.consolidate()
        self.imu_position_tdb.consolidate()
        self.shotdata_tdb.consolidate()
        self.gnss_obs_tdb.consolidate()
        self.gnss_obs_secondary_tdb.consolidate()

    @check_network_station_campaign
    def _build_rinex_meta(self) -> None:
        """
        Build the RINEX metadata for a station.
        Args:
            station_dir (Path): The station directory to build the RINEX metadata for.
        """
        # Get the RINEX metadata
        self.rinex_metav2 = self.directory_handler[self.current_network][self.current_station].location / "rinex_metav2.json"
        self.rinex_metav1 = self.directory_handler[self.current_network][self.current_station].location / "rinex_metav1.json"
        if not self.rinex_metav2.exists():
            with open(self.rinex_metav2, "w") as f:
                json.dump(get_metadatav2(site=self.current_station), f)

        if not self.rinex_metav1.exists():
            with open(self.rinex_metav1, "w") as f:
                json.dump(get_metadata(site=self.current_station), f)

    def change_working_station(
        self,
        network: str,
        station: str,
        campaign: str,
        start_date: date = None,
        end_date: date = None,
    ):
        """
        Change the working station.

        Args:
            network (str): The network name.
            station (str): The station name.
            campaign (str): The campaign name.
            start_date (date): The start date for the data. Default is None.
            end_date (date): The end date for the data. Default is None.
        """
        # Set class attributes & create the directory structure
        self.current_station = station
        self.current_network = network
        self.current_campaign = campaign

        # Build the campaign directory structure and TileDB arrays, this changes the logger directory as well
        self._build_station_dir_structure(network, station, campaign)

        if start_date == None or end_date == None:
            logger.logwarn(f"No date range set for {network}, {station}, {campaign}")

        self.date_range = [start_date, end_date]
        self._build_tileDB_arrays()
        self._build_rinex_meta()

        logger.loginfo(f"Changed working station to {network} {station} {campaign}")

    @check_network_station_campaign
    def get_dtype_counts(self):
        """
        Get the data type counts (local) for the current station from the catalog.

        Returns:
            Dict[str,int]: A dictionary of data types and their counts.
        """
        return self.catalog.get_dtype_counts(
            network=self.current_network, station=self.current_station, campaign=self.current_campaign
        )

    @check_network_station_campaign
    def discover_data_and_add_files(self, directory_path: Path) -> None:
        """
        For a given directory of data, iterate through all files and add them to the catalog.

        Note: Be sure to correctly set the network, station, and campaign before running this function.

        Args:
            dir_path (Path): The directory path to look for files and add them to the catalog.
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
        Using a list of local filepaths, add the data to the catalog.

        Args:
            local_filepaths (List[str]): A list of local filepaths to add to the catalog.
        """

        file_data_list = []
        for file_path in local_filepaths:
            if not file_path.exists():
                logger.logerr(f"File {str(file_path)} does not exist")
                continue
            file_type, _size = get_file_type_local(file_path)
            if file_type is not None:
                symlinked_path = self.directory_handler[self.current_network][self.current_station][self.current_campaign].raw / file_path.name
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
        Add campaign data to the catalog.

        Args:
            remote_filepaths (List[str]): A list of file locations on gage-data.
            remote_type (Union[REMOTE_TYPE,str]): The type of remote location.
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
        Retrieves and catalogs data from the remote locations stored in the catalog.

        Args:
            file_types (list/str): the type of files to download.
            override (bool): Whether to download the data even if it already exists. Default is False.
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
        Downloads a list of files from S3.

        Args:
            s3_assets (List[AssetEntry[str, str]]): A list of S3 assets to download.
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
        Downloads a file from the specified S3 bucket and prefix.

        Args:
            client (boto3.client): The boto3 client object.
            bucket (str): S3 bucket name
            prefix (str): S3 object prefix

        Returns:
            local_path (Path): The local path where the file was downloaded, or None if the download failed.
        """

        local_path = self.raw_dir / Path(prefix).name

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
        Download HTTP files with progress bar and updates the catalog with the local path.

        Args:
            http_assets (List[AssetEntry]): A list of HTTP assets to download.
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
        Downloads a file from the specified https url on gage-data

        Args:
            remote_url (Path): The path of the file in the gage-data storage.
            destination (Path): The local path where the file will be downloaded.

        Returns:
            local_path (Path): The local path where the file was downloaded, or None if the download failed.
        """
        try:
            local_path = self.raw_dir / Path(remote_url).name
            download_file_from_archive(url=remote_url, dest_dir=self.raw_dir)

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
        Updates the catalog with remote paths of files in the archive.
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
    def add_ctds_to_catalog(self):
        """
        Adds CTD data to the catalog.
        This function does the following:
        1) Looks for ctd data in the metadata/ctd directory of the archive.
        2) If found, adds it to the catalog.

        """
        logger.loginfo(
            f"Cataloging available sound speed data for {self.current_network} {self.current_station} {self.current_campaign}"
        )
        remote_filepath_dict = list_campaign_files_by_type(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign,
            show_logs=False,
        )
        ctds = remote_filepath_dict.get("ctd", [])
        logger.loginfo(f"Found {len(ctds)} CTD files in the archive")

        if len(ctds):
            self.add_data_remote(remote_filepaths=ctds, remote_type=REMOTE_TYPE.HTTP)

    @check_network_station_campaign
    def view_data(self):
        shotdata_dates = self.shotdata_tdb.get_unique_dates().tolist()
        kin_position_dates = self.kin_position_tdb.get_unique_dates().tolist()
        date_set = shotdata_dates + kin_position_dates
        date_set = sorted(list(set(date_set)))

        date_tick_map = {date: i for i, date in enumerate(date_set)}
        fig, ax = plt.subplots()
        # plot the kin_position dates with red vertical line
        kin_position_x = [date_tick_map[date] for date in kin_position_dates]
        kin_position_y = [1 for _ in kin_position_dates]

        ax.scatter(x=kin_position_x, y=kin_position_y, c="r", marker="o", label="Pride GNSS Positions")
        # plot the shotdata dates with blue vertical line
        shotdata_x = [date_tick_map[date] for date in shotdata_dates]
        shotdata_y = [2 for _ in shotdata_dates]
        ax.scatter(x=shotdata_x, y=shotdata_y, c="b", marker="o", label="ShotData")
        ax.xaxis.set_ticks(
            [i for i in date_tick_map.values()],
            [str(date) for date in date_tick_map.keys()],
        )
        ax.yaxis.set_ticks([])
        ax.set_xlabel("Date")
        fig.legend()
        fig.suptitle(f"Found Dates For {self.current_network} {self.current_station}")
        plt.show()

    @check_network_station_campaign
    def get_pipeline_sv3(self) -> Tuple[SV3Pipeline, SV3PipelineConfig]:
        """
        Creates and returns an SV3Pipeline object along with its configuration.
        This method initializes an SV3PipelineConfig object using the instance
        attributes such as network, station, campaign, writedir, pride_dir,
        shot_data_dest, kin_position_data_dest, and catalog_path. It then creates an
        SV3Pipeline object using the catalog and the created configuration.
        Returns:
            Tuple[SV3Pipeline, SV3PipelineConfig]: A tuple containing the
            SV3Pipeline object and its configuration.
        """

        config = SV3PipelineConfig()
        pipeline = SV3Pipeline(
           directory_handler=self.directory_handler, config=config
        )
        pipeline.set_site_data(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign
        )
        return pipeline, config

    @check_network_station_campaign
    def get_garpos_handler(self, site_data) -> GarposHandler:
        """
        Creates and returns a GarposHandler object.
        This method initializes a GarposHandler object using the instance
        attributes such as site_config, working_dir, and shotdata_tdb.

        Args:
            site_config (SiteConfig): A SiteConfig object.

        Returns:
            GarposHandler: A GarposHandler object.
        """
        station_data = self.data_catalog.catalog.networks[self.current_network].stations[
            self.current_station
        ]

        return GarposHandler(
            network=self.current_network,
            station=self.current_station,
            campaign=self.current_campaign,
            site_data=site_data,
            station_data=station_data,
            working_dir=self.garpos_dir,
        )
    
    def test_logger(self):
        print(f"PRINT: testing logger {logger} with handlers {logger.logger.handlers}")
        logger.loginfo(f"LOGGER: testing logger {logger} with handlers {logger.logger.handlers}")
        logger.logdebug("logdebug test")
        logger.loginfo("loginfo test")
        logger.logwarn("logwarn test")
        logger.logerr("logerr test")

    # TODO: this wouldn't work anymore, logger is process logger, not pulling in gnss logger. Maybe put this in the logger class.
    def print_logs(self, log: Literal["base", "gnss", "process"]):
        """
        Print logs to console.
        Args:
            log (Literal['base','gnss','process']): The type of log to print.
        """
        if log == "base":
            logger.route_to_console()
        elif log == "gnss":
            self.gnss_logger.route_to_console()
        elif log == "process":
            self.process_logger.route_to_console()
        else:
            raise ValueError(
                f"Log type {log} not recognized. Must be one of ['base','gnss','process']"
            )
