""" Contains the DataHandler class for handling data operations. """
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

import seaborn 
seaborn.set_theme(style="whitegrid")

from es_sfgtools.utils.archive_pull import download_file_from_archive
from es_sfgtools.processing.assets.file_schemas import AssetEntry, AssetType
from es_sfgtools.processing.assets.tiledb_temp import TDBAcousticArray,TDBGNSSArray,TDBPositionArray,TDBShotDataArray
from es_sfgtools.processing.pipeline.catalog import Catalog
from es_sfgtools.processing.pipeline.pipelines import SV3Pipeline
from es_sfgtools.processing.pipeline.constants import REMOTE_TYPE, FILE_TYPES
from es_sfgtools.processing.pipeline.datadiscovery import scrape_directory_local, get_file_type_local, get_file_type_remote

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def check_network_station_survey(func: Callable):
    """ Wrapper to check if network, station, and survey are set before running a function. """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.network is None:
            raise ValueError("Network name not set, use change_working_station")
        
        if self.station is None:
            raise ValueError("Station name not set, use change_working_station")
        
        if self.survey is None:
            raise ValueError("Survey name not set, use change_working_station")
        
        return func(self, *args, **kwargs)
    return wrapper

class DataHandler:
    """
    A class to handle data operations such as searching for, adding, downloading and processing data.
    """

    def __init__(self,
                 directory: Path | str,
                 network: str = None,
                 station: str = None,
                 survey: str = None,
                 ) -> None:
        """
        Initialize the DataHandler object.

        Args:
            directory (Path | str): The directory path to store files under.
            network (str, optional): The network name.
            station (str, optional): The station name.
            survey (str, optional): The survey name.
        """

        self.network = network
        self.station = station
        self.survey = survey
   
        # Create the directory structures
        self.main_directory = Path(directory)
        # Create the main and pride directory
        self.pride_dir = self.main_directory / "Pride"
        self.pride_dir.mkdir(exist_ok=True, parents=True)

        if self.network is not None and self.station is not None and self.survey is not None:
            # Create the network/station directory structure
            self.build_station_dir_structure(self.network, self.station, self.survey)
            # Create the TileDB arrays
            self.build_tileDB_arrays()

        # Create the catalog
        self.db_path = self.main_directory / "catalog.sqlite"
        if not self.db_path.exists():
            self.db_path.touch()
        self.catalog = Catalog(self.db_path)


    def build_station_dir_structure(self, network: str, station: str, survey: str):
        """
        Build the directory structure for a station.
        Format is as follows:
            - [SFG Data Directory]/
                - <network>/
                    - <station>/
                        - <survey>/
                            - raw/
                            - intermediate/
                            - processed/ 
                        - TileDB/

                - Pride/ 
        """

        # Create the network/station directory structure
        self.station_dir = self.main_directory / network / station
        self.station_dir.mkdir(parents=True, exist_ok=True)

        # Create the TileDB directory structure (network/station/TileDB)
        self.tileb_dir = self.station_dir / "TileDB"
        self.tileb_dir.mkdir(exist_ok=True)

        # Create the Data directory structure (network/station/Data)
        survey_data_dir = self.station_dir / survey
        survey_data_dir.mkdir(exist_ok=True)

        # Create the raw, intermediate, and processed directories (network/station/Data/raw) and store as class attributes
        self.raw_dir = survey_data_dir / "raw"
        self.raw_dir.mkdir(exist_ok=True)

        self.inter_dir = survey_data_dir / "intermediate"
        self.inter_dir.mkdir(exist_ok=True)

        self.proc_dir = survey_data_dir / "processed"
        self.proc_dir.mkdir(exist_ok=True)

    def build_tileDB_arrays(self):
        """
        Build the TileDB arrays for the current station. TileDB directory is /network/station/TileDB.
        """
        self.acoustic_tdb = TDBAcousticArray(self.tileb_dir/"acoustic_db.tdb")
        self.gnss_tdb = TDBGNSSArray(self.tileb_dir/"gnss_db.tdb")
        self.position_tdb = TDBPositionArray(self.tileb_dir/"position_db.tdb")
        self.shotdata_tdb = TDBShotDataArray(self.tileb_dir/"shotdata_db.tdb")
    
    def change_working_station(self, network: str, station: str, survey: str = None):
        """
        Change the working station.
        
        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name. Default is None.
        """

        # Set class attributes & create the directory structure
        self.station = station

        if network is not None:
            self.network = network

        if survey is not None:
            self.survey = survey

        # Build the directory structure and TileDB arrays
        self.build_station_dir_structure(network, station,survey)
        self.build_tileDB_arrays()

        logger.info(f"Changed working station to {network} {station}")


    @check_network_station_survey
    def get_dtype_counts(self):
        """ 
        Get the data type counts for the current station from the catalog.

        Returns:
            Dict[str,int]: A dictionary of data types and their counts.
        """ 
        return self.catalog.get_dtype_counts(network=self.network, 
                                             station=self.station, 
                                             survey=self.survey)

    @check_network_station_survey
    def discover_data_and_add_files(self, directory_path: Path) -> None:
        """
        For a given directory of data, iterate through all files and add them to the catalog.
        
        Note: Be sure to correctly set the network, station, and survey before running this function.

        Args:
            dir_path (Path): The directory path to look for files and add them to the catalog.
        """

        files:List[Path] = scrape_directory_local(directory_path)
        if len(files) == 0:
            logger.error(f"No files found in {directory_path}, ensure the directory is correct.")
            return

        self.add_data_to_catalog(files)

    @check_network_station_survey
    def add_data_to_catalog(self, local_filepaths: List[str]):
        """ 
        Using a list of local filepaths, add the data to the catalog. 
        
        Args:
            local_filepaths (List[str]): A list of local filepaths to add to the catalog.
        """

        file_data_list = []
        for file_path in local_filepaths:
            if not file_path.exists():
                logger.error(f"File {str(file_path)} does not exist")
                continue
            file_type, _size = get_file_type_local(file_path)
            if file_type is not None:
                file_data = AssetEntry(
                    local_path=file_path,
                    type=file_type,
                    network=self.network,
                    station=self.station,
                    survey=self.survey,
                )
                file_data_list.append(file_data)

        # Add each file (AssetEntry) to the catalog
        count = len(file_data_list)
        uploadCount = 0
        for file_assest in file_data_list:
            if self.catalog.add_entry(file_assest):
                uploadCount += 1

        logger.info(f"Added {uploadCount} out of {count} files to the catalog")


    def add_data_remote(self, 
                        remote_filepaths: List[str],
                        remote_type:Union[REMOTE_TYPE,str] = REMOTE_TYPE.HTTP
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
                raise ValueError(f"Remote type {remote_type} must be one of {REMOTE_TYPE.__members__.keys()}")
            
        
        # Create an AssetEntry for each file and append to a list
        file_data_list = []
        for file in remote_filepaths:
            # Get the file type, If the file type is not recognized, it returns None
            file_type = get_file_type_remote(file)

            if file_type is None: # If the file type is not recognized, skip it
                logger.warning(f"File type not recognized for {file}")
                continue

            file_data = AssetEntry(
                remote_path=file,
                remote_type=remote_type,
                type=file_type,
                network=self.network,
                station=self.station,
                survey=self.survey,
            )
            file_data_list.append(file_data)

        # Add each file (AssetEntry) to the catalog
        count = len(file_data_list)
        uploadCount = 0
        for file_assest in file_data_list:
            if self.catalog.add_entry(file_assest):
                uploadCount += 1

        logger.info(f"Added {uploadCount} out of {count} files to the catalog")

    def download_data(self, file_types: List[AssetType] | List[str] | str = FILE_TYPES, override: bool=False):
        """
        Retrieves and catalogs data from the remote locations stored in the catalog.

        Args:
            file_type (str): The type of file to download
            override (bool): Whether to download the data even if it already exists. Default is False.
        """

        # Grab assests from the catalog that match the network, station, survey, and file type
        if not isinstance(file_types,list):
            file_types = [file_types]

        # Remove duplicates
        file_types = list(set(file_types)) 
        for type in file_types:
            if isinstance(type,str):
                try:
                    file_types[file_types.index(type)] = AssetType(type)
                except:
                    raise ValueError(f"File type {type} must be one of {AssetType.__members__.keys()}")
                
        # Pull files from the catalog by type
        for type in file_types:
            assets = self.catalog.get_assets(network=self.network,
                                            station=self.station,
                                            survey=self.survey,
                                            type=type)

            if len(assets) == 0:
                logger.error(f"No matching data found in catalog")
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
                logger.info(f"No new files to download")
            
            # split the entries into s3 and http
            s3_assets = [file for file in assets_to_download if file.remote_type == REMOTE_TYPE.S3.value]
            http_assets = [file for file in assets_to_download if file.remote_type == REMOTE_TYPE.HTTP.value]

            # Download Files from either S3 or HTTP
            if len(s3_assets) > 0:
                with threading.Lock():
                    client = boto3.client('s3')
                self._download_S3_files(client=client,
                                        s3_assets=s3_assets)
                for file in s3_assets:
                    if file.local_path is not None:
                        self.catalog.update_local_path(file.id, file.local_path)
            
            if len(http_assets) > 0:
                self.download_HTTP_files(http_assets=http_assets)

    def _download_S3_files(self, s3_assets: List[AssetEntry]):
        """ 
        Downloads a list of files from S3.

        Args:
            s3_assets (List[AssetEntry[str, str]]): A list of S3 assets to download.
        """

        s3_entries_processed = []
        for file in s3_assets:
            _path = Path(file.remote_path)
            s3_entries_processed.append({
                "bucket":(bucket :=_path.root),
                "prefix":_path.relative_to(bucket)
            })

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            local_path_results = executor.map(self._S3_download_file, s3_entries_processed)
            for local_downloaded_path, file_asset in zip(local_path_results, s3_assets):
                if local_downloaded_path is not None:
                    # Update the local path in the AssetEntry
                    file_asset.local_path = str(local_downloaded_path)
                    # Update catalog with local path
                    self.catalog.update_local_path(id=file_asset.id, 
                                                   local_path=file_asset.local_path)

    def _S3_download_file(self, client, bucket: str, prefix: str) -> Path:
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
            logger.info(f"Downloading {prefix} to {local_path}")
            client.download_file(Bucket=bucket, 
                                 Key=str(prefix), 
                                 Filename=str(local_path))
            logger.info(f"Downloaded {str(prefix)} to {str(local_path)}")

        except Exception as e:
            logger.error(f"Error downloading {prefix} from {bucket }\n {e} \n HINT: $ aws sso login")
            local_path = None

        finally:
            return local_path

    def download_HTTP_files(self, http_assets: List[AssetEntry]):
        """ 
        Download HTTP files with progress bar and updates the catalog with the local path. 
        
        Args:
            http_assets (List[AssetEntry]): A list of HTTP assets to download.
        """

        for file_asset in tqdm(http_assets, desc="Downloading HTTP Files"):
            if (local_path := self._HTTP_download_file(file_asset.remote_path)) is not None:
                # Update the local path in the AssetEntry
                file_asset.local_path = str(local_path)
                # Update catalog with local path
                self.catalog.update_local_path(id=file_asset.id, 
                                               local_path=file_asset.local_path)


    def _HTTP_download_file(self, remote_url: Path, token_path='.') -> Path:
        """
        Downloads a file from the specified https url on gage-data

        Args:
            remote_url (Path): The path of the file in the gage-data storage.
            destination (Path): The local path where the file will be downloaded.
            token_path (str): The path to the token file for authentication.

        Returns:
            local_path (Path): The local path where the file was downloaded, or None if the download failed.
        """
        try:
            local_path = self.raw_dir / Path(remote_url).name
            download_file_from_archive(url=remote_url, 
                                       dest_dir=self.raw_dir, 
                                       token_path=token_path,
                                       )
            
            if not local_path.exists(): 
                raise Exception

            logger.info(f"Downloaded {str(remote_url)} to {str(local_path)}")

        except Exception as e:
            logger.error(f"Error downloading {str(remote_url)} \n {e}" + "\n HINT: Check authentication credentials")
            local_path = None

        finally:
            return local_path

    def view_data(self):
        shotdata_dates = self.shotdata_tdb.get_unique_dates().tolist()
        gnss_dates = self.gnss_tdb.get_unique_dates().tolist()
        date_set = shotdata_dates + gnss_dates
        date_set = sorted(list(set(date_set)))
       
        date_tick_map = {date:i for i, date in enumerate(date_set)}
        fig, ax = plt.subplots()
        # plot the gnss dates with red vertical line
        gnss_x = [date_tick_map[date] for date in gnss_dates]
        gnss_y = [1 for _ in gnss_dates]
   
        ax.scatter(x=gnss_x,y=gnss_y,c='r', marker='o',label='Pride GNSS Positions')
        # plot the shotdata dates with blue vertical line
        shotdata_x = [date_tick_map[date] for date in shotdata_dates]
        shotdata_y = [2 for _ in shotdata_dates]
        ax.scatter(x=shotdata_x,y=shotdata_y,c='b', marker='o',label='ShotData')
        ax.xaxis.set_ticks(
            [i for i in date_tick_map.values()],
            [str(date) for date in date_tick_map.keys()],
        )
        ax.yaxis.set_ticks([])
        ax.set_xlabel("Date")
        fig.legend()
        fig.suptitle(f"Found Dates For {self.network} {self.station}")
        plt.show()
  
    @check_network_station_survey
    def run_novatel_pipeline(self, override:bool=False, show_details:bool=False):
        # Pass catalog to pipeline
        pipeline = SV3Pipeline(catalog=self.catalog)

        pipeline.process_novatel(
            network=self.network,
            station=self.station,
            survey=self.survey,
            writedir=self.inter_dir,
            override=override,
            show_details=show_details)
        
    @check_network_station_survey
    def run_rinex_pipeline(self, override:bool=False, show_details:bool=False):
        # Pass catalog to pipeline
        pipeline = SV3Pipeline(catalog=self.catalog)

        pipeline.process_rinex(
            network=self.network,
            station=self.station,
            survey=self.survey,
            inter_dir=self.inter_dir,
            pride_dir=self.pride_dir,
            override=override,
            show_details=show_details,
        )
    
    @check_network_station_survey
    def run_dfop00_pipeline(self, override:bool=False):
        # Pass catalog to pipeline
        pipeline = SV3Pipeline(catalog=self.catalog)

        pipeline.process_dfop00(
            network=self.network,
            station=self.station,
            survey=self.survey,
            override=override,
            shotdatadest=self.shotdata_tdb,
        )

    @check_network_station_survey
    def run_kin_pipeline(self, override:bool=False, show_details:bool=False):
        # Pass catalog to pipeline
        pipeline = SV3Pipeline(catalog=self.catalog)

        pipeline.process_kin(
            network=self.network,
            station=self.station,
            survey=self.survey,
            gnss_tdb=self.gnss_tdb,
            override=override,
            show_details=show_details,
        )

    @check_network_station_survey
    def update_shotdata(self, override:bool=False, plot:bool=False):
        # Pass catalog to pipeline
        pipeline = SV3Pipeline(catalog=self.catalog)

        pipeline.update_shotdata(
            shotdatasource=self.shotdata_tdb,
            gnssdatasource=self.gnss_tdb,
            override=override,
            plot=plot
        )

    @check_network_station_survey
    def run_all_sv3_pipelines(self, override:bool=False, show_details:bool=False, plot:bool=False, update_shotdata:bool=False):
        # Pass catalog to pipeline
        logger.info(f"Running all SV3 pipelines for {self.network} {self.station} {self.survey}")

        logger.info("Starting Novatel Pipeline..")
        self.run_novatel_pipeline(override=override, show_details=show_details)
        logger.info("Done. \n Starting rinex pipeline..")
        self.run_rinex_pipeline(override=override, show_details=show_details)
        logger.info("Done. \n Starting DFOP00 pipeline..")
        self.run_dfop00_pipeline(override=override)
        logger.info("Done. \n Starting Kin pipeline..")
        self.run_kin_pipeline(override=override, show_details=show_details)
        logger.info("Done.")
        if update_shotdata:
            logger.info("Updating ShotData..")
            self.update_shotdata(override=override, plot=plot)
            logger.info("Done.")
        logger.info("\n All SV3 pipelines complete.")

