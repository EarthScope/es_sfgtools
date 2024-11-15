import os
from pathlib import Path
from typing import List,Callable,Union,Generator,Tuple,LiteralString,Optional,Dict,Literal
import pandas as pd
import datetime
import logging
import pandera as pa
import uuid
import boto3
from enum import Enum
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn 
from tqdm.auto import tqdm 
from multiprocessing import Pool, cpu_count
from functools import partial
import numpy as np
import warnings
import folium
import json
import concurrent.futures
import itertools
import logging
import multiprocessing
import threading

import time
import itertools
# import multiprocessing_logging
from functools import wraps
warnings.filterwarnings("ignore")
seaborn.set_theme(style="whitegrid")
from es_sfgtools.utils.archive_pull import download_file_from_archive
from es_sfgtools.processing.assets.file_schemas import AssetEntry,AssetType,MultiAssetEntry,MultiAssetPre
from es_sfgtools.processing.operations import sv2_ops,sv3_ops,gnss_ops,site_ops
from es_sfgtools.processing.assets import observables,siteconfig,constants,file_schemas
from es_sfgtools.modeling.garpos_tools import schemas as modeling_schemas
from es_sfgtools.modeling.garpos_tools import functions as modeling_funcs
from es_sfgtools.modeling.garpos_tools import hyper_params
from es_sfgtools.processing.assets.tiledb_temp import TDBAcousticArray,TDBGNSSArray,TDBPositionArray,TDBShotDataArray
from es_sfgtools.processing.operations.utils import merge_shotdata_gnss
from es_sfgtools.processing.pipeline.catalog import Catalog


from es_sfgtools.processing.pipeline.pipelines import SV3Pipeline
from es_sfgtools.processing.pipeline.constants import FILE_TYPE,DATA_TYPE,REMOTE_TYPE,ALIAS_MAP,FILE_TYPES
from es_sfgtools.processing.pipeline.datadiscovery import scrape_directory_local, get_file_type_local, get_file_type_remote
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

def check_network_station_survey(func:Callable):
    @wraps(func)
    def wrapper(self,*args,**kwargs):
        if self.network is None:
            raise ValueError("Network name not set, use change_working_station")
        if self.station is None:
            raise ValueError("Station name not set, use change_working_station")
        if self.survey is None:
            raise ValueError("Survey name not set, use change_working_survey")
        return func(self,*args,**kwargs)
    return wrapper

class DataHandler:
    """
    A class to handle data operations such as adding campaign data, downloading data, and processing data.
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

        Returns:
            None
        """

        self.network = network
        self.station = station
        self.survey = survey
   
        # Create the main & pride directory
        self.main_directory = Path(directory)
        self.pride_dir = self.main_directory / "Pride"
        self.pride_dir.mkdir(exist_ok=True, parents=True)

        # Create the catalog
        self.db_path = self.main_directory / "catalog.sqlite"
        if not self.db_path.exists():
            self.db_path.touch()
        self.catalog = Catalog(self.db_path)



    def build_station_dir_structure(self, network: str, station: str):
        """
        Build the directory structure for a station.
        Format is as follows:
            - SFGDirectory/
                - <network>/
                    - <station>/
                        - TileDB/
                        - Data/
                            - raw/
                            - intermediate/
                            - processed/  
        """

        # Create the network/station directory structure
        self.station_dir = self.main_directory / network / station
        self.station_dir.mkdir(parents=True,exist_ok=True)

        # Create the TileDB directory structure (network/station/TileDB)
        self.tileb_dir = self.station_dir / "TileDB"
        self.tileb_dir.mkdir(exist_ok=True)

        # Create the Data directory structure (network/station/Data)
        data_dir = self.station_dir / "Data"
        data_dir.mkdir(exist_ok=True)

        # Create the raw, intermediate, and processed directories (network/station/Data/raw) and store as class attributes
        self.raw_dir = data_dir / "raw"
        self.raw_dir.mkdir(exist_ok=True)

        self.inter_dir = data_dir / "intermediate"
        self.inter_dir.mkdir(exist_ok=True)

        self.proc_dir = data_dir / "processed"
        self.proc_dir.mkdir(exist_ok=True)

    def build_tileDB_arrays(self):
        """
        Build the TileDB arrays for the current station. TileDB directory is /network/station/TileDB
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

        Returns:
            None
        """
        # Set class attributes & create the directory structure
        self.network = network
        self.station = station
        if survey is not None:
            self.survey = survey

        # Build the directory structure and TileDB arrays
        self.build_station_dir_structure(network, station)
        self.build_tileDB_arrays()

        logger.info(f"Changed working station to {network} {station}")


    @check_network_station_survey
    def get_dtype_counts(self):
        return self.catalog.get_dtype_counts(network=self.network, 
                                             station=self.station, 
                                             survey=self.survey)

    @check_network_station_survey
    def _add_data_local(self,
                        local_filepaths:List[AssetEntry],
                        show_details:bool=True):
        count = 0
        file_data_list = []
        for discovered_file in local_filepaths:

            file_data = discovered_file.model_dump() | {
                "network": self.network,
                "station": self.station,
                "survey": self.survey,
                "timestamp_created": datetime.datetime.now(),
            }

            file_data_list.append(file_data)
            count += 1

        # See if the data is already in the catalog
        file_paths = [AssetEntry(**x) for x in file_data_list]
        uploadCount = 0
        for asset in file_paths:
            try:
                if self.catalog.add_entry(asset):
                    uploadCount += 1
            except Exception as e:
                pass
        response = f"Added {uploadCount} out of {count} files to the catalog"
        logger.info(response)
        if show_details:
            print(response)

    def discover_data_directory(self, network:str, station:str, survey:str, dir_path:Path, show_details:bool=True):
        """
        For a given directory of data, iterate through all files and add them to the catalog.
        
        Note: Be sure to correctly set the network, station, and survey before running this function.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            dir_path (Path): The directory path.
            show_details (bool): Log details of each file added.

        Returns:
            None
        """
        self.change_working_station(network=network,
                                    station=station,
                                    survey=survey)

        files = scrape_directory_local(dir_path)
        if len(files) == 0:
            response = f"No files found in {dir_path}"
            logger.error(response)
            if show_details:
                print(response)
            return

        self._add_data_local(files,show_details=show_details)

    @check_network_station_survey
    def add_data_local(self,
                        local_filepaths:Union[List[Union[str,Path]],str],
                        show_details:bool=True):
        
        if isinstance(local_filepaths,str):
            local_filepaths = [Path(local_filepaths)]

        discovered_files : List[AssetEntry] = [get_file_type_local(file) for file in local_filepaths]
        discovered_files = [x for x in discovered_files if x is not None]

        if len(discovered_files) == 0:
            response = f"No files found in {local_filepaths}"
            logger.error(response)
            if show_details:
                print(response)
            return
        
        self._add_data_local(discovered_files,show_details=show_details)

    def add_data_remote(self, 
                        remote_filepaths: List[str],
                        remote_type:Union[REMOTE_TYPE,str] = REMOTE_TYPE.HTTP,
                        show_details:bool=True):
        """
        Add campaign data to the catalog.

        Args:
            remote_filepaths (List[str]): A list of file locations on gage-data.
            remote_type (Union[REMOTE_TYPE,str]): The type of remote location.

        Returns:
            None
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
                continue

            file_data = AssetEntry(
                remote_path=file,
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
            print("Adding", file_assest)
            if self.catalog.add_entry(file_assest):
                uploadCount += 1
        response = f"Added {uploadCount} out of {count} files to the catalog"
        logger.info(response)

        if show_details:
            print(response)

    def download_data(self, file_types: List = FILE_TYPES, override: bool=False, show_details:bool=True):
        """
        Retrieves and catalogs data from the remote locations stored in the catalog.

        Args:
            file_type (str): The type of file to download
            override (bool): Whether to download the data even if it already exists
            show_details (bool): Log details of each file downloaded  

        Raises:
            Exception: If no matching data found in catalog.
        """

        # Grab assests from the catalog that match the network, station, survey, and file type
        assets = self.catalog.get_assets(network=self.network,
                                       station=self.station,
                                       survey=self.survey,
                                       types=file_types)
        print(assets)

        if len(assets) == 0:
            response = f"No matching data found in catalog"
            logger.error(response)
            print(response)
            return
        
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
                    if not Path(file_asset.local_path).exists():
                        assets_to_download.append(file_asset)

        if len(assets_to_download) == 0:
            logger.info(f"No new files to download")
        
        # split the entries into s3 and http
        s3_assets = [x for x in assets if x['remote_type'] == REMOTE_TYPE.S3.value]
        http_assets = [x for x in assets if x['remote_type'] == REMOTE_TYPE.HTTP.value]

        if len(s3_assets) > 0:
            with threading.Lock(): # TODO is this necessary?
                client = boto3.client('s3')
            self._download_S3_files(client=client,
                                    s3_assets=s3_assets, 
                                    show_details=show_details)
            # TODO update catalog with local path
        
        if len(http_assets) > 0:
            self.download_HTTP_files(http_assets=http_assets, 
                                      show_details=show_details)
            # TODO update catalog with local path


        # download http entries
        # TODO: re-implement multithreading, switched to serial downloading.
        # need to solve cataloging each file after download and making progress bar work in parallel


    def _download_S3_files(self, s3_assets: List[AssetEntry[str, str]]):
        """ 
        Download a list of files from S3.

        Args:
            s3_assets (List[AssetEntry[str, str]]): A list of S3 assets to download.
        
        Returns:
            None
        """

        s3_entries_processed = []
        for file in s3_assets:
            _path = Path(file.remote_path)
            s3_entries_processed.append({
                "bucket":(bucket :=_path.root),
                "prefix":_path.relative_to(bucket)
            })

        with concurrent.futures.ThreadPoolExecutor() as executor:
            local_path_results = executor.map(self._S3_download_file, s3_entries_processed)
            for local_downloaded_path, file_asset in zip(local_path_results, s3_assets):
                if local_downloaded_path is not None:
                    # Update the local path in the AssetEntry
                    file_asset.local_path = str(local_downloaded_path)
                    # Update catalog with local path

    def _S3_download_file(self, client, bucket: str, prefix: str) -> Union[Path,None]:
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
            response = f"Downloaded {str(prefix)} to {str(local_path)}"
            logger.info(response)

        except Exception as e:
            logger.error(f"Error downloading {prefix} from {bucket }\n {e} \n HINT: $ aws sso login")
            local_path = None

        finally:
            return local_path

    def download_HTTP_files(self, http_assets: List[AssetEntry[str, str]], show_details: bool = True):
        if len(http_assets) > 0:
            _download_func = partial(self.HTTP_download_file, 
                                     destination_dir=self.raw_dir, 
                                     show_details=show_details)
            
            for file_asset in tqdm(http_assets, total=len(http_assets), desc=f"Downloading {file_asset.remote_path}"):
                if (local_path :=_download_func(file_asset.remote_path)) is not None:
                    file_asset.local_path = str(local_path)

    

    def HTTP_download_file(self, remote_url: Path, destination_dir: Path, token_path='.') -> Union[Path, None]:
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
            local_path = destination_dir / Path(remote_url).name
            download_file_from_archive(url=remote_url, 
                                       dest_dir=destination_dir, 
                                       token_path=token_path,
                                       )
            
            if not local_path.exists(): # TODO: check if this is necessary
                raise Exception

            response = f"Downloaded {str(remote_url)} to {str(local_path)}"
            logger.info(response)

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
    def pipeline_sv3(self,override:bool=False,show_details:bool=False,plot:bool=False):
        pipeline = SV3Pipeline(catalog=self.catalog)
        pipeline.process_novatel(
            network=self.network,
            station=self.station,
            survey=self.survey,
            writedir=self.inter_dir,
            override=False,
            show_details=show_details)
        
        pipeline.process_rinex(
            network=self.network,
            station=self.station,
            survey=self.survey,
            inter_dir=self.inter_dir,
            pride_dir=self.pride_dir,
            override=override,
            show_details=show_details,
        )

        pipeline.process_dfop00(
            network=self.network,
            station=self.station,
            survey=self.survey,
            override=override,
            shotdatadest=self.shotdata_tdb,
        )
        pipeline.process_kin(
            network=self.network,
            station=self.station,
            survey=self.survey,
            gnss_tdb=self.gnss_tdb,
            override=override,
            show_details=show_details,
        )
        pipeline.update_shotdata(
            shotdatasource=self.shotdata_tdb,
            gnssdatasource=self.gnss_tdb,
            override=override,
            plot=plot
        )
