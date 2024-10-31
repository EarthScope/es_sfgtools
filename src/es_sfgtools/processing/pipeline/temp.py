import os
from pathlib import Path
from typing import List,Callable,Union,Generator,Tuple,LiteralString,Optional,Dict
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
import multiprocessing_logging
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
from es_sfgtools.processing.operations.utils import get_merge_signature,merge_shotdata_gnss
from .catalog import Catalog


from .pipelines import SV3Pipeline
from .constants import FILE_TYPE,DATA_TYPE,REMOTE_TYPE,ALIAS_MAP,FILE_TYPES
from .datadiscovery import scrape_directory_local,get_file_type_local,get_file_type_remote
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
                 directory:Path | str
                 ) -> None:
        """
        Initialize the DataHandler object.

        Creates the following files and directories within the data directory if they do not exist:
            - SFGDirectory/
                - catalog.sqlite
                - Pride/
                - <network>/
                    - <station>/
                        - TileDB/
                            - acoustic_db.tdb
                            - gnss_db.tdb
                            - position_db.tdb
                            - shotdata_db.tdb

                        - Data/
                            - raw/
                            - intermediate/
                            - processed/
                            - Garpos


        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            data_dir (Path): The working directory path.

        Returns:
            None
        """
   
        if isinstance(directory,str):
            directory = Path(data_dir)
    
        self.main_directory = directory
        self.pride_dir = self.main_directory / "Pride"

        self.db_path = self.main_directory / "catalog.sqlite"
        self.pride_dir.mkdir(exist_ok=True,parents=True)
        if not self.db_path.exists():
            self.db_path.touch()

        self.catalog = Catalog(self.db_path)
        self.network = None
        self.station = None
        self.survey = None


    def build_station_dir_structure(self,network:str,station:str):
        station_dir = self.main_directory / network / station
        station_dir.mkdir(parents=True,exist_ok=True)
        tileb_dir = station_dir / "TileDB"
        tileb_dir.mkdir(exist_ok=True)
        data_dir = station_dir / "Data"
        data_dir.mkdir(exist_ok=True)
        raw_dir = data_dir / "raw"
        raw_dir.mkdir(exist_ok=True)
        inter_dir = data_dir / "intermediate"
        inter_dir.mkdir(exist_ok=True)
        proc_dir = data_dir / "processed"
        proc_dir.mkdir(exist_ok=True)
    
    def change_working_station(self,network:str,station:str):
        self.network = network
        self.station = station
        self.build_station_dir_structure(network,station)
        self.working_dir = self.main_directory / self.network / self.station
        self.tileb_dir = self.working_dir / "TileDB"
        self.acoustic_tdb = TDBAcousticArray(self.tileb_dir/"acoustic_db.tdb")
        self.gnss_tdb = TDBGNSSArray(self.tileb_dir/"gnss_db.tdb")
        self.position_tdb = TDBPositionArray(self.tileb_dir/"position_db.tdb")
        self.shotdata_tdb = TDBShotDataArray(self.tileb_dir/"shotdata_db.tdb")
        self.build_station_dir_structure(self.network,self.station)
        response = f"Changed working station to {network} {station}"
        logger.info(response)
        print(response)
    
    def change_working_survey(self,survey:str):
        self.survey = survey
    

    @check_network_station_survey
    def get_dtype_counts(self):
        return self.catalog.get_dtype_counts(network=self.network,station=self.station,survey=self.survey)

    @check_network_station_survey
    def _add_data_local(self,
                        local_filepaths:List[AssetEntry],
                        show_details:bool=True,
                        **kwargs):
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
            if self.catalog.add_entry(asset):
                uploadCount += 1
        response = f"Added {uploadCount} out of {count} files to the catalog"
        logger.info(response)
        if show_details:
            print(response)

    def discover_data_directory(self,network:str,station:str,survey:str,dir_path:Path,show_details:bool=True):
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
        self.change_working_station(network,station)
        self.change_working_survey(survey)
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
                        show_details:bool=True,
                        **kwargs):
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
                          show_details:bool=True,
                          **kwargs):
        """
        Add campaign data to the catalog.

        Args:
            remote_filepaths (List[str]): A list of file locations on gage-data.
            remote_type (Union[REMOTE_TYPE,str]): The type of remote location.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        if isinstance(remote_type,str):
            try:
                remote_type = REMOTE_TYPE(remote_type)
            except:
                raise ValueError(f"Remote type {remote_type} must be one of {REMOTE_TYPE.__members__.keys()}")

        file_data_list = []
        for file in remote_filepaths:
            discovered_file: Union[AssetEntry,None] = get_file_type_remote(file)
            # raise ValueError(f"File type not recognized for {file}")
            if discovered_file is None:
                continue

            file_data = AssetEntry(**(discovered_file.model_dump() | {
                "network": self.network,
                "station": self.station,
                "survey": self.survey,
                "timestamp_created": datetime.datetime.now(),
            }
            ))
            file_data_list.append(file_data)

        count = len(file_data_list)
        uploadCount = 0
        for asset in file_data_list:
            if self.catalog.add_entry(asset):
                uploadCount += 1
        response = f"Added {uploadCount} out of {count} files to the catalog"
        logger.info(response)
        if show_details:
            print(response)

    def download_data(self,
                    file_type: str="all",
                    override:bool=False,
                    show_details:bool=True):
        """
        Retrieves and catalogs data from the remote locations stored in the catalog.

        Args:
            file_type (str): The type of file to download
            override (bool): Whether to download the data even if it already exists
            from_s3 (bool): Use S3 download functionality if remote resourses are in an s3 bucket
            show_details (bool): Log details of each file downloaded  

        Raises:
            Exception: If no matching data found in catalog.
        """
        # os.environ["DH_SHOW_DETAILS"] = str(show_details)
        if file_type == 'all':
            file_types = FILE_TYPES
        else:
            file_types = [file_type]
        with self.engine.begin() as conn:
            entries = [dict(row._mapping) for row in conn.execute(
                sa.select(Assets).where(
                    Assets.network.in_([self.network]),Assets.station.in_([self.station]),Assets.survey.in_([self.survey]),Assets.type.in_(file_types)
                )
            ).fetchall()]
        if len(entries) == 0:
            response = f"No matching data found in catalog"
            logger.error(response)
            print(response)
            return
        # find entries that have a value for "local_path"
        entries_to_get = []
        for entry in entries:
            if entry['local_path'] is not None:
                if Path(entry['local_path']).exists():
                    entries_to_get.append(False)
                else:
                    entries_to_get.append(True)
            else:
                entries_to_get.append(True)

        to_get = np.logical_or(entries_to_get,override)
        entries = np.array(entries)[to_get].tolist()
        if len(entries) == 0:
            response = f"No new files of type {file_type} to download"
            logger.info(response)
            print(response)
            return
        # split the entries into s3 and http
        s3_entries = [x for x in entries if x['remote_type'] == REMOTE_TYPE.S3.value]
        http_entries = [x for x in entries if x['remote_type'] == REMOTE_TYPE.HTTP.value]
        updated_entries = []
        # download s3 entries
        if len(s3_entries) > 0:
            s3_entries_processed = []
            for entry in s3_entries:
                _path = Path(entry['remote_path'])
                s3_entries_processed.append({
                    "bucket":(bucket :=_path.root),
                    "prefix":_path.relative_to(bucket)
                })
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(self._download_data_s3,s3_entries_processed)
                for result,entry in zip(results,s3_entries):
                    if result is not None:
                        entry['local_path'] = str(result)
                        updated_entries.append(entry)

        # download http entries
        # TODO: re-implement multithreading, switched to serial downloading.
        # need to solve cataloging each file after download and making progress bar work in parallel

        if len(http_entries) > 0:
            _download_func = partial(self._download_https,destination_dir=self.raw_dir, show_details=show_details)
            for entry in tqdm(http_entries, total=len(http_entries), desc=f"Downloading {file_type} files"):
                if (local_path :=_download_func(entry['remote_path']))  is not None:
                    entry["local_path"] = str(local_path)
                    with self.engine.begin() as conn:
                        conn.execute(
                            sa.update(Assets).where(Assets.remote_path == entry['remote_path']).values(dict(entry))
                        )

    def _download_data_s3(self,bucket:str,prefix:str,**kwargs) -> Union[Path,None]:
        """
        Retrieves and catalogs data from the s3 locations stored in the catalog.

        Args:
            bucket (str): S3 bucket name
            prefix (str): S3 object prefix

        Raises:
            Exception: If no matching data found in catalog.
        """
        with threading.Lock():
            client = boto3.client('s3')

        local_path = self.raw_dir / Path(prefix).name

        # If the file does not exist and has not been processed, then download it!
        try:
            client.download_file(Bucket=bucket, Key=str(prefix), Filename=str(local_path))
            response = f"Downloaded {str(prefix)} to {str(local_path)}"
            logger.info(response)
            if os.environ.get("DH_SHOW_DETAILS",False):
                print(response)
            return local_path

        except Exception as e:
            response = f"Error downloading {prefix} \n {e}"
            response += "\n HINT: $ aws sso login"
            logger.error(response)
            if os.environ.get("DH_SHOW_DETAILS",False):
                print(response)
            return None

    def _download_https(self, 
                        remote_url: Path, 
                        destination_dir: Path, 
                        token_path='.',
                        show_details: bool=True) -> Union[Path,None]:
        """
        Downloads a file from the specified https url on gage-data

        Args:
            remote_url (Path): The path of the file in the gage-data storage.
            destination (Path): The local path where the file will be downloaded.

        Returns:
            bool: True if the file was downloaded successfully, False otherwise.
        """
        try:
            local_path = destination_dir / Path(remote_url).name
            download_file_from_archive(url=remote_url, 
                                    dest_dir=destination_dir, 
                                    token_path=token_path,
                                    )
            if not local_path.exists():
                raise Exception

            response = f"Downloaded {str(remote_url)} to {str(local_path)}"
            logger.info(response)
            if show_details:
                print(response)
            return local_path

        except Exception as e:
            response = f"Error downloading {str(remote_url)} \n {e}"
            response += "\n HINT: Check authentication credentials"
            logger.error(response)
            if show_details:
                print(response)
            return None

 


    @check_network_station_survey
    def pipeline_sv3(self,override:bool=False,show_details:bool=False,plot:bool=False):
        pipeline = SV3Pipeline(catalog=self.catalog)
        pipeline.process_novatel(network=self.network,station=self.station,survey=self.survey,override=override,show_details=show_details)
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
            shotdatadest=self.shotdata_tdb,
        )
        pipeline.update_shotdata(
            shotdatasource=self.shotdata_tdb,
            gnssdatasource=self.gnss_tdb,
            plot=plot
        )

