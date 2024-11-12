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
from es_sfgtools.processing.operations.utils import merge_shotdata_gnss
from .catalog import Catalog

from .plotting import plot_gnss_data
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
            directory = Path(directory)
    
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
        self.raw_dir = self.working_dir / "Data" / "raw"
        self.inter_dir = self.working_dir / "Data" / "intermediate"
        self.proc_dir = self.working_dir / "Data" / "processed"
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
            try:
                if self.catalog.add_entry(asset):
                    uploadCount += 1
            except Exception as e:
                pass
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

    def view_data(self):

        rinex_assets = self.catalog.get_asset_by_type(AssetType.RINEX)
        plot_gnss_data(self.gnss_tdb,rinex_assets)

  
    @check_network_station_survey
    def get_asset_by_type(self,type:AssetType|str) -> List[AssetEntry] | None:
        if isinstance(type,str):
            try:
                type = AssetType(type.lower())
            except:
                raise ValueError(f"AssetType {type} must be one of {AssetType.__members__.keys()}")
        found_assets:List[AssetEntry] = self.catalog.get_assets(self.network,self.station,self.survey,type)
        if len(found_assets) == 0:
            response = f"No {type} assets found"
            logger.error(response)
            print(response)
            return
        return found_assets
    
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

