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

import sqlalchemy as sa
from .database import Base,Assets,MultiAssets,ModelResults
from .constants import FILE_TYPE,DATA_TYPE,REMOTE_TYPE,ALIAS_MAP,FILE_TYPES
from .datadiscovery import scrape_directory_local,get_file_type_local,get_file_type_remote
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

TARGET_MAP = {
    AssetType.QCPIN:{AssetType.SHOTDATA:sv3_ops.dev_qcpin_to_shotdata},
    AssetType.NOVATEL:{AssetType.RINEX:gnss_ops.novatel_to_rinex,AssetType.POSITION:sv2_ops.novatel_to_positiondf},
    AssetType.RINEX:{AssetType.KIN:gnss_ops.rinex_to_kin},
    AssetType.KIN:{AssetType.GNSS:gnss_ops.kin_to_gnssdf},
    AssetType.SONARDYNE:{AssetType.ACOUSTIC:sv2_ops.sonardyne_to_acousticdf},
    AssetType.MASTER:{AssetType.SITECONFIG:site_ops.masterfile_to_siteconfig},
    AssetType.LEVERARM:{AssetType.ATDOFFSET:site_ops.leverarmfile_to_atdoffset},
    AssetType.SEABIRD:{AssetType.SVP:site_ops.seabird_to_soundvelocity},
    AssetType.NOVATEL770:{AssetType.RINEX:gnss_ops.novatel_to_rinex},
    AssetType.DFOP00:{AssetType.SHOTDATA:sv3_ops.dev_dfop00_to_shotdata},
}


# Reverse the target map so we can get the parent type from the child type keys
# Format should be {child_type:[parent_type_0,parent_type_1,..]}
SOURCE_MAP = {}
for parent,children in TARGET_MAP.items():
    for child in children.keys():
        if not SOURCE_MAP.get(child,[]):
            SOURCE_MAP[child] = []
        SOURCE_MAP[child].append(parent)


class MergeFrequency(Enum):
    HOUR = "h"
    DAY = 'D'

class DataHandler:
    """
    A class to handle data operations such as adding campaign data, downloading data, and processing data.
    """

    def __init__(self,
                 network: str,
                 station:str,
                 survey:str,
                 data_dir:Union[Path,str],
                 show_details:bool=True
                 ) -> None:
        """
        Initialize the DataHandler object.

        Creates the following files and directories within the data directory if they do not exist:
            - catalog.sqlite
            - <network>/
                - <station>/
                    - <tileDB>/
                        - acoustic_db.tdb
                        - gnss_db.tdb
                        - position_db.tdb
                        - shotdata_db.tdb

                    - <survey>/
                        - raw/
                        - intermediate/
                        - processed/
                        - Garpos
             - Pride/

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            data_dir (Path): The working directory path.

        Returns:
            None
        """
        self.network = network
        self.station = station
        self.survey = survey

        if isinstance(data_dir,str):
            data_dir = Path(data_dir)

        self.data_dir = data_dir
        self.working_dir = self.data_dir / self.network / self.station / self.survey
        self.raw_dir = self.working_dir / "raw/"
        self.inter_dir = self.working_dir / "intermediate/"
        self.proc_dir = self.working_dir / "processed/"
        self.pride_dir = self.data_dir / "Pride"
        self.garpos_dir = self.working_dir / "Garpos"
        self.tileb_dir = self.working_dir / "tileDB"
        self.working_dir.mkdir(parents=True,exist_ok=True)
        self.pride_dir.mkdir(parents=True,exist_ok=True)
        self.inter_dir.mkdir(exist_ok=True)
        self.proc_dir.mkdir(exist_ok=True)
        self.raw_dir.mkdir(exist_ok=True)
        self.garpos_dir.mkdir(exist_ok=True)

        self.tileb_dir.mkdir(exist_ok=True)

        self.db_path = self.data_dir/"catalog.sqlite"
        if not self.db_path.exists():
            self.db_path.touch()

        self.acoustic_tdb = TDBAcousticArray(self.tileb_dir/"acoustic_db.tdb")
        self.gnss_tdb = TDBGNSSArray(self.tileb_dir/"gnss_db.tdb")
        self.position_tdb = TDBPositionArray(self.tileb_dir/"position_db.tdb")
        self.shotdata_tdb = TDBShotDataArray(self.tileb_dir/"shotdata_db.tdb")
        
        self.catalog = Catalog(self.db_path)

        logging.basicConfig(level=logging.INFO,
                            format="{asctime} {message}",
                            style="{",
                            datefmt="%Y-%m-%d %H:%M:%S",
                            filename=self.working_dir/"datahandler.log")
        response = f"Data Handler initialized, data will be stored in {self.working_dir}"
        logger.info(response)
        if show_details:
            print(response)

    def get_dtype_counts(self):
        return self.catalog.get_dtype_counts(network=self.network,station=self.station,survey=self.survey)

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

    def add_data_directory(self,dir_path:Path,show_details:bool=True):
        """
        Add all files in a directory to the catalog.

        Args:
            dir_path (Path): The directory path.
            show_details (bool): Log details of each file added.

        Returns:
            None
        """
        files = scrape_directory_local(dir_path)
        if len(files) == 0:
            response = f"No files found in {dir_path}"
            logger.error(response)
            if show_details:
                print(response)
            return

        self._add_data_local(files,show_details=show_details)

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

    def get_parent_stack(
        self, child_type: AssetType
    ) -> List[AssetType]:
        """
        Get a list of parent types for a given child type.

        Args:
            child_type (Union[FILE_TYPE,DATA_TYPE]): The child type.

        Returns:
            List[Union[FILE_TYPE,DATA_TYPE]]: A list of parent types.
        """
        stack = [child_type]
        pointer = 0
        while pointer < len(stack):
            parents: List[AssetType] = SOURCE_MAP.get(
                stack[pointer], []
            )
            for parent in parents:
                stack.append(parent)
            pointer += 1
        return stack[::-1]

    def get_child_stack(
        self, parent_type: AssetType) -> List[AssetType]:

        stack = [parent_type]
        pointer = 0
        while pointer < len(stack):
            children: List[Union[FILE_TYPE, DATA_TYPE]] = list(TARGET_MAP.get(stack[pointer],{}).keys())
            for child in children:
                stack.append(child)
            pointer += 1
        return stack

    @staticmethod
    def _partial_function(process_func:Callable,parent:AssetEntry,inter_dir:Path,pride_dir:Path,show_details:bool=False) -> Callable:
        match process_func:
            case gnss_ops.rinex_to_kin:

                process_func_p = partial(
                    process_func,
                    writedir=inter_dir,
                    pridedir=pride_dir,
                    site=parent.station,
                    show_details=show_details,
                )
            case gnss_ops.novatel_to_rinex:
                try:
                    year = str(parent.timestamp_data_start.year)[2:]
                except:
                    year = None
                process_func_p = partial(
                    process_func,
                    writedir=inter_dir,
                    site=parent.station,
                    year=year,
                    show_details=show_details,
                )
            # case gnss_ops.qcpin_to_novatelpin:
            #     process_func_p = partial(process_func, writedir=inter_dir)

            case _:
                process_func_p = process_func
        return process_func_p

    @staticmethod
    def _process_targeted(
        parent: AssetEntry|MultiAssetEntry,
        child_type: AssetType,
        inter_dir: Path,
        proc_dir: Path,
        pride_dir: Path,
        show_details: bool = False,
    ) -> Union[Tuple[AssetEntry,AssetEntry, str],Tuple[None,None,str]]:

        response = f"Processing {parent.local_path} ({parent.id}) of Type {parent.type} to {child_type.value}\n"

        # Get the processing function that converts the parent entry to the child entry
        try:
            process_func = TARGET_MAP.get(parent.type).get(child_type)
        except KeyError:
            response += f"  No processing function found for {parent.type} to {child_type.value}\n"
            logger.error(response)
            return None, None, response

        process_func_partial = DataHandler._partial_function(
            process_func=process_func,
            parent=parent,
            inter_dir=inter_dir,
            pride_dir=pride_dir,
            show_details=show_details,
        )

        try:
            processed = process_func_partial(parent)
            if processed is None:
                raise Exception(f"Processing failed for {parent.id}")
        except Exception as e:
            response += f"{process_func.__name__} failed with error: {e}"
            logger.error(response)
            return None, None, ''

        local_path = None
        timestamp_data_start = None
        timestamp_data_end = None
        match child_type:
            case (
                AssetType.GNSS
                | AssetType.ACOUSTIC
                | AssetType.POSITION
                | AssetType.SHOTDATA
            ):
                local_path = proc_dir / f"{parent.id}_{child_type.value}.csv"
                processed.to_csv(local_path, index=False)

                # handle the case when the child timestamp is None
                if pd.isna(parent.timestamp_data_end):
                    for col in processed.columns:
                        if pd.api.types.is_datetime64_any_dtype(processed[col]):
                            timestamp_data_start = processed[col].min()
                            timestamp_data_end = processed[col].max()
                            break
                else:
                    timestamp_data_start = parent.timestamp_data_start
                    timestamp_data_end = parent.timestamp_data_end

                local_path = (
                    proc_dir
                    / f"{parent.id}_{child_type.value}_{timestamp_data_start.date().isoformat()}.csv"
                )
                processed.to_csv(local_path, index=False)

                if isinstance(parent, MultiAssetEntry):
                    schema = MultiAssetEntry
                else:
                    schema = AssetEntry

                processed = schema(
                    local_path=local_path,
                    type=child_type,
                    parent_id=parent.id,
                    timestamp_data_start=timestamp_data_start,
                    timestamp_data_end=timestamp_data_end,
                    network=parent.network,
                    station=parent.station,
                    survey=parent.survey,
                )

            case AssetType.RINEX:
                local_path = processed.local_path
                timestamp_data_start = processed.timestamp_data_start
                timestamp_data_end = processed.timestamp_data_end

            case AssetType.KIN:
                local_path = processed.local_path

            case AssetType.SITECONFIG | AssetType.ATDOFFSET:
                local_path = proc_dir / f"{parent.id}_{child_type.value}.json"
                with open(local_path, "w") as f:
                    f.write(processed.model_dump_json())

            case AssetType.NOVATELPIN:
                local_path = inter_dir / f"{parent.id}_{child_type.value}.txt"
                processed.local_path = local_path
                processed.write(dir=local_path.parent)

            case _:
                local_path = None
                pass

        if (
            pd.isna(parent.timestamp_data_start)
            and processed.timestamp_data_start is not None
        ):
            parent.timestamp_data_start = processed.timestamp_data_start
            parent.timestamp_data_end = processed.timestamp_data_end
            response += f"  Discovered timestamp: {timestamp_data_start} for parent {parent.type.value} uuid {parent.id}\n"

        if not local_path.exists():
            response += f"  {child_type.value} not created for {parent.id}\n"
            logger.error(response)
            return None, parent, response

        return processed, parent, response

    def _process_entries(
            self,
            parent_entries: List[AssetEntry | MultiAssetEntry],
            child_type: AssetType,
            show_details: bool = False,
    ) -> List[Tuple[List[AssetEntry | MultiAssetEntry], List[AssetEntry | MultiAssetEntry]]]:

        process_func_partial = partial(
            self._process_targeted,
            child_type=child_type,
            inter_dir=self.inter_dir,
            proc_dir=self.proc_dir,
            pride_dir=self.pride_dir,
        )
        parent_data_list = []
        child_data_list = []
        source_values = list(set([x.type.value for x in parent_entries if x is not None]))
        with multiprocessing.Pool() as pool:
            results = pool.imap(process_func_partial, parent_entries)
            for child_data, parent_data, response in tqdm(
                results,
                total=len(parent_entries),
                desc=f"Processing {source_values} To {child_type.value}",
            ):
                if parent_data is not None and child_data is not None:
                    if parent_data.timestamp_data_start is None and child_data.timestamp_data_start is not None:
                        parent_data.timestamp_data_start = child_data.timestamp_data_start
                        parent_data.timestamp_data_end = child_data.timestamp_data_end
                    parent_data_list.append(parent_data)
                    child_data_list.append(child_data)

        response = f"Processed {len(child_data_list)} Out of {len(parent_entries)} For {child_type.value}"
        logger.info(response)
        if show_details:
            print(response)
        return parent_data_list, child_data_list

    def  _process_data_link(self,
                           target:AssetType | MultiAssetEntry,
                           source:AssetType | MultiAssetEntry,
                           override:bool=False,
                           parent_entries:Union[List[AssetEntry],List[MultiAssetEntry]]=None,
                           show_details:bool=False) -> Tuple[List[AssetEntry | MultiAssetEntry],List[AssetEntry | MultiAssetEntry]]:
        """
        Process data from a source to a target.

        Args:
            target (Union[FILE_TYPE,DATA_TYPE]): The target data type.
            source (List[FILE_TYPE]): The source data types.
            override (bool): Whether to override existing child entries.

        Raises:
            Exception: If no matching data is found in the catalog.
        """
        # Get the parent entries
        if parent_entries is None:
            parent_entries = self.catalog.get_single_entries_to_process(
                network=self.network,station=self.station,survey=self.survey,parent_type=source,child_type=target,override=override)

        if parent_entries is None:
            return [],[]
        parent_data_list,child_data_list = self._process_entries(parent_entries=parent_entries,child_type=target,show_details=show_details)
        for parent_data,child_data in zip(parent_data_list,child_data_list):

            self.catalog.add_or_update(parent_data)
            if child_data is not None:
                self.catalog.add_or_update(child_data)

        return parent_data_list,child_data_list

    def _process_data_graph(self, 
                            child_type:AssetType,
                            override:bool=False,
                            show_details:bool=False):

        msg = f"\nProcessing Upstream Data for {child_type.value}\n"
        logger.info(msg)
        if show_details: print(msg)

        processing_queue = self.get_parent_stack(child_type=child_type)
        while processing_queue:
            parent = processing_queue.pop(0)
            if parent != child_type:
                children:dict = TARGET_MAP.get(parent,{})
                children_to_process = [k for k in children.keys() if k in processing_queue]
                for child in children_to_process:
                    msg = f"\nProcessing {parent.value} to {child.value}"
                    logger.info(msg)
                    if show_details: print(msg)

                    self._process_data_link(
                        target=child,
                        source=parent,
                        override=override,
                        show_details=show_details)
        msg = f"Processed Upstream Data for {child_type.value}\n"
        logger.info(msg)
        if show_details: print(msg)

    def _process_data_graph_forward(self, 
                            parent_type:AssetType,
                            override:bool=False,
                            show_details:bool=False):

        processing_queue = [{parent_type:TARGET_MAP.get(parent_type)}]
        while processing_queue:
            # process each level of the child graph
            parent_targets = processing_queue.pop(0)
            parent_type = list(parent_targets.keys())[0]
            for child in parent_targets[parent_type].keys():

                self._process_data_link(target=child,source=parent_type,override=override,show_details=show_details)
                child_targets = TARGET_MAP.get(child,{})
                if child_targets:
                    processing_queue.append({child:child_targets})

    
    def process_novatel(self,override:bool=False,show_details:bool=False) -> List[AssetEntry]:

        novatel_770_entries: List[AssetEntry] = self.catalog.get_assets(
            network=self.network,station=self.station,survey=self.survey,type=AssetType.NOVATEL770
        )
       
        merge_signature = {
            "parent_type": AssetType.NOVATEL770.value,
            "child_type": AssetType.RINEX.value,
            "parent_ids": [x.id for x in novatel_770_entries],
        }
        if override or not self.catalog.is_merge_complete(**merge_signature):
            rinex_entries: List[AssetEntry] = gnss_ops.novatel_to_rinex_batch(
                source=novatel_770_entries,writedir=self.inter_dir,show_details=show_details
            )
            uploadCount = 0
            for rinex_entry in rinex_entries:
                if self.catalog.add_entry(rinex_entry):
                    uploadCount += 1
            self.catalog.add_merge_job(**merge_signature)
            response = f"Added {uploadCount} out of {len(rinex_entries)} Rinex Entries to the catalog"
            logger.info(response)
            if show_details:
                print(response)
            return rinex_entries

    def process_rinex(self,rinex_entries:List[AssetEntry]=None,override:bool=False,show_details:bool=False) ->None:

        """
        Process Rinex Data.
        Args:
            override (bool, optional): Flag to override existing data. Defaults to False.
            show_details (bool, optional): Flag to show processing details. Defaults to False.
        Raises:
            ValueError: If no Rinex files are found.
        """

        response = f"Processing Rinex Data for {self.network} {self.station} {self.survey}"
        logger.info(response)
        if show_details:
            print(response)
        if rinex_entries is None:
            rinex_entries: List[AssetEntry] = self.catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                survey=self.survey,
                parent_type=AssetType.RINEX,
                child_type=AssetType.KIN,
                override=override
            )
        if not rinex_entries:
            response = f"No Rinex Files Found to Process for {self.network} {self.station} {self.survey}"
            logger.error(response)
            if show_details:
                print(response)
            warnings.warn(response)
            return []

        response = f"Found {len(rinex_entries)} Rinex Files to Process"
        logger.info(response)
        if show_details:
            print(response)

        process_rinex_partial = partial(
            gnss_ops.rinex_to_kin,
            writedir=self.inter_dir,
            pridedir=self.pride_dir,
            show_details=show_details
            
        )
        kin_entries = []
        resfile_entries = []
        count = 0
        uploadCount = 0
        with multiprocessing.Pool() as pool:
            results = pool.imap(process_rinex_partial, rinex_entries)
            for (kinfile,resfile) in tqdm(results, total=len(rinex_entries), desc="Processing Rinex Files"):
                if kinfile is not None:
                    count += 1
                    if self.catalog.add_entry(kinfile):
                        uploadCount += 1
                    kin_entries.append(kinfile)
                    if resfile is not None:
                        count += 1
                        if self.catalog.add_entry(resfile):
                            uploadCount += 1
                        resfile_entries.append(resfile)
                if (gnss_df := gnss_ops.kin_to_gnssdf(kinfile)) is not None and resfile is not None:
                    wrms = gnss_ops.get_wrms_from_res(str(resfile.local_path))
                    gnss_df = pd.merge(gnss_df,wrms,left_on="time",right_on="time")

                    response = f'Adding GNSS Data of shape {gnss_df.shape} and daterange {gnss_df["time"].min().isoformat()} to {gnss_df["time"].max().isoformat()} to GNSS TDB'
                    logger.info(response)
                    if show_details:
                        print(response)
                    self.gnss_tdb.write_df(gnss_df)
                

        response = f"Generated {count} Kin Files From {len(rinex_entries)} Rinex Files, Added {uploadCount} to the Catalog"
        logger.info(response)
        if show_details:
            print(response)
    
    def process_dfop00(self,override:bool=False,show_details:bool=False) -> None:
        dfop00_entries: List[AssetEntry] = self.catalog.get_single_entries_to_process(
            network=self.network,station=self.station,survey=self.survey,parent_type=AssetType.DFOP00,override=override
        )
        if not dfop00_entries:
            response = f"No DFOP00 Files Found to Process for {self.network} {self.station} {self.survey}"
            logger.error(response)
            if show_details:
                print(response)
            warnings.warn(response)
            return

        response = f"Found {len(dfop00_entries)} DFOP00 Files to Process"
        logger.info(response)
        if show_details:
            print(response)

       
        count = 0

        with multiprocessing.Pool() as pool:
            results = pool.imap(sv3_ops.dev_dfop00_to_shotdata, dfop00_entries)
            for shotdata_df in tqdm(results, total=len(dfop00_entries), desc="Processing DFOP00 Files"):
                if shotdata_df is not None:
                    self.shotdata_tdb.write_df(shotdata_df)
                    count += 1
                    
        response = f"Generated {count} ShotData dataframes From {len(dfop00_entries)} DFOP00 Files"
        logger.info(response)
        if show_details:
            print(response)
        

    def update_shotdata(self,plot:bool=False):
        print("Updating shotdata with interpolated gnss data")
        # TODO Need to only update positions for a single shot and not each transponder
        # For each shotdata multiasset entry, update the shotdata position with gnss data
        try:
            merge_signature,dates = get_merge_signature(self.shotdata_tdb,self.gnss_tdb)
        except Exception as e:
            print(e)
            return
        merge_job = {
            "parent_type": AssetType.GNSS.value,
            "child_type": AssetType.SHOTDATA.value,
            "parent_ids": merge_signature
        }
        if not self.catalog.is_merge_complete(**merge_job):
            merge_shotdata_gnss(
                shotdata=self.shotdata_tdb,
                gnss=self.gnss_tdb,
                dates=dates,
                plot=plot
            )
        self.catalog.add_merge_job(**merge_job)


    def pipeline_sv2(self,override:bool=False,show_details:bool=False):
        self._process_data_graph(AssetType.POSITION,override=override,show_details=show_details)
        self._process_data_graph(AssetType.ACOUSTIC,override=override,show_details=show_details)
        self._process_data_graph(AssetType.RINEX,override=override,show_details=show_details)
        position_ma_list: List[MultiAssetEntry] = self.dev_group_session_data(source=AssetType.POSITION,override=override)
        acoustic_ma_list: List[MultiAssetEntry] = self.dev_group_session_data(source=AssetType.ACOUSTIC,override=override)
        rinex_ma_list: List[MultiAssetEntry] = self.dev_group_session_data(source=AssetType.RINEX,override=override)
        shot_ma_list: List[MultiAssetEntry] = sv2_ops.multiasset_to_shotdata(
            acoustic_assets=acoustic_ma_list,position_assets=position_ma_list,working_dir=self.proc_dir)
        processed_rinex_kin:Tuple[List[AssetEntry | MultiAssetEntry],List[AssetEntry | MultiAssetEntry]] = self._process_data_link(
            target=AssetType.KIN,source=AssetType.RINEX,override=override,parent_entries=rinex_ma_list,show_details=show_details)
        processed_kin_gnss: Tuple[List[AssetEntry | MultiAssetEntry],List[AssetEntry | MultiAssetEntry]] = self._process_data_link(
            target=AssetType.GNSS,source=AssetType.KIN,override=override,parent_entries=processed_rinex_kin[1],show_details=show_details)

    def pipeline_sv3(self,override:bool=False,show_details:bool=False,plot:bool=False):
        self.process_novatel(override=override,show_details=show_details)
        self.process_rinex(override=override,show_details=show_details)
        self.process_dfop00(override=override,show_details=show_details)
        self.update_shotdata(plot=plot)
