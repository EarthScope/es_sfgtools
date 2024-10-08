import os
from pathlib import Path
from typing import List,Callable,Union,Generator,Tuple,LiteralString,Optional
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
import logging
import multiprocessing
import threading

warnings.filterwarnings("ignore")
seaborn.set_theme(style="whitegrid")
from es_sfgtools.utils.archive_pull import download_file_from_archive
from es_sfgtools.processing.assets import AssetEntry,AssetType,MultiAssetEntry
from es_sfgtools.processing.operations import sv2_ops,sv3_ops,gnss_ops,site_ops
from es_sfgtools.processing.assets import observables,siteconfig,constants,file_schemas
from es_sfgtools.modeling.garpos_tools import schemas as modeling_schemas
from es_sfgtools.modeling.garpos_tools import functions as modeling_funcs
from es_sfgtools.modeling.garpos_tools import hyper_params

import sqlalchemy as sa
from .database import Base,Assets,MultiAssets,ModelResults
from .constants import FILE_TYPE,DATA_TYPE,REMOTE_TYPE,ALIAS_MAP,FILE_TYPES
from .datadiscovery import scrape_directory_local,get_file_type_local,get_file_type_remote
from .data_ops import create_multi_asset_dataframe,create_multi_asset_rinex
logger = logging.getLogger(__name__)

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
        self.working_dir.mkdir(parents=True,exist_ok=True)
        self.pride_dir.mkdir(parents=True,exist_ok=True)
        self.inter_dir.mkdir(exist_ok=True)
        self.proc_dir.mkdir(exist_ok=True)
        self.raw_dir.mkdir(exist_ok=True)
        self.garpos_dir.mkdir(exist_ok=True)

        self.db_path = self.data_dir/"catalog.sqlite"
        if not self.db_path.exists():
            self.db_path.touch()

        self.engine = sa.create_engine(f"sqlite+pysqlite:///{self.db_path}",poolclass=sa.pool.NullPool)
        Base.metadata.create_all(self.engine)

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

        with self.engine.begin() as conn:
            data_type_counts = [dict(row._mapping) for row in conn.execute(
                sa.select(sa.func.count(Assets.type),Assets.type).where(
                    Assets.network.in_([self.network]),Assets.station.in_([self.station]),Assets.survey.in_([self.survey]),Assets.local_path.is_not(None)
                    ).group_by(Assets.type)
                ).fetchall()]
            if len(data_type_counts) == 0:
                return {"Local files found":0}
        return {x["type"]:x["count_1"] for x in data_type_counts}    

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
        with self.engine.begin() as conn:
            # get local file paths under the same network, station, survey
            file_data_map = {x['local_path']:x for x in file_data_list}
            existing_files = [row[0] for row in conn.execute(sa.select(Assets.local_path).where(
                Assets.network.in_([self.network]),Assets.station.in_([self.station]),Assets.survey.in_([self.survey])
            )).fetchall()]            
            # remove existing files from the file_data_map
            while existing_files:
                file = existing_files.pop()
                file_data_map.pop(file,None)

            if len(file_data_map) == 0:
                response = f"No new files to add"
                logger.info(response)
                if show_details:
                    print(response)
                return
            response = f"Adding {len(file_data_map)} new files to the catalog"
            logger.info(response)
            if show_details:
                print(response)
            # now add the new files
            conn.execute(
                sa.insert(Assets).values(list(file_data_map.values()))
            )

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
                        local_filepaths:List[Union[str,Path]],
                        show_details:bool=True,
                        **kwargs):
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

            file_data = discovered_file.model_dump() | {
                "network": self.network,
                "station": self.station,
                "survey": self.survey,
                "timestamp_created": datetime.datetime.now(),
            }
            file_data_list.append(file_data)

        # See if the data is already in the catalog
        file_data_map = {x['remote_path']:x for x in file_data_list}
        response = f"Total remote files found {len(file_data_map)}"
        logger.info(response)
        if show_details:
            print(response)
        with self.engine.begin() as conn:
            existing_files = [dict(row._mapping) for row in conn.execute(sa.select(Assets.remote_path).where(
                Assets.network.in_([self.network]),Assets.station.in_([self.station]),Assets.survey.in_([self.survey])
            )).fetchall()]
            response = f"Total files tracked in catalog {len(existing_files)}"
            logger.info(response)
            if show_details:
                print(response)
            # remove existing files from the file_data_map
            for file in existing_files:
                file_data_map.pop(file['remote_path'],None)

            if len(file_data_map) == 0:
                response = f"No new files found to add"
                logger.info(response)
                print(response)
                return
            response = f"Adding {len(file_data_map)} new files to the catalog"
            logger.info(response)
            print(response)
            # now add the new files
            conn.execute(
                sa.insert(Assets).values(list(file_data_map.values()))
            )

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

    # def clear_raw_processed_data(self, network: str, station: str, survey: str):
    # """
    # Clear all raw data in type FILE_TYPE from the working directory.

    # Args:
    #     network (str): The network name.
    #     station (str): The station name.
    #     survey (str): The survey name.

    # Returns:
    #     None
    # """

    # # Get raw data into stack
    # parent_entries = self.catalog_data[
    #     (self.catalog_data.network == network)
    #     & (self.catalog_data.station == station)
    #     & (self.catalog_data.survey == survey)
    #     & (self.catalog_data.type.isin(FILE_TYPES))
    #     & (~self.catalog_data.local_path.isna())
    # ].to_dict(orient="records")

    # pbar = tqdm.tqdm(total=len(parent_entries), desc="Removing Raw Files")
    # while parent_entries:
    #     entry = parent_entries.pop()

    #     entry_type = FILE_TYPE(entry["type"])

    #     is_parent_processed = True
    #     child_types = [x.value for x in list(TARGET_MAP.get(entry_type).keys())]

    #     child_entries = self.catalog_data[
    #         (self.catalog_data.source_uuid == entry["uuid"])
    #         & (self.catalog_data.type.isin(child_types))
    #     ]

    #     # Check if we can find all child types, if not then skip
    #     if not entry["processed"]:
    #         pbar.update(1)
    #         continue

    #     # Now we check if the children files exist
    #     for _, child in child_entries.iterrows():
    #         if child["type"] in DATA_TYPES:
    #             self.catalog_data.loc[self.catalog_data.uuid == child["uuid"], "processed"] = True
    #             child["processed"] = True
    #         is_parent_processed &= child["processed"]
    #         try:
    #             # If the child is a FILE_TYPE, then we will add it to the stack (only applies to the rinex and kin files)
    #             # We check this in the try/except block by attempting to instantiate a FILE_TYPE
    #             _ = FILE_TYPE(child["type"])
    #             parent_entries.append(dict(child))
    #             pbar.update(1)
    #         except ValueError:
    #             # The child is a DATA_TYPE, so we can pass
    #             pass

    #     # If all children files exist, is_parent_processed == True, and we can delete the parent file
    #     if is_parent_processed and not pd.isna(entry["local_path"]):
    #         Path(entry["local_path"]).unlink()
    #         self.catalog_data.loc[(self.catalog_data.uuid == entry["uuid"]), "processed"] = True
    #         self.catalog_data.loc[self.catalog_data.uuid == entry["uuid"], "local_path"] = None

    #         response = f"Removed Raw File {entry['uuid']} of Type {entry['type']} From {entry['local_path']} "
    #         logger.info(response)

    #     self.catalog_data.to_csv(self.catalog, index=False)

    # pbar.close()

    def add_entry(self, entry: dict):
        """
        Add an entry in the catalog.  This may result in duplicates, which need to be cleaned up via
        consolidate_entries()

        Args:
            entry (dict): The new entry.

        Returns:
            None
        """
        with self.engine.begin() as conn:
            conn.execute(sa.insert(Assets).values(entry))

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
        self, parent_type: FILE_TYPE) -> List[Union[FILE_TYPE, DATA_TYPE]]:

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
        process_func = TARGET_MAP.get(parent.type).get(child_type)
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
                processed = AssetEntry(
                    local_path=local_path,
                    type=child_type,
                    parent_id=parent.id,
                    timestamp_data_start=timestamp_data_start,
                    timestamp_data_end=timestamp_data_end,
                    timestamp_created=datetime.datetime.now(),
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

        assert local_path.exists(), f"Local path {local_path} does not exist"

        return processed, parent, response

    def _update_parent_child_catalog(self,
                              parent_data:AssetEntry|MultiAssetEntry,
                              child_data:AssetEntry|MultiAssetEntry):
        
        table = Assets if isinstance(parent_data,AssetEntry) else MultiAssets
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(table=table)
                .where(table.id.is_(parent_data.id))
                .values(parent_data.model_dump())
            )
            found = conn.execute(
                sa.select(table).where(
                    table.local_path.is_(str(child_data.local_path)),
                )
            ).fetchall()
            if found:
                child_data.parent_id = parent_data.id
                child_data.id = found[0].id
                if child_data.timestamp_data_start is None:
                    child_data.timestamp_data_start = found[0].timestamp_data_start
                    child.timestamp_data_end = found[0].timestamp_data_end  
                    
                conn.execute(
                    sa.delete(table=table).where(
                        table.local_path.in_([x.local_path for x in found])
                    ))
                conn.execute(
                    sa.insert(table).values(child_data.model_dump())
                )
            else:
                conn.execute(sa.insert(table).values([child_data.model_dump()]))
            conn.commit()

    def _get_entries_to_process(self,parent_type:AssetType,child_type:AssetType,override:bool=False) -> List[AssetEntry]:
        with self.engine.begin() as conn:
            parent_entries = conn.execute(
                sa.select(Assets).where(
                    Assets.network.is_(self.network),Assets.station.is_(self.station),Assets.survey.is_(self.survey),
                    Assets.local_path.isnot(None),Assets.type.is_(parent_type.value)
                )
            ).fetchall()
            if not parent_entries:
                logger.error(f"No entries of type {parent_type.value} found in catalog for {self.network} {self.station} {self.survey}")
                return []
            # Create a map of parent entries for easy lookup
            parent_entries_map = {x.id: x for x in parent_entries}

            # Fetch child entries matching the parent entries
            parent_id = list(parent_entries_map.keys())
            child_entries = conn.execute(
                sa.select(Assets).where(
                    Assets.network == self.network,
                    Assets.station == self.station,
                    Assets.survey == self.survey,
                    Assets.type == child_type.value,
                    Assets.parent_id.in_(parent_id),
                )
            ).fetchall()

            # If not overriding, remove processed parent entries
            if not override:
                while child_entries:
                    child = child_entries.pop()
                    parent_entries_map.pop(int(child.parent_id), None)

            return [AssetEntry(**dict(row._mapping)) for row in parent_entries_map.values()]

    def  _process_data_link(self,
                           target:AssetType,
                           source:AssetType,
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
            parent_entries = self._get_entries_to_process(parent_type=source,child_type=target,override=override)
        if parent_entries is None:
            return [],[]
        process_func_partial = partial(self._process_targeted,child_type=target,inter_dir=self.inter_dir,proc_dir=self.proc_dir,pride_dir=self.pride_dir)
        parent_data_list = []
        child_data_list = []
        with multiprocessing.Pool() as pool:
            results = pool.imap(process_func_partial,parent_entries)
            for child_data,parent_data,response in tqdm(results,total=len(parent_entries),desc=f"Processing {source.value} To {target.value}"):
                if child_data is None:
                    logger.error(response)
                    continue
                self._update_parent_child_catalog(parent_data,child_data)
                parent_data_list.append(parent_data)
                child_data_list.append(child_data)
                logger.info(response)
                if show_details:
                    print(response)
            response = f"Processed {len(child_data_list)} Out of {len(parent_entries)} For {target.value}"
            logger.info(response)
            return parent_data_list,child_data_list

    def _process_data_graph(self, 
                            child_type:AssetType,
                            override:bool=False,
                            show_details:bool=False):

        processing_queue = self.get_parent_stack(child_type=child_type)
        if show_details:
            print(f"processing queue: {[item.value for item in processing_queue]}")
        while processing_queue:
            parent = processing_queue.pop(0)
            if parent != child_type:

                children:dict = TARGET_MAP.get(parent,{})

                children_to_process = [k for k in children.keys() if k in processing_queue]
                if show_details:
                    print(f"parent: {parent.value}")
                    print(f"children to process: {[item.value for item in children_to_process]}")
                for child in children_to_process:
                    if show_details:
                        print(f"processing child:{child.value}")
                    processed_parents,processed_children = self._process_data_link(
                        target=child,
                        source=parent,
                        override=override,
                        show_details=show_details)
                    # Check if all children of this parent have been processed

                    # TODO check if all children of this parent have been processed
                
                
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

                proc_parents,proc_children = self._process_data_link(target=child,source=parent_type,override=override,show_details=show_details)
                child_targets = TARGET_MAP.get(child,{})
                if child_targets:
                    processing_queue.append({child:child_targets})

    def process_gnss_data(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(AssetType.GNSS,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_metadata(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(AssetType.SITECONFIG,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self._process_data_graph(AssetType.ATDOFFSET,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self._process_data_graph(AssetType.SVP,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_qc_data(self, override:bool=False, show_details:bool=False):
        self._process_data_graph_forward(AssetType.QCPIN,override=override, show_details=show_details)

    def process_sv3_data(self, override:bool=False, show_details:bool=False):
        self._process_data_graph_forward(AssetType.DFOP00,override=override, show_details=show_details,)

    def dev_group_session_data(self,
                           source:Union[str,AssetType] = AssetType.SHOTDATA,
                           override:bool=False
                           ) -> Union[List[MultiAssetEntry],None]:
        """
        Group the session data by timestamp.

        Args:
            timespan (str): The timespan to group the data by.
            show_details (bool): Log verbose output.

        Returns:
            dict: The grouped data.
        """ 
        if isinstance(source,str):
            try:
                source = AssetType(source)
            except:
                raise ValueError(f"Source {source} must be one of {AssetType.__members__.keys()}")
            
        match source:
            case AssetType.POSITION | AssetType.SHOTDATA | AssetType.ACOUSTIC | AssetType.GNSS:
                multi_asset_list: List[MultiAssetEntry] = create_multi_asset_dataframe(
                assetType=source,
                writedir=self.proc_dir,
                network=self.network,
                station=self.station,
                survey=self.survey,
                override=override,
                engine=self.engine,
            )
            case AssetType.RINEX:
                multi_asset_list:List[MultiAssetEntry] = create_multi_asset_rinex(
                engine=self.engine,
                network=self.network,
                station=self.station,
                survey=self.survey,
                working_dir=self.inter_dir,
                ovveride=override
                )

        logger.info(f"Created {len(multi_asset_list)} MultiAssetEntries for {source.value}")
       
        return multi_asset_list
    
    def query_catalog(self,
                      query:str) -> pd.DataFrame:
        with self.engine.begin() as conn:
            try:
                return pd.read_sql_query(query,conn)
            except sa.exc.ResourceClosedError:
                # handle queries that don't return results
                conn.execute(sa.text(query))

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

    def pipeline_sv3(self,override:bool=False,show_details:bool=False):
        self._process_data_graph_forward(AssetType.DFOP00,override=override,show_details=show_details)
        self._process_data_graph(AssetType.RINEX,override=override,show_details=show_details)
        shotdata_ma_list: List[MultiAssetEntry] = self.dev_group_session_data(source=AssetType.SHOTDATA,override=override)
        print(shotdata_ma_list)
        rinex_ma_list: List[MultiAssetEntry] = self.dev_group_session_data(source=AssetType.RINEX,override=override)
        print(rinex_ma_list)
        processed_rinex_kin:Tuple[List[AssetEntry | MultiAssetEntry],List[AssetEntry | MultiAssetEntry]] = self._process_data_link(
            target=AssetType.KIN,source=AssetType.RINEX,override=override,parent_entries=rinex_ma_list,show_details=show_details)
        print(processed_rinex_kin)
        processed_kin_gnss: Tuple[List[AssetEntry | MultiAssetEntry],List[AssetEntry | MultiAssetEntry]] = self._process_data_link(
            target=AssetType.GNSS,source=AssetType.KIN,override=override,parent_entries=processed_rinex_kin[1],show_details=show_details)
        print(processed_kin_gnss)
    # def run_session_data(self,
    #                      siteConfig:siteconfig.SiteConfig,
    #                      soundVelocity:siteconfig.SoundVelocity,
    #                      atdOffset:siteconfig.ATDOffset,
    #                      source_type:str=AssetType.DFPO00.value,
    #                      date_range:List[datetime.date]=[]):

    # def get_observation_session_data(self,network:str,station:str,survey:str,plot:bool=False) -> pd.DataFrame:

    #     time_groups = self.catalog_data[self.catalog_data.type.isin(['gnss','acoustic','imu'])].groupby('timestamp')
    #     valid_groups = [group for name, group in time_groups] #
    #     result = pd.concat(valid_groups)
    #     primary_colums = ['network','station','survey','timestamp','type','local_path']
    #     all_columns = list(result.columns)
    #     for column in primary_colums[::-1]:
    #         all_columns.remove(column)
    #         all_columns.insert(0,column)

    #     result = result[all_columns]
    #     result = DataCatalog.validate(result)
    #     times = result.timestamp.unique()
    #     result.set_index(["network", "station", "survey", "timestamp"], inplace=True)
    #     result.sort_index(inplace=True)

    #     if plot:
    #         fig, ax = plt.subplots(figsize=(16, 2))
    #         ax.set_title(f"Observable Data Availability For Network: {network} Station: {station} Survey: {survey}")

    #         ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    #         ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    #         for time in times:
    #             ax.scatter(pd.to_datetime(time), 1,marker='o',color='green')

    #         plt.xticks(rotation=45)
    #         ax.set_xlabel("Timestamp")
    #         ax.set_yticks([])

    #         plt.show()

    #     return result

    # def group_observation_session_data(self,data:pd.DataFrame,timespan:str="DAY") -> dict:
    #     # Create a group of dataframes for each timestamp
    #     assert timespan in ['HOUR','DAY'], "Timespan must be either 'HOUR' or 'DAY'"
    #     if timespan == 'HOUR':
    #         grouper = pd.Grouper(key="timestamp", freq="h")
    #     else:
    #         grouper = pd.Grouper(key="timestamp", freq="D")
    #     out = {}

    #     obs_types = [AssetType.IMU.value, AssetType.GNSS.value, AssetType.ACOUSTIC.value]
    #     for timestamp, group in data.groupby(grouper):
    #         if group.shape[0] < 1:
    #             continue
    #         out[timestamp] = {}
    #         for obs_type in obs_types:
    #             out[timestamp][obs_type] = list(group[group.type == obs_type].local_path.values)

    #     # prune empty entries
    #     out = {str(k):v for k,v in out.items() if any([len(x) > 0 for x in v.values()])}
    #     return out

    # def plot_campaign_data(self,network:str,station:str,survey:str):
    #     """
    #     Plot the timestamps and data type for processed IMU,GNSS,and Acoustic data for a given network, station, and survey.

    #     Args:
    #         network (str): The network name.
    #         station (str): The station name.
    #         survey (str): The survey name.

    #     Raises:
    #         Exception: If no matching data is found in the catalog.
    #     """

    #     data_type_to_plot = [AssetType.IMU.value,AssetType.GNSS.value,AssetType.ACOUSTIC.value]

    #     entries = self.catalog_data[
    #         (self.catalog_data.network == network)
    #         & (self.catalog_data.station == station)
    #         & (self.catalog_data.survey == survey)
    #         & (self.catalog_data.type.isin(data_type_to_plot))
    #     ]

    #     if entries.shape[0] < 1:
    #         raise Exception('No matching data found in catalog')

    #     # plot the timestamps and data type for processed IMU,GNSS,and Acoustic data
    #     cmap = {
    #         AssetType.IMU.value: "blue",
    #         AssetType.GNSS.value: "green",
    #         AssetType.ACOUSTIC.value: "red",
    #     }

    #     fig, axes = plt.subplots(3, 1, figsize=(10, 4), sharex=True)

    #     fig.suptitle(f"Observable Data Availablility For Network: {network} Station: {station} Survey: {survey}")
    #     # Set the x-axis to display dates
    #     for ax in axes:
    #         ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    #         ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    #     data_type_titles = {
    #         AssetType.IMU.value: "IMU Data",
    #         AssetType.GNSS.value: "GNSS Data",
    #         AssetType.ACOUSTIC.value: "Acoustic Data",
    #     }

    #     for i, data_type in enumerate(data_type_to_plot):
    #         data = entries[entries.type == data_type]
    #         for timestamp in data.timestamp:
    #             axes[i].axvline(pd.to_datetime(timestamp), color=cmap[data_type], linestyle="-")
    #         axes[i].set_title(data_type_titles[data_type])
    #         axes[i].get_yaxis().set_visible(False)  # Hide y-axis values

    #     # Add x-axis label to the bottom subplot
    #     axes[-1].set_xlabel("Timestamp")

    #     plt.tight_layout()
    #     plt.show()

    # def get_site_config(self,network:str,station:str,survey:str) -> siteconfig.SiteConfig:
    #     """
    #     Get the Site Config data for a given network, station, and survey.

    #     Args:
    #         network (str): The network name.
    #         station (str): The station name.
    #         survey (str): The survey name.

    #     Raises:
    #         Exception: If no matching data is found in the catalog.
    #     """

    #     data_type_to_plot = [AssetType.SITECONFIG.value]

    #     entries = self.catalog_data[
    #         (self.catalog_data.network == network)
    #         & (self.catalog_data.station == station)
    #         & (self.catalog_data.survey == survey)
    #         & (self.catalog_data.type.isin(data_type_to_plot))
    #     ]

    #     if entries.shape[0] < 1:
    #         raise Exception('No matching site config data found in catalog')

    #     # load the site config data
    #     path = entries.local_path.values[0]
    #     with open(path, "r") as f:
    #         site_config = json.load(f)
    #         site_config_schema = SCHEMA_MAP[AssetType.SITECONFIG](**site_config)
    #     return site_config_schema

    # def get_svp_data(self,network:str,station:str,survey:str) -> pd.DataFrame:
    #     """
    #     Get the Sound Velocity Profile data for a given network, station, and survey.

    #     Args:
    #         network (str): The network name.
    #         station (str): The station name.
    #         survey (str): The survey name.

    #     Raises:
    #         Exception: If no matching data is found in the catalog.
    #     """

    #     entries = self.catalog_data[
    #         (self.catalog_data.network == network)
    #         & (self.catalog_data.station == station)
    #         & (self.catalog_data.survey == survey)
    #         & (self.catalog_data.type==AssetType.SVP.value)
    #     ]

    #     if entries.shape[0] < 1:
    #         raise Exception('No matching SVP data found in catalog')

    #     # load the SVP data
    #     path = entries.local_path.values[0]
    #     svp_data = pd.read_csv(path)
    #     return svp_data

    # def get_atd_offset(self,network:str,station:str,survey:str) -> siteconfig.ATDOffset:
    #     """
    #     Get the ATD Offset data for a given network, station, and survey.

    #     Args:
    #         network (str): The network name.
    #         station (str): The station name.
    #         survey (str): The survey name.

    #     Raises:
    #         Exception: If no matching data is found in the catalog.
    #     """

    #     entries = self.catalog_data[
    #         (self.catalog_data.network == network)
    #         & (self.catalog_data.station == station)
    #         & (self.catalog_data.survey == survey)
    #         & (self.catalog_data.type == AssetType.ATDOFFSET.value)
    #     ]

    #     if entries.shape[0] < 1:
    #         raise Exception('No matching ATD Offset data found in catalog')

    #     # load the ATD Offset data
    #     path = entries.local_path.values[0]
    #     with open(path, "r") as f:
    #         atd_offset = json.load(f)
    #         atd_offset_schema = SCHEMA_MAP[AssetType.ATDOFFSET](**atd_offset)
    #     return atd_offset_schema

    # def plot_site_config(self,site_config:siteconfig.SiteConfig,zoom:int=5):
    #     """
    #     Plot the timestamps and data type for processed Site Config data for a given network, station, and survey.

    #     """

    #     map = folium.Map(location=[site_config.position_llh.latitude, site_config.position_llh.longitude], zoom_start=zoom)
    #     folium.Marker(
    #         location=[site_config.position_llh.latitude, site_config.position_llh.longitude],
    #         icon=folium.Icon(color="blue"),
    #     ).add_to(map)

    #     for transponder in site_config.transponders:
    #         folium.Marker(
    #             location=[transponder.position_llh.latitude, transponder.position_llh.longitude],
    #             popup=f"Transponder: {transponder.id}",
    #             icon=folium.Icon(color="red"),
    #         ).add_to(map)

    #     # map.save(self.working_dir/f"site_config_{network}_{station}_{survey}.html")
    #     return map
