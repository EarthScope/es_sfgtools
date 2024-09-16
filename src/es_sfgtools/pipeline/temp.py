import os
from pathlib import Path
from typing import List,Callable,Union,Generator,Tuple
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
import  folium
import json
import concurrent.futures
import logging
import multiprocessing
import threading

warnings.filterwarnings("ignore")
seaborn.set_theme(style="whitegrid")
from es_sfgtools.utils.archive_pull import download_file_from_archive
from es_sfgtools.processing import functions as proc_funcs
from es_sfgtools.processing import schemas as proc_schemas

import sqlalchemy as sa
from .database import Base,Assets

logger = logging.getLogger(__name__)

class REMOTE_TYPE(Enum):
    S3 = "s3"
    HTTP = "http"

class FILE_TYPE(Enum):
    SONARDYNE = "sonardyne"
    NOVATEL = "novatel"
    KIN = "kin"
    RINEX = "rinex"
    MASTER= "master"    
    LEVERARM = "leverarm"
    SEABIRD = "svpavg"
    NOVATEL770 = "novatel770"
    DFPO00 = "dfop00"
    OFFLOAD = "offload"
    QCPIN = "pin"
    NOVATELPIN = "novatelpin"
    

    @classmethod
    def to_schema(cls):
        return [x.name for x in cls]


FILE_TYPES = [x.value for x in FILE_TYPE]
ALIAS_MAP = {
    "nov770":"novatel770"}
ALIAS_MAP = ALIAS_MAP | {x:x for x in FILE_TYPES}

class DATA_TYPE(Enum):
    IMU = "imu"
    GNSS = "gnss"
    ACOUSTIC = "acoustic"
    SITECONFIG = "siteconfig"
    ATDOFFSET = "atdoffset"
    SVP = "svp"
    SHOTDATA = "shotdata"

    @classmethod
    def to_schema(cls):
        return [x.name for x in cls]


DATA_TYPES = [x.value for x in DATA_TYPE]

TARGET_MAP = {
    FILE_TYPE.QCPIN:{DATA_TYPE.SHOTDATA:proc_funcs.dev_qcpin_to_shotdata},
    FILE_TYPE.NOVATELPIN:{FILE_TYPE.RINEX:proc_funcs.novatel_to_rinex},
    FILE_TYPE.NOVATEL:{FILE_TYPE.RINEX:proc_funcs.novatel_to_rinex, DATA_TYPE.IMU:proc_funcs.novatel_to_imudf},
    FILE_TYPE.RINEX:{FILE_TYPE.KIN:proc_funcs.rinex_to_kin},
    FILE_TYPE.KIN:{DATA_TYPE.GNSS:proc_funcs.kin_to_gnssdf},
    FILE_TYPE.SONARDYNE:{DATA_TYPE.ACOUSTIC:proc_funcs.sonardyne_to_acousticdf},
    FILE_TYPE.MASTER:{DATA_TYPE.SITECONFIG:proc_funcs.masterfile_to_siteconfig},
    FILE_TYPE.LEVERARM:{DATA_TYPE.ATDOFFSET:proc_funcs.leverarmfile_to_atdoffset},
    FILE_TYPE.SEABIRD:{DATA_TYPE.SVP:proc_funcs.seabird_to_soundvelocity},
    FILE_TYPE.NOVATEL770:{FILE_TYPE.RINEX:proc_funcs.novatel_to_rinex},
    #FILE_TYPE.DFPO00:{DATA_TYPE.IMU:proc_funcs.dfpo00_to_imudf, DATA_TYPE.ACOUSTIC:proc_funcs.dfpo00_to_acousticdf}
    FILE_TYPE.DFPO00:{DATA_TYPE.SHOTDATA:proc_funcs.dev_dfop00_to_shotdata}
}


# Reverse the target map so we can get the parent type from the child type keys
# Format should be {child_type:[parent_type_0,parent_type_1,..]}
SOURCE_MAP = {}
for parent,children in TARGET_MAP.items():
    for child in children.keys():
        if not SOURCE_MAP.get(child,[]):
            SOURCE_MAP[child] = []
        SOURCE_MAP[child].append(parent)

SCHEMA_MAP = {
    FILE_TYPE.NOVATEL:proc_schemas.NovatelFile,
    FILE_TYPE.SONARDYNE:proc_schemas.SonardyneFile,
    FILE_TYPE.RINEX:proc_schemas.RinexFile,
    FILE_TYPE.KIN:proc_schemas.KinFile,
    FILE_TYPE.MASTER:proc_schemas.MasterFile,
    FILE_TYPE.LEVERARM:proc_schemas.LeverArmFile,
    FILE_TYPE.SEABIRD:proc_schemas.SeaBirdFile,
    FILE_TYPE.NOVATEL770:proc_schemas.Novatel770File,
    FILE_TYPE.DFPO00:proc_schemas.DFPO00RawFile,
    DATA_TYPE.IMU:proc_schemas.IMUDataFrame,
    DATA_TYPE.GNSS:proc_schemas.PositionDataFrame,
    DATA_TYPE.ACOUSTIC:proc_schemas.AcousticDataFrame,
    DATA_TYPE.SITECONFIG:proc_schemas.SiteConfig,
    DATA_TYPE.ATDOFFSET:proc_schemas.ATDOffset,
    FILE_TYPE.QCPIN:proc_schemas.QCPinFile,
    FILE_TYPE.NOVATELPIN:proc_schemas.NovatelPinFile
}

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

    def add_data_local(self,
                        local_filepaths:List[str],
                        discover_file_type:bool=False,
                        show_details:bool=True,
                        **kwargs):
        count = 0
        file_data_list = []
        for file in local_filepaths:
            assert Path(file).exists(), f"File {file} does not exist"
            if discover_file_type:
                discovered_file_type = None
                file_path_proc = file.replace("_", "").lower()

                for alias, file_type in ALIAS_MAP.items():
                    if alias in file_path_proc:
                        discovered_file_type = file_type
                        break
            else:
                discovered_file_type = FILE_TYPE.QCPIN.value

            if discovered_file_type is None:
                logger.error(f"File type not recognized for {file}")
                warnings.warn(f"File type not recognized for {file}", UserWarning)
                continue
            
            
            file_data = {
                "network": self.network,
                "station": self.station,
                "survey": self.survey,
                "local_path": str(file),
                "type": discovered_file_type,
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
                if bool(os.environ.get("DH_SHOW_DETAILS",False)):
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
            discovered_file_type = None
            file_path_proc = file.replace("_", "").lower()

            for alias, file_type in ALIAS_MAP.items():
                if alias in file_path_proc:
                    discovered_file_type = file_type
                    break

            if discovered_file_type is None:
                logger.error(f"File type not recognized for {file}")
                continue
                # raise ValueError(f"File type not recognized for {file}")

            file_data = {
                "network": self.network,
                "station": self.station,
                "survey": self.survey,
                "remote_path": file,
                "remote_type": remote_type.value,
                "type": discovered_file_type,
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
        self, child_type: Union[FILE_TYPE, DATA_TYPE]
    ) -> List[Union[FILE_TYPE, DATA_TYPE]]:
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
            parents: List[Union[FILE_TYPE, DATA_TYPE]] = SOURCE_MAP.get(
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
    def _process_targeted(
        parent: dict,
        child_type: Union[FILE_TYPE, DATA_TYPE],
        inter_dir: Path,
        proc_dir: Path,
        pride_dir: Path,
        show_details: bool=False,
    ) -> Tuple[dict, dict, bool]:

        response = " "
        # TODO: implement multithreaded logging, had to switch to print statement below

        # handle the case when the parent timestamp is None
        child_timestamp = parent.get("timestamp_data_start", None)

        response += f"Processing {parent['local_path']} ({parent['id']}) of Type {parent['type']} to {child_type.value}\n"
        # Get the processing function that converts the parent entry to the child entry
        process_func = TARGET_MAP.get(FILE_TYPE(parent["type"])).get(child_type)
        # Build the source object from the parent entry
        source = SCHEMA_MAP[FILE_TYPE(parent["type"])](
            local_path=Path(parent["local_path"]),
            uuid=parent["id"],
            timestamp_data_start=parent["timestamp_data_start"],
        )
        # build partial processing function
        match process_func:
            case proc_funcs.rinex_to_kin:
                process_func_p = partial(
                    process_func,
                    writedir=inter_dir,
                    pridedir=pride_dir,
                    site=parent["station"],
                    show_details=show_details,
                )

            case proc_funcs.novatel_to_rinex:
                process_func_p = partial(
                    process_func,
                    site=parent["station"],
                    year=parent.get("timestamp_data_start", datetime.datetime.now().year),
                    show_details=show_details,
                )

            case proc_funcs.qcpin_to_novatelpin:
                process_func_p = partial(process_func, outpath=inter_dir)
            case _:
                process_func_p = process_func

        processed = None
        timestamp_data_start = parent.get("timestamp_data_start", None)
        timestamp_data_end = parent.get("timestamp_data_end", None)

        if hasattr(source,"local_path") is not None and source.local_path.exists():
            if source.local_path.stat().st_size == 0:
                response += f"File {source.local_path} is empty\n"
                processed = None
            else:
                processed = process_func_p(source)

        match type(processed):
            case pd.DataFrame:
                local_path = proc_dir / f"{parent['id']}_{child_type.value}.csv"
                processed.to_csv(local_path, index=False)

                # handle the case when the child timestamp is None
                if pd.isna(parent["timestamp_data_start"]):
                    for col in processed.columns:
                        if pd.api.types.is_datetime64_any_dtype(processed[col]):
                            timestamp_data_start = processed[col].min()
                            timestamp_data_end = processed[col].max()
                            break

            case proc_schemas.RinexFile:
                processed.write(inter_dir)
                local_path = processed.local_path
                timestamp_data_start = processed.timestamp_data_start
                timestamp_data_end = processed.timestamp_data_end

            case proc_schemas.KinFile:
                local_path = processed.local_path

            case proc_schemas.SiteConfig:
                local_path = proc_dir / f"{parent['id']}_{child_type.value}.json"
                with open(local_path, "w") as f:
                    f.write(processed.model_dump_json())

            case proc_schemas.ATDOffset:
                local_path = proc_dir / f"{parent['id']}_{child_type.value}.json"
                with open(local_path, "w") as f:
                    f.write(processed.model_dump_json())

            case proc_schemas.NovatelPinFile:
                local_path = inter_dir / f"{parent['id']}_{child_type.value}.txt"
                processed.local_path = local_path
                processed.write(dir=local_path.parent)

            case _:
                local_path = None
                pass

        if (
            pd.isna(parent.get("timestamp_data_start", None))
            and timestamp_data_start is not None
        ):
            parent["timestamp_data_start"] = timestamp_data_start
            parent["timestamp_data_end"] = timestamp_data_end
            response += f"  Discovered timestamp: {timestamp_data_start} for parent {parent['type']} uuid {parent['id']}\n"

        if local_path is not None and local_path.exists():
            local_path = str(local_path)
        else:
            local_path = None

        processed_meta = {
            "network": parent["network"],
            "station": parent["station"],
            "survey": parent["survey"],
            "local_path": local_path,
            "type": child_type.value,
            "timestamp_data_start": timestamp_data_start,
            "timestamp_data_end": timestamp_data_end,
            "parent_id": parent["id"],
        }
        if local_path is not None and Path(local_path).exists():
            response += f"  Successful Processing: {str(processed_meta)}\n"
        else:
            response += f"  Failed Processing: {str(processed_meta)}\n"

        return processed_meta, parent, response

    def _update_parent_child_catalog(self,
                              parent_data:dict,
                              child_data:dict):
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(Assets)
                .where(Assets.id.is_(parent_data["id"]))
                .values(parent_data)
            )
            found = conn.execute(
                sa.select(Assets).where(
                    Assets.parent_id.is_(parent_data["id"]),
                    Assets.type.is_(child_data["type"]),
                )
            ).fetchall()
            if found:
                conn.execute(
                    sa.update(Assets)
                    .where(
                        Assets.parent_id.is_(parent_data["id"]),
                        Assets.type.is_(child_data["type"]),
                    )
                    .values(child_data)
                )
            else:
                conn.execute(sa.insert(Assets).values([child_data]))
            conn.commit()

    def _process_data_link(self,
                           target:Union[FILE_TYPE,DATA_TYPE],
                           source:List[FILE_TYPE],
                           override:bool=False,
                           show_details:bool=False) -> pd.DataFrame:
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
        with self.engine.begin() as conn:
            parent_entries = conn.execute(
                sa.select(Assets).where(
                    Assets.network.is_(self.network),Assets.station.is_(self.station),Assets.survey.is_(self.survey),
                    Assets.local_path.isnot(None),Assets.type.in_([x.value for x in source]),Assets.local_path.isnot(None)
                )
            ).fetchall()

        if not parent_entries:
            response = f"No unprocessed data found in catalog for types {[x.value for x in source]}"
            logger.error(response)
            print(response)
            return

        # Filter out parent entries that have already been processed
        parent_entries_map = {x.id:x for x in parent_entries}
        with self.engine.begin() as conn:
            child_entries = conn.execute(
                sa.select(Assets).where(
                    Assets.network.is_(self.network),Assets.station.is_(self.station),Assets.survey.is_(self.survey),
                    Assets.type.is_(target.value),Assets.parent_id.in_([x.id for x in parent_entries])
                )
            ).fetchall()
        if not override:
            while child_entries:
                child = child_entries.pop()
                found = parent_entries_map.pop(int(child.parent_id),None)

        parent_entries_to_process = list(parent_entries_map.values())

        if parent_entries_to_process:
            response = f"Processing {len(parent_entries_to_process)} Parent Files to {target.value} Data"
            logger.info(response)
            print(response)
            process_func_partial = partial(self._process_targeted,child_type=target,inter_dir=self.inter_dir,proc_dir=self.proc_dir,pride_dir=self.pride_dir)

            child_data_list = []
            parent_data_list = []

            parent_entries_to_process = [dict(row._mapping) for row in parent_entries_to_process]
            with multiprocessing.Pool() as pool:
                results = pool.imap(process_func_partial,parent_entries_to_process)
                for child_data,parent_data,response in tqdm(results,total=len(parent_entries_to_process),desc=f"Processing {source[0].value} To {target.value}"):
                    if show_details:
                        print(response)
                        logger.info(response)
                    if child_data is None:
                        continue
                    parent_data_list.append(parent_data)
                    child_data_list.append(child_data)
                    self._update_parent_child_catalog(parent_data,child_data)
    

            response = f"Processed {len(child_data_list)} Out of {len(parent_entries_to_process)} For {target.value}"
            logger.info(response)
            print(response)

            return parent_data_list

    def _process_data_graph(self, 
                            child_type:Union[FILE_TYPE,DATA_TYPE],
                            override:bool=False,
                            show_details:bool=False,
                            update_timestamp:bool=False):

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
                    processed_parents:List[dict] = self._process_data_link(
                        target=child,
                        source=[parent],
                        override=override,
                        show_details=show_details)
                    # Check if all children of this parent have been processed

                    # TODO check if all children of this parent have been processed

    def _process_data_graph_forward(self, 
                            parent_type:FILE_TYPE,
                            override:bool=False,
                            show_details:bool=False):

        processing_queue = [{parent_type:TARGET_MAP.get(parent_type)}]
        while processing_queue:
            # process each level of the child graph
            parent_targets = processing_queue.pop(0)
            parent_type = list(parent_targets.keys())[0]
            for child in parent_targets[parent_type].keys():

                self._process_data_link(target=child,source=[parent_type],override=override,show_details=show_details)
                child_targets = TARGET_MAP.get(child,{})
                if child_targets:
                    processing_queue.append({child:child_targets})

    def process_acoustic_data(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(DATA_TYPE.ACOUSTIC,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_imu_data(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(DATA_TYPE.IMU,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_rinex(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(FILE_TYPE.RINEX,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_gnss_data_kin(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(FILE_TYPE.KIN,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_gnss_data(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(DATA_TYPE.GNSS,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_metadata(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(DATA_TYPE.SITECONFIG,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self._process_data_graph(DATA_TYPE.ATDOFFSET,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self._process_data_graph(DATA_TYPE.SVP,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_siteconfig(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(DATA_TYPE.SITECONFIG,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_atdoffset(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(DATA_TYPE.ATDOFFSET,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_svp(self, override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(DATA_TYPE.SVP,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_target(self,parent:str,child:str,override:bool=False,show_details:bool=False):
        target = DATA_TYPE(child)
        source = FILE_TYPE(parent)
        self._process_data_link(target=target,source=[source],override=override,show_details=show_details)

    def process_qc_data(self, override:bool=False, show_details:bool=False):
        self._process_data_graph_forward(FILE_TYPE.QCPIN,override=override, show_details=show_details)
    
    def process_sv3_data(self, override:bool=False, show_details:bool=False):
        self._process_data_graph_forward(FILE_TYPE.DFPO00,override=override, show_details=show_details,)

    def process_campaign_data(
        self, override: bool = False, show_details: bool=False,update_timestamp:bool=False
    ):
        """
        Process all data for a given network, station, and survey, generating child entries where applicable.

        Args:
            override (bool): Whether to override existing child entries. 
            show_details (bool): Log verbose output

        Raises:
            Exception: If no matching data is found in the catalog.
        """
        self.process_acoustic_data(override=override, show_details=show_details,update_timestamp=update_timestamp)
        self.process_imu_data(override=override, show_details=show_details,update_timestamp=update_timestamp)
        self.process_gnss_data(override=override, show_details=show_details,update_timestamp=update_timestamp)
        self.process_metadata(override=override, show_details=show_details,update_timestamp=update_timestamp)

        response = f"Network {self.network} Station {self.station} Survey {self.survey} Preprocessing complete"
        logger.info(response)
        print(response)

    def process_qc_data(self, override:bool=False, show_details:bool=False):
        # perform forward processing of qc pin data
        self._process_data_graph_forward(FILE_TYPE.QCPIN,override=override, show_details=show_details)

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

    #     obs_types = [DATA_TYPE.IMU.value, DATA_TYPE.GNSS.value, DATA_TYPE.ACOUSTIC.value]
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

    #     data_type_to_plot = [DATA_TYPE.IMU.value,DATA_TYPE.GNSS.value,DATA_TYPE.ACOUSTIC.value]

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
    #         DATA_TYPE.IMU.value: "blue",
    #         DATA_TYPE.GNSS.value: "green",
    #         DATA_TYPE.ACOUSTIC.value: "red",
    #     }

    #     fig, axes = plt.subplots(3, 1, figsize=(10, 4), sharex=True)

    #     fig.suptitle(f"Observable Data Availablility For Network: {network} Station: {station} Survey: {survey}")
    #     # Set the x-axis to display dates
    #     for ax in axes:
    #         ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    #         ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    #     data_type_titles = {
    #         DATA_TYPE.IMU.value: "IMU Data",
    #         DATA_TYPE.GNSS.value: "GNSS Data",
    #         DATA_TYPE.ACOUSTIC.value: "Acoustic Data",
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

    # def get_site_config(self,network:str,station:str,survey:str) -> proc_schemas.SiteConfig:
    #     """
    #     Get the Site Config data for a given network, station, and survey.

    #     Args:
    #         network (str): The network name.
    #         station (str): The station name.
    #         survey (str): The survey name.

    #     Raises:
    #         Exception: If no matching data is found in the catalog.
    #     """

    #     data_type_to_plot = [DATA_TYPE.SITECONFIG.value]

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
    #         site_config_schema = SCHEMA_MAP[DATA_TYPE.SITECONFIG](**site_config)
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
    #         & (self.catalog_data.type==DATA_TYPE.SVP.value)
    #     ]

    #     if entries.shape[0] < 1:
    #         raise Exception('No matching SVP data found in catalog')

    #     # load the SVP data
    #     path = entries.local_path.values[0]
    #     svp_data = pd.read_csv(path)
    #     return svp_data

    # def get_atd_offset(self,network:str,station:str,survey:str) -> proc_schemas.ATDOffset:
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
    #         & (self.catalog_data.type == DATA_TYPE.ATDOFFSET.value)
    #     ]

    #     if entries.shape[0] < 1:
    #         raise Exception('No matching ATD Offset data found in catalog')

    #     # load the ATD Offset data
    #     path = entries.local_path.values[0]
    #     with open(path, "r") as f:
    #         atd_offset = json.load(f)
    #         atd_offset_schema = SCHEMA_MAP[DATA_TYPE.ATDOFFSET](**atd_offset)
    #     return atd_offset_schema

    # def plot_site_config(self,site_config:proc_schemas.SiteConfig,zoom:int=5):
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
