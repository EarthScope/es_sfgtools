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
from tqdm.autonotebook import tqdm 
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
from sqlalchemy.orm import sessionmaker
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

    @classmethod
    def to_schema(cls):
        return [x.name for x in cls]


DATA_TYPES = [x.value for x in DATA_TYPE]

TARGET_MAP = {
    FILE_TYPE.QCPIN:{DATA_TYPE.IMU:proc_funcs.qcpin_to_imudf,DATA_TYPE.ACOUSTIC:proc_funcs.qcpin_to_acousticdf,FILE_TYPE.NOVATELPIN:proc_funcs.qcpin_to_novatelpin},
    FILE_TYPE.NOVATELPIN:{FILE_TYPE.RINEX:proc_funcs.novatel_to_rinex},
    FILE_TYPE.NOVATEL:{FILE_TYPE.RINEX:proc_funcs.novatel_to_rinex, DATA_TYPE.IMU:proc_funcs.novatel_to_imudf},
    FILE_TYPE.RINEX:{FILE_TYPE.KIN:proc_funcs.rinex_to_kin},
    FILE_TYPE.KIN:{DATA_TYPE.GNSS:proc_funcs.kin_to_gnssdf},
    FILE_TYPE.SONARDYNE:{DATA_TYPE.ACOUSTIC:proc_funcs.sonardyne_to_acousticdf},
    FILE_TYPE.MASTER:{DATA_TYPE.SITECONFIG:proc_funcs.masterfile_to_siteconfig},
    FILE_TYPE.LEVERARM:{DATA_TYPE.ATDOFFSET:proc_funcs.leverarmfile_to_atdoffset},
    FILE_TYPE.SEABIRD:{DATA_TYPE.SVP:proc_funcs.seabird_to_soundvelocity},
    FILE_TYPE.NOVATEL770:{FILE_TYPE.RINEX:proc_funcs.novatel_to_rinex},
    FILE_TYPE.DFPO00:{DATA_TYPE.IMU:proc_funcs.dfpo00_to_imudf, DATA_TYPE.ACOUSTIC:proc_funcs.dfpo00_to_acousticdf}
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

class InputURL(pa.DataFrameModel):
    uuid: pa.typing.Series[pa.String] = pa.Field(
        description="Unique identifier", default=None
    )
    bucket: pa.typing.Series[pa.String] = pa.Field(description="S3 bucket",default=None,nullable=True)
    network: pa.typing.Series[pa.String] = pa.Field(description="Network name")
    station: pa.typing.Series[pa.String] = pa.Field(description="Station name")
    survey: pa.typing.Series[pa.String] = pa.Field(description="Survey name")
    remote_prefix: pa.typing.Series[pa.String] = pa.Field(description="Remote S3 URL",default=None,nullable=True)
    type: pa.typing.Series[pa.String] = pa.Field(
        description="Type of data",
        default=None,
        isin=FILE_TYPES + DATA_TYPES + [None],
    )
    timestamp: pa.typing.Series[pa.Timestamp] = pa.Field(description="Timestamp",default=None,nullable=True)

    class Config:
        coerce=True
        drop_invalid_rows=True


class DataCatalog(InputURL):

    local_location: pa.typing.Series[pa.String] = pa.Field(
        description="Local location", default=None,nullable=True
    )
    source_uuid: pa.typing.Series[pa.String] = pa.Field(description="Source identifier",default=pd.NA,nullable=True )
    processed: pa.typing.Series[pa.Bool] = pa.Field(description="Child data has been aquired",default=False)
    timestamp_minted: pa.typing.Series[pa.Timestamp] = pa.Field(description="Timestamp of the catalog entry",default=datetime.datetime.now())
    class Config:
        coerce=True
        add_missing_columns=True
        drop_invalid_rows=True

    @pa.parser("timestamp")
    def parse_timestamp(cls, value):
        return pd.to_datetime(value,format="mixed")


class DataHandler:
    """
    A class to handle data operations such as adding campaign data, downloading data, and processing data.
    """

    def __init__(self,working_dir:Union[Path,str]) -> None:
        """
        Initialize the DataHandler object.

        Creates the following directories if they do not exist:
            - raw/
            - intermediate/
            - processed/

        Args:
            working_dir (Path): The working directory path.

        Returns:
            None
        """
        if isinstance(working_dir,str):
            working_dir = Path(working_dir)

        self.working_dir = working_dir
        self.raw_dir = self.working_dir / "raw/"
        self.inter_dir = self.working_dir / "intermediate/"
        self.proc_dir = self.working_dir / "processed/"
        self.pride_dir = self.inter_dir / "Pride"
        self.garpos_dir = self.working_dir / "Garpos"
        self.pride_dir.mkdir(parents=True,exist_ok=True)
        self.working_dir.mkdir(parents=True,exist_ok=True)
        self.inter_dir.mkdir(exist_ok=True)
        self.proc_dir.mkdir(exist_ok=True)
        self.raw_dir.mkdir(exist_ok=True)
        self.garpos_dir.mkdir(exist_ok=True)

        self.db_path = self.working_dir/"catalog.sqlite"
        if not self.db_path.exists():
            self.db_path.touch()
            self.engine = sa.create_engine(f"sqlite+pysqlite:///{self.db_path}")
            Base.metadata.create_all(self.engine)

        else:
            self.engine = sa.create_engine(f"sqlite+pysqlite:///{self.db_path}")

        logging.basicConfig(level=logging.INFO,filename=self.working_dir/"datahandler.log")
        logger.info(f"Data Handler initialized, data will be stored in {self.working_dir}")

    def _get_timestamp(self,remote_prefix:str) -> pd.Timestamp:
        """
        Get the timestamp from the remote file prefix.

        Args:
            remote_prefix (str): The remote prefix.

        Returns:
            pd.Timestamp: The timestamp extracted from the remote prefix.
        """
        basename = Path(remote_prefix).name

        try:
            date_str = basename.split("_")[1].split(".")[0]
            return pd.to_datetime(date_str,format="%Y%m%d%H%M%S")
        except:
            return None

    # def load_catalog_from_csv(self):
    #     self.consolidate_entries()
    #     self.catalog_data = pd.read_csv(self.catalog, parse_dates=['timestamp'])
    #     self.catalog_data['timestamp'] = self.catalog_data['timestamp'].astype('datetime64[ns]')

    def get_dtype_counts(self):
        
        with self.engine.begin() as conn:
            data_type_counts = conn.execute(
                sa.select([Assets.type,sa.func.count(Assets.type)]).group_by(Assets.type)
                ).fetchall()
        return data_type_counts

    def add_data_local(self,
                    network:str,
                    station:str,
                    survey:str,
                    local_filepaths:List[str],
                    discover_file_type:bool=False,
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
                # logger.error(f"File type not recognized for {file}")
                # warnings.warn(f"File type not recognized for {file}", UserWarning)
                continue

            file_data = {
                "network": network,
                "station": station,
                "survey": survey,
                "local_path": str(file),
                "type": discovered_file_type,
                "timestamp_created": datetime.datetime.now(),
            }

            file_data_list.append(file_data)
            count += 1

        # See if the data is already in the catalog
        with sessionmaker(bind=self.engine) as conn:
            # get local file paths under the same network, station, survey
            file_data_map = {x['local_path']:x for x in file_data_list}

            existing_files = conn.execute(sa.select(Assets.local_path).where(
                Assets.network.in_([network]),Assets.station.in_([station]),Assets.survey.in_([survey])
            )).fetchall()
            # remove existing files from the file_data_map
            for file in existing_files:
                file_data_map.pop(file.local_path,None)
            
            if len(file_data_map) == 0:
                response = f"No new files to add"
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
            conn.commit()
            

    def add_data_remote(self, 
                          network: str, 
                          station: str, 
                          survey: str, 
                          remote_filepaths: List[str],
                          remote_type:Union[REMOTE_TYPE,str] = REMOTE_TYPE.HTTP,

                          **kwargs):
        """
        Add campaign data to the catalog.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
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
                raise ValueError(f"File type not recognized for {file}")

            file_data = {
                "network": network,
                "station": station,
                "survey": survey,
                "remote_filepath": file,
                "remote_type": remote_type.value,
                "type": discovered_file_type,
                "timestamp_created": datetime.datetime.now(),
            }
            file_data_list.append(file_data)

        # See if the data is already in the catalog
        file_data_map = {x['remote_filepath']:x for x in file_data_list}
        with sessionmaker(bind=self.engine) as conn:
            existing_files = conn.execute(sa.select(Assets.remote_path).where(
                Assets.network.in_([network]),Assets.station.in_([station]),Assets.survey.in_([survey])
            )).fetchall()
            # remove existing files from the file_data_map
            for file in existing_files:
                file_data_map.pop(file.remote_path,None)
            
            if len(file_data_map) == 0:
                response = f"No new files to add"
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
            conn.commit()
        
    def download_data(self,
                    network:str,
                    station:str,
                    survey:str,
                    file_type: str="all",
                    override:bool=False,
                    show_details:bool=True):
        """
        Retrieves and catalogs data from the remote locations stored in the catalog.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            file_type (str): The type of file to download
            override (bool): Whether to download the data even if it already exists
            from_s3 (bool): Use S3 download functionality if remote resourses are in an s3 bucket
            show_details (bool): Log details of each file downloaded  

        Raises:
            Exception: If no matching data found in catalog.
        """
        os.environ["DH_SHOW_DETAILS"] = str(show_details)

        with self.engine.begin() as conn:
            entries = conn.execute(
                sa.select(Assets).where(
                    Assets.network.in_([network]),Assets.station.in_([station]),Assets.survey.in_([survey])
                )
            ).fetchall()
        if len(entries) == 0:
            response = f"No matching data found in catalog"
            logger.error(response)
            print(response)
            return
        # find entries that have a value for "local_path"
        local_entries = [False for x in entries if x.local_path is not None and Path(x.local_path).exists() else True]
        to_get = np.logical_or(local_entries,override)
        entries = np.array(entries)[to_get].tolist()
        if len(entries) == 0:
            response = f"No new files to download"
            logger.info(response)
            print(response)
            return
        # split the entries into s3 and http
        s3_entries = [x for x in entries if x.remote_type == REMOTE_TYPE.S3.value]
        http_entries = [x for x in entries if x.remote_type == REMOTE_TYPE.HTTP.value]
        updated_entries = []
        # download s3 entries
        if len(s3_entries) > 0:
            s3_entries_processed = []
            for entry in s3_entries:
                _path = Path(entry.remote_path)
                s3_entries_processed.append({
                    "bucket":_path.root,
                    "prefix":_path.relative_to(bucket)
                })
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(self._download_data_s3,s3_entries_processed)
                for result,entry in zip(results,s3_entries):
                    if result is not None:
                        entry.local_path = str(result)
                        updated_entries.append(entry)
        
        # download http entries
        if len(http_entries) > 0:
            _download_func = partial(self._download_https,destination_dir=self.raw_dir)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(_download_func,[x.remote_path for x in http_entries])
                for result,entry in zip(results,http_entries):
                    if result is not None:
                        entry.local_path = str(result)
                        updated_entries.append(entry)
        
        # update the database
        with self.engine.begin() as conn:
            while updated_entries:
                entry = updated_entries.pop()
                conn.execute(
                    sa.update(Assets).where(Assets.remote_path == entry.remote_path).values(dict(entry))
                )
    
    def _download_data_s3(self,bucket:str,prefix:str,**kwargs) -> Union[Path,None]:
        """
        Retrieves and catalogs data from the s3 locations stored in the catalog.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.

        Raises:
            Exception: If no matching data found in catalog.
        """
        with threading.Lock():
            client = boto3.client('s3')
  

        local_location = self.raw_dir / Path(entry.remote_prefix).name

        # If the file does not exist and has not been processed, then download it!
        try:
            client.download_file(Bucket=bucket, Key=str(prefix), Filename=str(local_location))
            response = f"Downloaded {str(prefix)} to {str(local_location)}"
            logger.info(response)
            if os.environ.get("DH_SHOW_DETAILS",False):
                print(response)
            return local_location
        
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
                        token_path='.') -> Union[Path,None]:
        """
        Downloads a file from the specified https url on gage-data

        Args:
            remote_url (Path): The path of the file in the gage-data storage.
            destination (Path): The local path where the file will be downloaded.

        Returns:
            bool: True if the file was downloaded successfully, False otherwise.
        """
        try:
            local_location = destination_dir / Path(remote_url).name
            download_file_from_archive(url=remote_url, 
                                    dest_dir=destination_dir, 
                                    token_path=token_path,
                                    show_details=show_details)
            if not local_location.exists():
                raise Exception
            
            response = f"Downloaded {str(remote_url)} to {str(local_location)}"
            logger.info(response)
            if os.environ.get("DH_SHOW_DETAILS",False):
                print(response)
            return local_location
        
        except Exception as e:
            response = f"Error downloading {str(remote_url)} \n {e}"
            response += "\n HINT: Check authentication credentials"
            logger.error(response)
            if os.environ.get("DH_SHOW_DETAILS",False):
                print(response)
            return None

    def clear_raw_processed_data(self, network: str, station: str, survey: str):
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
        #     & (~self.catalog_data.local_location.isna())
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
        #     if is_parent_processed and not pd.isna(entry["local_location"]):
        #         Path(entry["local_location"]).unlink()
        #         self.catalog_data.loc[(self.catalog_data.uuid == entry["uuid"]), "processed"] = True
        #         self.catalog_data.loc[self.catalog_data.uuid == entry["uuid"], "local_location"] = None

        #         response = f"Removed Raw File {entry['uuid']} of Type {entry['type']} From {entry['local_location']} "
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

    def add_or_update_entry(self, entry: dict,conn):



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

    def _process_targeted(
        self, parent: dict, child_type: Union[FILE_TYPE, DATA_TYPE]
    ) -> Tuple[dict,dict,bool]:

        if os.environ.get("DH_SHOW_DETAILS",False):
            show_details = True
        response = ""
        # TODO: implement multithreaded logging, had to switch to print statement below

        # handle the case when the parent timestamp is None
        child_timestamp = parent.get("timestamp_data_start",None)

        response += f"Processing {parent["local_path"]} ({parent["id"]}) of Type {parent["type"]} to {child_type.value}\n"
        # Get the processing function that converts the parent entry to the child entry
        process_func = TARGET_MAP.get(FILE_TYPE(parent["type"])).get(child_type)
        # Build the source object from the parent entry
        source = SCHEMA_MAP[FILE_TYPE(parent["type"])](
            location=Path(parent["local_path"]),
            uuid=parent["id"],
            timestamp_data_start=parent["timestamp_data_start"],
        )
        # build partial processing function
        match process_func:
            case proc_funcs.rinex_to_kin:
                process_func_p = partial(
                    process_func,
                    writedir=self.inter_dir,
                    pridedir=self.pride_dir,
                    site=parent["station"],
                )

            case proc_funcs.novatel_to_rinex:
                process_func_p = partial(
                    process_func,site=parent["station"],year=parent.get("timestamp_data_start",datetime.datetime.now()).year,show_details=show_details
                )

            case proc_funcs.qcpin_to_novatelpin:
                process_func_p = partial(
                    process_func,outpath=self.inter_dir
                )
            case _:
                process_func_p = process_func

        processed = None
        timestamp_data_start = parent.get("timestamp_data_start",None)
        timestamp_data_end = parent.get("timestamp_data_end",None)

        if source.local_path is not None and source.local_path.exists():
            if source.local_path.stat().st_size == 0:
                response += f"File {source.local_path} is empty\n"
                processed = None
            else:
                processed = process_func_p(source)


        match type(processed):
            case pd.DataFrame:
                local_location = self.proc_dir / f"{parent["id"]}_{child_type.value}.csv"
                processed.to_csv(local_location, index=False)

                # handle the case when the child timestamp is None
                if pd.isna(parent["timestamp_data_start"]):
                    for col in processed.columns:
                        if pd.api.types.is_datetime64_any_dtype(processed[col]):
                            timestamp_data_start = processed[col].min()
                            timestamp_data_end = processed[col].max()
                            break

            case proc_schemas.RinexFile:
                processed.write(self.inter_dir)
                local_location = processed.location
                timestamp_data_start = processed.timestamp_data_start
                timestamp_data_end = processed.timestamp_data_end

            case proc_schemas.KinFile:

                # processed.write(self.inter_dir)
                local_location = processed.location

            case proc_schemas.SiteConfig:
                local_location = (
                    self.proc_dir / f"{parent["id"]}_{child_type.value}.json"
                )
                with open(local_location, "w") as f:
                    f.write(processed.model_dump_json())

            case proc_schemas.ATDOffset:
                local_location = (
                    self.proc_dir / f"{parent["id"]}_{child_type.value}.json"
                )
                with open(local_location, "w") as f:
                    f.write(processed.model_dump_json())

            case proc_schemas.NovatelPinFile:
                local_location = (
                    self.inter_dir / f"{parent["id"]}_{child_type.value}.txt"
                )
                processed.location = local_location
                processed.write(dir=local_location.parent)

            case _:
                is_processed = False
                local_location = None
                pass

        if pd.isna(parent.get("timestamp_data_start",None)) and timestamp_data_start is not None:
            parent["timestamp_data_start"] = timestamp_data_start
            parent["timestamp_data_end"] = timestamp_data_end
            response += f"Discovered timestamp: {timestamp_data_start} for parent {parent["type"]} uuid {parent["id"]}\n"

        processed_meta = {
            "network": parent["network"],
            "station": parent["station"],
            "survey": parent["survey"],
            "local_location": if local_location.exists(): str(local_location) else None,
            "type": child_type.value,
            "timestamp_data_start": timestamp_data_start,
            "timestamp_data_end": timestamp_data_end,
            "source_id": parent["id"]
        }
        if local_location is not None and local_location.exists():
            response += f"Successful Processing: {str(processed_meta)}\n"
        else:
            response += f"Failed Processing: {str(processed_meta)}\n"
       
        return processed_meta,parent,response

    def _process_data_link(self,
                           network:str,
                           station:str,
                           survey:str,
                           target:Union[FILE_TYPE,DATA_TYPE],
                           source:List[FILE_TYPE],
                           override:bool=False) -> pd.DataFrame:
        """
        Process data from a source to a target.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            target (Union[FILE_TYPE,DATA_TYPE]): The target data type.
            source (List[FILE_TYPE]): The source data types.
            override (bool): Whether to override existing child entries.

        Raises:
            Exception: If no matching data is found in the catalog.
        """
        show_details = bool(os.environ.get("DH_SHOW_DETAILS",False))
        # Get the parent entries
        with self.engine.begin() as conn:
            parent_entries = conn.execute(
                sa.select(Assets).where(
                    Assets.network.is_(network),Assets.station.is_(station),Assets.survey.is_(survey),
                    Assets.local_location.isnot(None),Assets.type.in_([x.value for x in source])
                )
            ).fetchall()

        if not parent_entries:
            response = f"No unprocessed data found in catalog for types {[x.value for x in source]}"
            logger.error(response)
            # print(response)
            return
        
        # Filter out parent entries that have already been processed
        parent_entries_map = {x["id"]:x for x in parent_entries}
        with self.engine.begin() as conn:
            child_entries = conn.execute(
                sa.select(Assets).where(
                    Assets.network.is_(network),Assets.station.is_(station),Assets.survey.is_(survey),
                    Assets.type.is_(target.value),Assets.parent_id.in_([x["id"] for x in parent_entries])
                )
            ).fetchall()
        if not override:
            while child_entries:
                child = child_entries.pop()
                parent_entries_map.pop(child["parent_id"],None)

        parent_entries_to_process = list(parent_entries_map.values())

        if parent_entries_to_process:
            logger.info(f"Processing {len(p)} Parent Files to {target.value} Data")
            process_func_partial = partial(self._process_targeted,child_type=target)

            child_data_list = []
            parent_data_list = []
            with multiprocessing.Pool() as pool:
                results = pool.imap_unordered(process_func_partial,parent_entries_to_process)
                for child_data,parent_data,response in tqdm(results,total=len(parent_entries_to_process),desc=f"Processing {source} To {target.value}"):
                    if show_details:
                        print(response)
                        logger.info(response)
                    if child_data is None:
                        continue
                    parent_data_list.append(parent_data)
                    child_data_list.append(child_data)                    
                    # update parent entry
                    with self.engine.begin() as conn:
                        conn.execute(
                            sa.update(Assets).where(Assets.id.is_(parent_data["id"])).values(parent_data)
                        )
                        found = conn.execute(
                            sa.select(Assets).where(
                                Assets.source_id.is_(parent_data["id"]), Assets.type.is_(target.value)
                            )
                        )
                        if found:
                            conn.execute(
                                sa.update(Assets).where(
                                    Assets.source_id.is_(parent_data["id"]), Assets.type.is_(target.value)
                                ).values(child_data)
                            )
                        else:
                            conn.execute(sa.insert(Assets).values(child_data))


            logger.info(f"Processed {len(child_data_list)} Out of {len(parent_entries_to_process)} For {target.value}")
       
            return parent_data_list

    def _process_data_graph(self, 
                            network: str, 
                            station: str, 
                            survey: str,
                            child_type:Union[FILE_TYPE,DATA_TYPE],
                            override:bool=False,
                            show_details:bool=False,
                            update_timestamp:bool=False):
        if show_details:
            os.environ["DH_SHOW_DETAILS"] = "True"

        # self.load_catalog_from_csv()
        processing_queue = self.get_parent_stack(child_type=child_type)
        if show_details:
            logger.info(f"processing queue: {[item.value for item in processing_queue]}")
        while processing_queue:
            parent = processing_queue.pop(0)
            if parent != child_type:

                children:dict = TARGET_MAP.get(parent,{})

                children_to_process = [k for k in children.keys() if k in processing_queue]
                if show_details:
                    logger.info(f"parent: {parent.value}")
                    logger.info(f"children to process: {[item.value for item in children_to_process]}")
                for child in children_to_process:
                    if show_details:
                        logger.info(f"processing child:{child.value}")
                    processed_parents:List[dict] = self._process_data_link(
                        network,
                        station,
                        survey,
                        target=child,
                        source=[parent],
                        override=override)
                    # Check if all children of this parent have been processed
                    
                    #TODO check if all children of this parent have been processed

    def _process_data_graph_forward(self, 
                            network: str, 
                            station: str, 
                            survey: str,
                            parent_type:FILE_TYPE,
                            update_timestamp:bool=False,
                            override:bool=False,
                            show_details:bool=False):

        self.load_catalog_from_csv()
        processing_queue = [{parent_type:TARGET_MAP.get(parent_type)}]
        while processing_queue:
            # process each level of the child graph
            parent_targets = processing_queue.pop(0)
            parent_type = list(parent_targets.keys())[0]
            for child in parent_targets[parent_type].keys():

                self._process_data_link(network,station,survey,target=child,source=[parent_type],override=override,update_timestamp=update_timestamp,show_details=show_details)
                child_targets = TARGET_MAP.get(child,{})
                if child_targets:
                    processing_queue.append({child:child_targets})

    def process_acoustic_data(self, network: str, station: str, survey: str,override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(network,station,survey,DATA_TYPE.ACOUSTIC,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_imu_data(self, network: str, station: str, survey: str,override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(network,station,survey,DATA_TYPE.IMU,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_rinex(self, network: str, station: str, survey: str,override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(network,station,survey,FILE_TYPE.RINEX,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_gnss_data_kin(self, network: str, station: str, survey: str,override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(network,station,survey,FILE_TYPE.KIN,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_gnss_data(self, network: str, station: str, survey: str,override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(network,station,survey,DATA_TYPE.GNSS,override=override, show_details=show_details,update_timestamp=update_timestamp)

    def process_siteconfig(self, network: str, station: str, survey: str,override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        self._process_data_graph(network,station,survey,DATA_TYPE.SITECONFIG,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self._process_data_graph(network,station,survey,DATA_TYPE.ATDOFFSET,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self._process_data_graph(network,station,survey,DATA_TYPE.SVP,override=override, show_details=show_details,update_timestamp=update_timestamp)
    def process_target(self,network:str,station:str,survey:str,parent:str,child:str,override:bool=False,show_details:bool=False):
        target = DATA_TYPE(child)
        source = FILE_TYPE(parent)
        self._process_data_link(network,station,survey,target=target,source=[source],override=override,show_details=show_details)

    def process_campaign_data(
        self, network: str, station: str, survey: str, override: bool = False, show_details: bool=False,update_timestamp:bool=False
    ):
        """
        Process all data for a given network, station, and survey, generating child entries where applicable.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            override (bool): Whether to override existing child entries.

        Raises:
            Exception: If no matching data is found in the catalog.
        """
        self.process_acoustic_data(network,station,survey,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self.process_imu_data(network,station,survey,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self.process_gnss_data(network,station,survey,override=override, show_details=show_details,update_timestamp=update_timestamp)
        self.process_siteconfig(network,station,survey,override=override, show_details=show_details,update_timestamp=update_timestamp)

        logger.info(
            f"Network {network} Station {station} Survey {survey} Preprocessing complete"
        )

    def process_qc_data(self, network: str, station: str, survey: str,override:bool=False, show_details:bool=False,update_timestamp:bool=False):
        # perform forward processing of qc pin data
        self._process_data_graph_forward(network,station,survey,FILE_TYPE.QCPIN,override=override, show_details=show_details,update_timestamp=update_timestamp)




    # def get_observation_session_data(self,network:str,station:str,survey:str,plot:bool=False) -> pd.DataFrame:

    #     time_groups = self.catalog_data[self.catalog_data.type.isin(['gnss','acoustic','imu'])].groupby('timestamp')
    #     valid_groups = [group for name, group in time_groups] #
    #     result = pd.concat(valid_groups)
    #     primary_colums = ['network','station','survey','timestamp','type','local_location']
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
    #             out[timestamp][obs_type] = list(group[group.type == obs_type].local_location.values)

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
    #     path = entries.local_location.values[0]
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
    #     path = entries.local_location.values[0]
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
    #     path = entries.local_location.values[0]
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
