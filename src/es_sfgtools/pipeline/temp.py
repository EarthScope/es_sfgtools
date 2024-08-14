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
warnings.filterwarnings("ignore")
seaborn.set_theme(style="whitegrid")
from es_sfgtools.utils.gage_data import get_file_from_gage_data
from es_sfgtools.processing import functions as proc_funcs
from es_sfgtools.processing import schemas as proc_schemas

logger = logging.getLogger(__name__)

class FILE_TYPE(Enum):
    SONARDYNE = "sonardyne"
    NOVATEL = "novatel"
    KIN = "kin"
    RINEX = "rinex"
    MASTER= "master"    
    LEVERARM = "leverarm"
    SEABIRD = "svpavg"
    NOVATEL770 = "novatel770"
    DFPO00 = "dfpo00"
    OFFLOAD = "offload"

    @classmethod
    def to_schema(cls):
        return [x.name for x in cls]


FILE_TYPES = [x.value for x in FILE_TYPE]

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
    FILE_TYPE.NOVATEL:{FILE_TYPE.RINEX:proc_funcs.novatel_to_rinex, DATA_TYPE.IMU:proc_funcs.novatel_to_imudf},
    FILE_TYPE.RINEX:{FILE_TYPE.KIN:proc_funcs.rinex_to_kin},
    FILE_TYPE.KIN:{DATA_TYPE.GNSS:proc_funcs.kin_to_gnssdf},
    FILE_TYPE.SONARDYNE:{DATA_TYPE.ACOUSTIC:proc_funcs.sonardyne_to_acousticdf},
    FILE_TYPE.MASTER:{DATA_TYPE.SITECONFIG:proc_funcs.masterfile_to_siteconfig},
    FILE_TYPE.LEVERARM:{DATA_TYPE.ATDOFFSET:proc_funcs.leverarmfile_to_atdoffset},
    FILE_TYPE.SEABIRD:{DATA_TYPE.SVP:proc_funcs.seabird_to_soundvelocity},
    FILE_TYPE.NOVATEL770:{FILE_TYPE.RINEX:proc_funcs.novatel770_to_rinex},
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
    timestamp: pa.typing.Series[pa.DateTime] = pa.Field(description="Timestamp",default=None,nullable=True)

    class Config:
        coerce=True
        add_missing_columns=True

class DataCatalog(InputURL):
    
    local_location: pa.typing.Series[pa.String] = pa.Field(
        description="Local location", default=None,nullable=True
    )
    source_uuid: pa.typing.Series[pa.String] = pa.Field(description="Source identifier",default=None,nullable=True )
    processed: pa.typing.Series[pa.Bool] = pa.Field(description="Child data has been aquired",default=False)
    class Config:
        coerce=True
        add_missing_columns=True


class DataHandler:
    """
    A class to handle data operations such as adding campaign data, downloading data, and processing data.
    """

    def __init__(self,working_dir:Path) -> None:
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

        self.working_dir = working_dir
        self.raw_dir = self.working_dir / "raw/"
        self.inter_dir = self.working_dir / "intermediate/"
        self.proc_dir = self.working_dir / "processed/"

        self.working_dir.mkdir(parents=True,exist_ok=True)
        self.inter_dir.mkdir(exist_ok=True)
        self.proc_dir.mkdir(exist_ok=True)
        self.raw_dir.mkdir(exist_ok=True)
        self.working_dir.mkdir(exist_ok=True)

        self.catalog = self.working_dir/"catalog.csv"
        if self.catalog.exists():
            self.catalog_data = DataCatalog.validate(pd.read_csv(self.catalog))
        else:
            self.catalog_data = pd.DataFrame()
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

    def get_local_counts(self):
        try:
            local_files = self.catalog_data[self.catalog_data['local_location'].notnull()]
            data_type_counts = local_files.type.value_counts()
        except (AttributeError, KeyError):
            data_type_counts = pd.Series()   
        return data_type_counts
    
    def get_dtype_counts(self):
        try:
            data_type_counts = self.catalog_data[self.catalog_data.type.isin(FILE_TYPES)].type.value_counts()
        except AttributeError:
            data_type_counts = "No data types found"    
        return data_type_counts
    
    def add_campaign_data(self, 
                          network: str, 
                          station: str, 
                          survey: str, 
                          remote_filepaths: List[str], 
                          **kwargs):
        """
        Add campaign data to the catalog.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            remote_filepaths (List[str]): A list of file locations on gage-data.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        incoming = []
        for file in remote_filepaths:
            discovered_file_type = None
            for file_type in FILE_TYPES:
                if file_type in file.replace("_", ""):
                    discovered_file_type = file_type
                    break
            
            if discovered_file_type is None:
                logger.error(f"File type not recognized for {file}")
                continue

            file_data = {
                "uuid": uuid.uuid4().hex,
                "network": network,
                "station": station,
                "survey": survey,
                "remote_filepath": file,
                "type": discovered_file_type,
                "timestamp": self._get_timestamp(file)
            }
            incoming.append(file_data)

        # See if the data is already in the catalog
        incoming_df = InputURL.validate(pd.DataFrame(incoming))
        if self.catalog_data.shape[0] > 0:
            # Match against network, station, survey, type, and timestamp
            matched = pd.merge(
                self.catalog_data,
                incoming_df,
                how="right",
                on=["network", "station", "survey", "type", "timestamp"],
                indicator=True
            )

            # Get uuid's for new data
            new_data = matched[matched["_merge"] == "right_only"]
            incoming_df = incoming_df[incoming_df.uuid.isin(new_data.uuid_y)]

        # If matched, there will be an "id" field
        if incoming_df.shape[0] > 0:
            incoming_df = DataCatalog.validate(incoming_df, lazy=True)
            logger.info(f"Adding {incoming_df.shape[0]} to the current catalog")
            self.catalog_data = pd.concat([self.catalog_data, incoming_df])

        self.catalog_data.to_csv(self.catalog,index=False)

    def download_campaign_data(self,
                               network:str,
                               station:str,
                               survey:str,
                               file_type: str,
                               override:bool=False,
                               from_s3:bool=False):
        """
        Retrieves and catalogs data from the remote locations stored in the catalog.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            file_type (str): The type of file to download
            override (bool): Whether to download the data even if it already exists
            from_s3 (bool): Use S3 download functionality if remote resourses are in an s3 bucket  

        Raises:
            Exception: If no matching data found in catalog.
        """
        # TODO make multithreaded
        # Find all entries in the catalog that match the params
        local_counts = self.get_local_counts()
        try:
            local_files_of_type = local_counts[file_type]    
        except KeyError:
            local_files_of_type = 0
        logger.info(f"Data directory currently contains {local_files_of_type} files of type {file_type}")
        entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.type == file_type)
        ]
        logger.info(f"Downloading {entries.shape[0]-local_files_of_type} missing files of type {file_type}")
        if entries.shape[0] < 1:
            raise Exception('No matching data found in catalog')
        if from_s3:
            client = boto3.client('s3')
        count = 0
        for entry in entries.itertuples(index=True):
            if pd.isna(entry.remote_filepath):
                continue
            local_location = self.raw_dir / Path(entry.remote_filepath).name

            # If the file does not exist and has not been processed, then download it!
            if not local_location.exists() or override:
                if from_s3:
                    is_download = self._download_boto(
                        client=client,
                        bucket=entry.bucket,
                        remote_url=entry.remote_prefix,
                        destination=local_location,
                    )
                # Check if the entry is from an S3 location or gage-data
                else:
                    is_download = self._download_https(
                        remote_url=entry.remote_filepath, destination_dir=self.raw_dir
                    )
        
                if is_download:
                    # Check if we can find the file
                    assert local_location.exists(), "Downloaded file not found"
                    self.catalog_data.at[entry.Index,"local_location"] = str(local_location)
                    count += 1
                else:
                    raise Warning(f'File not downloaded to {str(local_location)}')
        if count == 0:
            response = f"No files downloaded"
            logger.error(response)
            #print(response)
        else:
            logger.info(f"Downloaded {count} files")

        self.catalog_data.to_csv(self.catalog,index=False)

    def add_campaign_data_s3(self, network: str, station: str, survey: str, bucket: str, prefixes: List[str], **kwargs):
        """
        Add campaign data to the catalog.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.
            bucket (str): The bucket name.
            prefixes (List[str]): A list of file prefixes.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        incoming = []
        for file in prefixes:
            discovered_file_type = None
            for file_type in FILE_TYPES:
                if file_type in file.replace("_", ""):
                    discovered_file_type = file_type
                    break

            if discovered_file_type is None:
                logger.error(f"File type not recognized for {file}")
                continue

            file_data = {
                "uuid": uuid.uuid4().hex,
                "network": network,
                "station": station,
                "survey": survey,
                "bucket": bucket,
                "remote_prefix": file,
                "type": discovered_file_type,
                "timestamp": self._get_timestamp(file)
            }
            incoming.append(file_data)

        # See if the data is already in the catalog
        incoming_df = InputURL.validate(pd.DataFrame(incoming))
        if self.catalog_data.shape[0] > 0:
            # Match against network, station, survey, type, and timestamp
            matched = pd.merge(
                self.catalog_data,
                incoming_df,
                how="right",
                on=["network", "station", "survey", "type", "timestamp"],
                indicator=True
            )

            # Get uuid's for new data
            new_data = matched[matched["_merge"] == "right_only"]
            incoming_df = incoming_df[incoming_df.uuid.isin(new_data.uuid_y)]

        # If matched, there will be an "id" field
        if incoming_df.shape[0] > 0:
            incoming_df = DataCatalog.validate(incoming_df, lazy=True)
            logger.info(f"Adding {incoming_df.shape[0]} to the current catalog")
            self.catalog_data = pd.concat([self.catalog_data, incoming_df])

        self.catalog_data.to_csv(self.catalog,index=False)
        # Get count of each data type in the catalog
        data_type_counts = self.catalog_data[self.catalog_data.type.isin(FILE_TYPES)].type.value_counts()
        return data_type_counts

    def download_campaign_data_s3(self,network:str,station:str,survey:str,override:bool=False):
        """
        Retrieves and catalogs data from the s3 locations stored in the catalog.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.

        Raises:
            Exception: If no matching data found in catalog.
        """
        # TODO make multithreaded
        # Find all entries in the catalog that match the params
        entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
        ]

        if entries.shape[0] < 1:
            raise Exception('No matching data found in catalog')
        client = boto3.client('s3')
        count = 0
        for entry in entries.itertuples(index=True):
            if pd.isna(entry.remote_prefix):
                continue
            local_location = self.raw_dir / Path(entry.remote_prefix).name

            # If the file does not exist and has not been processed, then download it!
            if not local_location.exists() or override:
                if self._download_boto(client=client,bucket=entry.bucket, remote_url=entry.remote_prefix,destination=local_location):
                    assert local_location.exists(), "Downloaded file not found"
                    self.catalog_data.at[entry.Index,"local_location"] = str(local_location)
                    count += 1

        if count == 0:
            response = f"No files downloaded"
            logger.error(response)
            print(response)

        logger.info(f"Downloaded {count} files to {str(self.raw_dir)}")

        self.catalog_data.to_csv(self.catalog,index=False)
    
    def _download_https(self, remote_url: Path, destination_dir: Path, token_path='.'):
        """
        Downloads a file from the specified https url on gage-data

        Args:
            remote_url (Path): The path of the file in the gage-data storage.
            destination (Path): The local path where the file will be downloaded.

        Returns:
            bool: True if the file was downloaded successfully, False otherwise.
        """
        try:
            #local_location = destination_dir / Path(remote_url).name
            get_file_from_gage_data(url=remote_url, 
                                    dest_dir=destination_dir, 
                                    token_path=token_path)
            #logger.info(f"Downloaded {str(remote_url)} to {str(local_location)}")
            return True
        except Exception as e:
            response = f"Error downloading {str(remote_url)} \n {e}"
            response += "\n HINT: Check authentication credentials"
            logger.error(response)
            print(response)
            return False
        
    def _download_boto(self, client: boto3.client, bucket: str, remote_url: Path, destination: Path):
        """
        Downloads a file from the specified S3 bucket.

        Args:
            client (boto3.client): The Boto3 client object for S3.
            bucket (str): The name of the S3 bucket.
            remote_url (Path): The path of the file in the S3 bucket.
            destination (Path): The local path where the file will be downloaded.

        Returns:
            bool: True if the file was downloaded successfully, False otherwise.
        """
        try:
            client.download_file(Bucket=bucket, Key=str(remote_url), Filename=str(destination))
            logger.info(f"Downloaded {str(remote_url)} to {str(destination)}")
            return True
        except Exception as e:
            response = f"Error downloading {str(remote_url)} \n {e}"
            response += "\n HINT: $ aws sso login"
            logger.error(response)
            print(response)
            return False

    def clear_raw_processed_data(self, network: str, station: str, survey: str):
        """
        Clear all raw data in type FILE_TYPE from the working directory.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.

        Returns:
            None
        """

        # Get raw data into stack
        parent_entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.type.isin(FILE_TYPES))
            & (~self.catalog_data.local_location.isna())
        ].to_dict(orient="records")

        pbar = tqdm.tqdm(total=len(parent_entries), desc="Removing Raw Files")
        while parent_entries:
            entry = parent_entries.pop()

            entry_type = FILE_TYPE(entry["type"])

            is_parent_processed = True
            child_types = [x.value for x in list(TARGET_MAP.get(entry_type).keys())]

            child_entries = self.catalog_data[
                (self.catalog_data.source_uuid == entry["uuid"])
                & (self.catalog_data.type.isin(child_types))
            ]

            # Check if we can find all child types, if not then skip
            if not entry["processed"]:
                pbar.update(1)
                continue

            # Now we check if the children files exist
            for _, child in child_entries.iterrows():
                if child["type"] in DATA_TYPES:
                    self.catalog_data.loc[self.catalog_data.uuid == child["uuid"], "processed"] = True
                    child["processed"] = True
                is_parent_processed &= child["processed"]
                try:
                    # If the child is a FILE_TYPE, then we will add it to the stack (only applies to the rinex and kin files)
                    # We check this in the try/except block by attempting to instantiate a FILE_TYPE
                    _ = FILE_TYPE(child["type"])
                    parent_entries.append(dict(child))
                    pbar.update(1)
                except ValueError:
                    # The child is a DATA_TYPE, so we can pass
                    pass

            # If all children files exist, is_parent_processed == True, and we can delete the parent file
            if is_parent_processed and not pd.isna(entry["local_location"]):
                Path(entry["local_location"]).unlink()
                self.catalog_data.loc[(self.catalog_data.uuid == entry["uuid"]), "processed"] = True
                self.catalog_data.loc[self.catalog_data.uuid == entry["uuid"], "local_location"] = None

                response = f"Removed Raw File {entry['uuid']} of Type {entry['type']} From {entry['local_location']} "
                logger.info(response)

            self.catalog_data.to_csv(self.catalog, index=False)

        pbar.close()

    def add_entry(self, entry: dict):
        """
        Add an entry in the catalog.  This may result in duplicates, which need to be cleaned up via
        consolidate_entries()

        Args:
            entry (dict): The new entry.

        Returns:
            None
        """
        with self.catalog.open("r") as f:
            keys=list(f.readline().rstrip().split(','))
        entry_str = "\n"
        for key in keys:
            if key in entry:
                entry_str += f"{str(entry[key])}"
            if key != keys[-1]:
                entry_str += ","
        
        with self.catalog.open("a") as f:
            print(entry_str)
            f.write(entry_str)

    def consolidate_entries(self):
        """
        Remove any duplicate entries, keeping the most complete.

        Args: 
            None
        Returns:
            None
        """
        df = pd.read_csv(str(self.catalog))
        df['count'] = pd.isnull(df).sum(1)
        df=df.sort_values(['count']).drop_duplicates(subset=['uuid'],keep='first').drop(labels='count',axis=1)
        df=df.sort_index()
        df.to_csv(self.catalog,index=False)
    
    def update_entry(self,entry:dict):
        """
        Replace an entry in the catalog with a new entry.

        Args:
            entry (dict): The new entry.

        Returns:
            None
        """
        old_entry = self.catalog_data[
            (self.catalog_data.type == entry["type"])
            & (self.catalog_data.source_uuid == entry["source_uuid"])
            & (self.catalog_data.network == entry["network"])
            & (self.catalog_data.station == entry["station"])
            & (self.catalog_data.survey == entry["survey"])
        ]
        if old_entry.shape[0] > 0:
            try:
                [Path(x.local_location).unlink() for x in old_entry.itertuples(index=True)]
            except:
                pass
            self.catalog_data = self.catalog_data.drop(old_entry.index)

        self.catalog_data = pd.concat(
            [self.catalog_data,DataCatalog.validate(pd.DataFrame([entry]),lazy=True)],
            ignore_index=True
        )
        self.catalog_data.to_csv(self.catalog,index=False)

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
            #while parents:
            #    parent = parents.pop()
            for parent in parents:
                stack.append(parent)
            pointer += 1
        return stack[::-1]

    def _process_targeted(
        self, parent: dict, child_type: Union[FILE_TYPE, DATA_TYPE]
    ) -> dict:
        #TODO: implement multithreaded logging, had to switch to print statement below
        if isinstance(parent, dict):
            parent = pd.Series(parent)

        # if parent.processed:
        #     return None
        print(
            f"Attemping to process {os.path.basename(parent.local_location)} ({parent.uuid}) of Type {parent.type} to {child_type.value}"
        )
        child_map = TARGET_MAP.get(FILE_TYPE(parent.type))
        if child_map is None:
            response = (
                f"Child type {child_type.value} not found for parent type {parent.type}"
            )
            logger.error(response)
            raise ValueError(response)

        process_func = child_map.get(child_type)
        source = SCHEMA_MAP[FILE_TYPE(parent.type)](
            location=Path(parent.local_location),
            uuid=parent.uuid,
            capture_time=parent.timestamp,
        )
        if source.location.stat().st_size == 0:
            logger.error(f"File {source.location} is empty")
            return None
        else:

            if process_func == proc_funcs.novatel_to_rinex:
                processed = process_func(
                    source, site=parent.station, year=parent.timestamp.year
                )
            elif process_func == proc_funcs.rinex_to_kin:
                processed = process_func(source, site=parent.station)
            else:
                processed = process_func(source)
            if processed is not None:

                child_uuid = uuid.uuid4().hex
                is_processed = False
                match type(processed):
                    case pd.DataFrame:
                        local_location = (
                            self.proc_dir / f"{parent.uuid}_{child_type.value}.csv"
                        )
                        processed.to_csv(local_location, index=False)
                        is_processed = True
                    case proc_schemas.RinexFile:
                        processed.write(self.inter_dir)
                        local_location = processed.location
                    case proc_schemas.KinFile:
                        processed.location += f"_{child_uuid}_{child_type.value}.kin"
                        processed.write(self.inter_dir)
                        local_location = processed.location
                    case proc_schemas.SiteConfig:
                        local_location = (
                            self.proc_dir / f"{parent.uuid}_{child_type.value}.json"
                        )
                        with open(local_location, "w") as f:
                            f.write(processed.model_dump_json())
                        is_processed = True
                    case proc_schemas.ATDOffset:
                        local_location = (
                            self.proc_dir / f"{parent.uuid}_{child_type.value}.json"
                        )
                        with open(local_location, "w") as f:
                            f.write(processed.model_dump_json())
                        is_processed = True

                processed_meta = {
                    "uuid": child_uuid,
                    "network": parent.network,
                    "station": parent.station,
                    "survey": parent.survey,
                    "local_location": str(local_location),
                    "type": child_type.value,
                    "timestamp": parent.timestamp,
                    "source_uuid": parent.uuid,
                    "processed": is_processed,
                }
                print(f"Successful Processing: {str(processed_meta)}")
                if is_processed == True:
                    self.update_entry(processed_meta)
                    #self.add_entry(processed_meta)
                return processed_meta

    def _process_data_link(self,
                           network:str,
                           station:str,
                           survey:str,
                           target:Union[FILE_TYPE,DATA_TYPE],
                           source:List[FILE_TYPE],
                           override:bool=False) -> None:
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
        parent_entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & np.logical_or(
                (self.catalog_data.processed == False),override
                )
            & (self.catalog_data.type.isin([x.value for x in source]))
        ]

        if parent_entries.shape[0] < 1:
            response = f"No unprocessed data found in catalog for types {[x.value for x in source]}"
            logger.error(response)
            print(response)
            return
        child_entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.type == target.value)
        ]

        parent_entries_to_process = parent_entries[np.logical_or(~parent_entries.uuid.isin(child_entries.source_uuid),override)]

        if parent_entries_to_process.shape[0] > 0:
            logger.info(f"Processing {parent_entries_to_process.shape[0]} Parent Files to {target.value} Data")

            process_func_partial = partial(self._process_targeted,child_type=target)

            meta_data_list = []
            with Pool(processes=cpu_count()) as pool:
                for meta_data in tqdm(
                    pool.map(
                        process_func_partial,
                        parent_entries_to_process.to_dict(orient="records"),
                    ),
                    total=parent_entries_to_process.shape[0],
                    desc=f"Processing {parent_entries_to_process.type.unique()} To {target.value}",
                ):
                    if meta_data is not None:
                        self.update_entry(meta_data)
                        #self.add_entry(meta_data)
                        meta_data_list.append(meta_data)

            parent_entries_processed = parent_entries_to_process[
                parent_entries_to_process.uuid.isin(
                    [x["source_uuid"] for x in meta_data_list]
                )
            ]
            logger.info(f"Processed {len(meta_data_list)} Out of {parent_entries_processed.shape[0]} For {target.value} Files from {parent_entries_processed.shape[0]} Parent Files")
            self.consolidate_entries()
            return parent_entries_processed

    def _process_data_graph(self, network: str, station: str, survey: str,child_type:Union[FILE_TYPE,DATA_TYPE],override:bool=False):
        processing_queue = self.get_parent_stack(child_type=child_type)
        #logger.info(f"processing queue: {processing_queue}")
        while processing_queue:
            parent = processing_queue.pop(0)
            #logger.info(f"parent: {parent}")
            children:dict = TARGET_MAP.get(parent,{})
            #logger.info(f"children: {children}")
            children_to_process = [k for k in children.keys() if k in processing_queue]
            #logger.info(f"children to process: {children_to_process}")
            for child in children_to_process:
                #logger.info(f"child:{child}")
                processed_parents:pd.DataFrame = self._process_data_link(network,station,survey,target=child,source=[parent],override=override)
                # Check if all children of this parent have been processed
                if processed_parents is not None:
                    for entry in processed_parents.itertuples(index=True):
                        self.catalog_data.at[entry.Index, "processed"] = self.catalog_data[
                            (self.catalog_data.source_uuid == entry.uuid)
                            & (self.catalog_data.type.isin([x.value for x in children.keys()]))
                        ].shape[0] == len(children)
                    self.catalog_data.to_csv(self.catalog,index=False)
                else:
                    response = f"All available instances of processing type {parent.value} to type {child.value} have been processed"
                    logger.info(response)
                    print(response)

        #     self._process_data_link(network,station,survey,DATA_TYPE.ACOUSTIC,[FILE_TYPE.SONARDYNE,FILE_TYPE.DFPO00],override=override)
        # self._process_data_link(network,station,survey,DATA_TYPE.ACOUSTIC,[FILE_TYPE.SONARDYNE,FILE_TYPE.DFPO00],override=override)

    def process_acoustic_data(self, network: str, station: str, survey: str,override:bool=False):
        self._process_data_graph(network,station,survey,DATA_TYPE.ACOUSTIC,override=override)

    def process_imu_data(self, network: str, station: str, survey: str,override:bool=False):
        self._process_data_graph(network,station,survey,DATA_TYPE.IMU,override=override)

    def process_rinex(self, network: str, station: str, survey: str,override:bool=False):
        self._process_data_graph(network,station,survey,FILE_TYPE.RINEX,override=override)

    def process_gnss_data_kin(self, network: str, station: str, survey: str,override:bool=False):
        self._process_data_graph(network,station,survey,FILE_TYPE.KIN,override=override)

    def process_gnss_data(self, network: str, station: str, survey: str,override:bool=False):
        self._process_data_graph(network,station,survey,DATA_TYPE.GNSS,override=override)

    def process_siteconfig(self, network: str, station: str, survey: str,override:bool=False):
        self._process_data_graph(network,station,survey,DATA_TYPE.SITECONFIG,override=override)
        self._process_data_graph(network,station,survey,DATA_TYPE.ATDOFFSET,override=override)
        self._process_data_graph(network,station,survey,DATA_TYPE.SVP,override=override)

    def process_campaign_data(
        self, network: str, station: str, survey: str, override: bool = False
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
        self.process_acoustic_data(network,station,survey,override=override)
        self.process_imu_data(network,station,survey,override=override)
        self.process_gnss_data(network,station,survey,override=override)
        self.process_siteconfig(network,station,survey,override=override)

        logger.info(
            f"Network {network} Station {station} Survey {survey} Preprocessing complete"
        )

    def query_catalog(self,
                      network:str,
                      station:str,
                      survey:str,
                      type:List[Union[DATA_TYPE,FILE_TYPE]],
                      year:int,
                      month:int,
                      day:int,
                      hour:int = 0,
                      time_span:datetime.timedelta = datetime.timedelta(hours=12)) -> pd.DataFrame:
        """
        Query the catalog
        """

        if not isinstance(type,list):
            type = [type]

        target_date = datetime.datetime(year,month,day,hour)
        start_date = target_date - time_span
        end_date = target_date + time_span

        entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.timestamp >= start_date)
            & (self.catalog_data.timestamp <= end_date)
            & (self.catalog_data.type.isin([x.value for x in type]))
        ]
        if entries.shape[0] < 1:
            raise Exception('No matching data found in catalog')
        return entries

    def get_observation_session_data(self,network:str,station:str,survey:str,plot:bool=False) -> pd.DataFrame:

        time_groups = self.catalog_data[self.catalog_data.type.isin(['gnss','acoustic','imu'])].groupby('timestamp')
        valid_groups = [group for name, group in time_groups if set(group['type']) == {'gnss', 'acoustic', 'imu'}]
        result = pd.concat(valid_groups)
        primary_colums = ['network','station','survey','timestamp','type','local_location']
        all_columns = list(result.columns)
        for column in primary_colums[::-1]:
            all_columns.remove(column)
            all_columns.insert(0,column)

        result = result[all_columns]
        result = DataCatalog.validate(result)
        times = result.timestamp.unique()
        result.set_index(["network", "station", "survey", "timestamp"], inplace=True)
        result.sort_index(inplace=True)

        if plot:
            fig, ax = plt.subplots(figsize=(16, 2))
            ax.set_title(f"Observable Data Availability For Network: {network} Station: {station} Survey: {survey}")

            ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

            for time in times:
                ax.scatter(pd.to_datetime(time), 1,marker='o',color='green')

            plt.xticks(rotation=45)
            ax.set_xlabel("Timestamp")
            ax.set_yticks([])

            plt.show()

        return result

    def group_observation_session_data(self,data:pd.DataFrame,timespan:str="DAY") -> pd.DataFrame:
        # Create a group of dataframes for each timestamp
        assert timespan in ['HOUR','DAY'], "Timespan must be either 'HOUR' or 'DAY'"
        if timespan == 'HOUR':
            grouper = pd.Grouper(level="timestamp", freq="h")
        else:
            grouper = pd.Grouper(level="timestamp", freq="D")
        out = {}

        obs_types = [DATA_TYPE.IMU.value, DATA_TYPE.GNSS.value, DATA_TYPE.ACOUSTIC.value]
        for timestamp, group in data.groupby(grouper):
            out[timestamp] = {}
            for obs_type in obs_types:
                out[timestamp][obs_type] = list(group[group.type == obs_type].local_location.values)

        # prune empty entries
        out = {str(k):v for k,v in out.items() if any([len(x) > 0 for x in v.values()])}
        return out

    def plot_campaign_data(self,network:str,station:str,survey:str):
        """
        Plot the timestamps and data type for processed IMU,GNSS,and Acoustic data for a given network, station, and survey.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.

        Raises:
            Exception: If no matching data is found in the catalog.
        """

        data_type_to_plot = [DATA_TYPE.IMU.value,DATA_TYPE.GNSS.value,DATA_TYPE.ACOUSTIC.value]

        entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.type.isin(data_type_to_plot))
        ]

        if entries.shape[0] < 1:
            raise Exception('No matching data found in catalog')

        # plot the timestamps and data type for processed IMU,GNSS,and Acoustic data
        cmap = {
            DATA_TYPE.IMU.value: "blue",
            DATA_TYPE.GNSS.value: "green",
            DATA_TYPE.ACOUSTIC.value: "red",
        }

        fig, axes = plt.subplots(3, 1, figsize=(10, 4), sharex=True)

        fig.suptitle(f"Observable Data Availablility For Network: {network} Station: {station} Survey: {survey}")
        # Set the x-axis to display dates
        for ax in axes:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

        data_type_titles = {
            DATA_TYPE.IMU.value: "IMU Data",
            DATA_TYPE.GNSS.value: "GNSS Data",
            DATA_TYPE.ACOUSTIC.value: "Acoustic Data",
        }

        for i, data_type in enumerate(data_type_to_plot):
            data = entries[entries.type == data_type]
            for timestamp in data.timestamp:
                axes[i].axvline(pd.to_datetime(timestamp), color=cmap[data_type], linestyle="-")
            axes[i].set_title(data_type_titles[data_type])
            axes[i].get_yaxis().set_visible(False)  # Hide y-axis values

        # Add x-axis label to the bottom subplot
        axes[-1].set_xlabel("Timestamp")

        plt.tight_layout()
        plt.show()

    def get_site_config(self,network:str,station:str,survey:str) -> proc_schemas.SiteConfig:
        """
        Get the Site Config data for a given network, station, and survey.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.

        Raises:
            Exception: If no matching data is found in the catalog.
        """

        data_type_to_plot = [DATA_TYPE.SITECONFIG.value]

        entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.type.isin(data_type_to_plot))
        ]

        if entries.shape[0] < 1:
            raise Exception('No matching site config data found in catalog')

        # load the site config data
        path = entries.local_location.values[0]
        with open(path, "r") as f:
            site_config = json.load(f)
            site_config_schema = SCHEMA_MAP[DATA_TYPE.SITECONFIG](**site_config)
        return site_config_schema

    def get_svp_data(self,network:str,station:str,survey:str) -> pd.DataFrame:
        """
        Get the Sound Velocity Profile data for a given network, station, and survey.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.

        Raises:
            Exception: If no matching data is found in the catalog.
        """

        entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.type==DATA_TYPE.SVP.value)
        ]

        if entries.shape[0] < 1:
            raise Exception('No matching SVP data found in catalog')

        # load the SVP data
        path = entries.local_location.values[0]
        svp_data = pd.read_csv(path)
        return svp_data

    def get_atd_offset(self,network:str,station:str,survey:str) -> proc_schemas.ATDOffset:
        """
        Get the ATD Offset data for a given network, station, and survey.

        Args:
            network (str): The network name.
            station (str): The station name.
            survey (str): The survey name.

        Raises:
            Exception: If no matching data is found in the catalog.
        """

        entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.type == DATA_TYPE.ATDOFFSET.value)
        ]

        if entries.shape[0] < 1:
            raise Exception('No matching ATD Offset data found in catalog')

        # load the ATD Offset data
        path = entries.local_location.values[0]
        with open(path, "r") as f:
            atd_offset = json.load(f)
            atd_offset_schema = SCHEMA_MAP[DATA_TYPE.ATDOFFSET](**atd_offset)
        return atd_offset_schema

    def plot_site_config(self,site_config:proc_schemas.SiteConfig,zoom:int=5):
        """
        Plot the timestamps and data type for processed Site Config data for a given network, station, and survey.

        """

        map = folium.Map(location=[site_config.position_llh.latitude, site_config.position_llh.longitude], zoom_start=zoom)
        folium.Marker(
            location=[site_config.position_llh.latitude, site_config.position_llh.longitude],
            icon=folium.Icon(color="blue"),
        ).add_to(map)

        for transponder in site_config.transponders:
            folium.Marker(
                location=[transponder.position_llh.latitude, transponder.position_llh.longitude],
                popup=f"Transponder: {transponder.id}",
                icon=folium.Icon(color="red"),
            ).add_to(map)

        # map.save(self.working_dir/f"site_config_{network}_{station}_{survey}.html")
        return map
