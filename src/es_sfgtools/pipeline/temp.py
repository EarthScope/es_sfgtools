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

warnings.filterwarnings("ignore")
seaborn.set_theme(style="whitegrid")
from es_sfgtools.utils.archive_pull import download_file_from_archive
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
        add_missing_columns=True


class DataCatalog(InputURL):

    local_location: pa.typing.Series[pa.String] = pa.Field(
        description="Local location", default=None,nullable=True
    )
    source_uuid: pa.typing.Series[pa.String] = pa.Field(description="Source identifier",default=None,nullable=True )
    processed: pa.typing.Series[pa.Bool] = pa.Field(description="Child data has been aquired",default=False)
    timestamp_minted: pa.typing.Series[pa.Timestamp] = pa.Field(description="Timestamp of the catalog entry",default=datetime.datetime.now())
    class Config:
        coerce=True
        #add_missing_columns=True

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
        


        self.catalog = self.working_dir/"catalog.csv"
        if self.catalog.exists():
            try:
                catalog_data = pd.read_csv(self.catalog)
                catalog_data.timestamp = pd.to_datetime(catalog_data.timestamp,format="mixed")

                self.catalog_data = DataCatalog.validate(catalog_data)
                #self.consolidate_entries()
            except pd.errors.EmptyDataError:
                # empty dataframe
                self.catalog_data = pd.DataFrame()

        else:
            self.catalog_data = pd.DataFrame()
            self.catalog_data.to_csv(self.catalog,index=False)

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

    def load_catalog_from_csv(self):
        self.consolidate_entries()
        self.catalog_data = pd.read_csv(self.catalog, parse_dates=['timestamp'])
        self.catalog_data['timestamp'] = self.catalog_data['timestamp'].astype('datetime64[ns]')

    def get_local_counts(self):
        try:
            self.load_catalog_from_csv()
            local_files = self.catalog_data[self.catalog_data['local_location'].notnull()]
            data_type_counts = local_files.type.value_counts()
        except (AttributeError, KeyError, FileNotFoundError):
            data_type_counts = pd.Series()   
        return data_type_counts

    def get_dtype_counts(self):
        try:
            data_type_counts = self.catalog_data[self.catalog_data.type.isin(FILE_TYPES)].type.value_counts()
        except AttributeError:
            data_type_counts = "No data types found"    
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
                "uuid": uuid.uuid4().hex,
                "network": network,
                "station": station,
                "survey": survey,
                "local_location": str(file),
                "type": discovered_file_type,
                "timestamp": None,
                "processed": False
            }
            file_data_list.append(file_data)
            count += 1
        data = DataCatalog(pd.DataFrame(file_data_list))
        self.update_entry(data)

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
                raise ValueError(f"File type not recognized for {file}")

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
                               file_type: str="all",
                               override:bool=False,
                               from_s3:bool=False,
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
        ]
        if file_type != "all":
            entries = entries[entries.type == file_type]
        missing_files = entries.shape[0]-local_files_of_type
        logger.info(f"Downloading {missing_files} missing files of type {file_type}")
        if entries.shape[0] < 1:
            raise Exception('No matching data found in catalog')
        if from_s3:
            client = boto3.client('s3')
        count = 0
        for entry in tqdm(entries.itertuples(index=True), total=missing_files):
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
                        remote_url=entry.remote_filepath, destination_dir=self.raw_dir, show_details=show_details
                    )

                if is_download:
                    # Check if we can find the file
                    assert local_location.exists(), "Downloaded file not found"
                    self.catalog_data.at[entry.Index,"local_location"] = str(local_location)
                    count += 1
                    # add a duplicate entry but with local_location
                    entry_dict = self.catalog_data[self.catalog_data.index==entry.Index].to_dict('records')[0]
                    entry_dict['local_location'] = str(local_location)
                    self.update_entry(entry_dict)
                else:
                    raise Warning(f'File not downloaded to {str(local_location)}')
        if count == 0:
            response = f"No files downloaded"
            logger.error(response)
        else:
            logger.info(f"Downloaded {count} files")

        # self.catalog_data.to_csv(self.catalog,index=False)

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

        self.load_catalog_from_csv()

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
            # print(response)

        logger.info(f"Downloaded {count} files to {str(self.raw_dir)}")

        self.catalog_data.to_csv(self.catalog,index=False)

    def _download_https(self, 
                        remote_url: Path, 
                        destination_dir: Path, 
                        token_path='.',
                        show_details: bool=True):
        """
        Downloads a file from the specified https url on gage-data

        Args:
            remote_url (Path): The path of the file in the gage-data storage.
            destination (Path): The local path where the file will be downloaded.

        Returns:
            bool: True if the file was downloaded successfully, False otherwise.
        """
        try:
            # local_location = destination_dir / Path(remote_url).name
            download_file_from_archive(url=remote_url, 
                                    dest_dir=destination_dir, 
                                    token_path=token_path,
                                    show_details=show_details)
            # logger.info(f"Downloaded {str(remote_url)} to {str(local_location)}")
            return True
        except Exception as e:
            response = f"Error downloading {str(remote_url)} \n {e}"
            response += "\n HINT: Check authentication credentials"
            logger.error(response)
            # print(response)
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
            # print(response)
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
        # find duplicates

        self.catalog_data = pd.concat(
            [self.catalog_data,DataCatalog.validate(pd.DataFrame([entry]),lazy=True)],
            ignore_index=True
        )
        self.catalog_data.to_csv(str(self.catalog),index=False)

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

    def update_entry(self,entry:Union[dict,pd.DataFrame]):
        """
        Replace an entry in the catalog with a new entry.

        Args:
            entry (dict): The new entry.

        Returns:
            None
        """
        if isinstance(entry, dict):
            old_entry = self.catalog_data[
                (self.catalog_data.uuid == entry["uuid"])
                & (self.catalog_data.type == entry["type"])
                & (self.catalog_data.local_location == entry["local_location"])
                & (self.catalog_data.network == entry["network"])
                & (self.catalog_data.station == entry["station"])
                & (self.catalog_data.survey == entry["survey"])
            ]
            if old_entry.shape[0] > 0:
                for x in old_entry.itertuples(index=True):
                    self.catalog_data.loc[(self.catalog_data.source_uuid == x.uuid),"source_uuid"] = entry["uuid"]
                    self.catalog_data = self.catalog_data.drop(x.Index)
            entry = pd.DataFrame([entry])

        elif isinstance(entry, pd.DataFrame):
            if self.catalog_data.shape[0] > 0:
                # Match against network, station, survey, type, and timestamp
                matched = pd.merge(
                    self.catalog_data,
                    entry,
                    how="right",
                    on=["network", "station", "survey", "type", "local_location"],
                    indicator=True,
                )
                # Get uuid's for new data
                new_data = matched[matched["_merge"] == "right_only"]
                entry = entry[entry.uuid.isin(new_data.uuid_y)]

        # If matched, there will be an "id" field
        if entry.shape[0] > 0:
            entry = DataCatalog.validate(entry, lazy=True)
            logger.info(f"Adding {entry.shape[0]} to the current catalog")
            self.catalog_data = pd.concat([self.catalog_data, entry])

        self.catalog_data.to_csv(self.catalog, index=False)

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
        self, parent: dict, child_type: Union[FILE_TYPE, DATA_TYPE], show_details: bool=False
    ) -> Tuple[dict,dict,bool]:

        # TODO: implement multithreaded logging, had to switch to print statement below
        if isinstance(parent, dict):
            parent = pd.Series(parent)
        # handle the case when the parent timestamp is None
        child_timestamp = parent.timestamp
        response = f"Processing {os.path.basename(parent.local_location)} ({parent.uuid}) of Type {parent.type} to {child_type.value}\n"
        # Get the processing function that converts the parent entry to the child entry
        process_func = TARGET_MAP.get(FILE_TYPE(parent.type)).get(child_type)
        # Build the source object from the parent entry
        source = SCHEMA_MAP[FILE_TYPE(parent.type)](
            location=Path(parent.local_location),
            uuid=parent.uuid,
            capture_time=parent.timestamp,
        )
        # build partial processing function
        match process_func:
            case proc_funcs.rinex_to_kin:
                process_func_p = partial(
                    process_func,
                    writedir=self.inter_dir,
                    pridedir=self.pride_dir,
                    site=parent.station,
                )

            case proc_funcs.novatel_to_rinex:
                process_func_p = partial(
                    process_func,site=parent.station,year=parent.timestamp.year,show_details=show_details
                )

            case proc_funcs.qcpin_to_novatelpin:
                process_func_p = partial(
                    process_func,outpath=self.inter_dir
                )
            case _:
                process_func_p = process_func

        processed = None
        if source.location is not None and source.location.exists():
            if source.location.stat().st_size == 0:
                response += f"File {source.location} is empty\n"
                processed = None
            else:
                processed = process_func_p(source)
    
        child_uuid = uuid.uuid4().hex
        is_processed = True

        match type(processed):
            case pd.DataFrame:
                local_location = self.proc_dir / f"{parent.uuid}_{child_type.value}.csv"
                processed.to_csv(local_location, index=False)

                # handle the case when the child timestamp is None
                if pd.isna(child_timestamp):
                    for col in processed.columns:
                        if pd.api.types.is_datetime64_any_dtype(processed[col]):
                            child_timestamp = processed[col].min()
                            break

            case proc_schemas.RinexFile:
                processed.write(self.inter_dir)
                child_timestamp = pd.to_datetime(processed.start_time)
                local_location = processed.location

            case proc_schemas.KinFile:
                processed.write(self.inter_dir)
                local_location = processed.location

            case proc_schemas.SiteConfig:
                local_location = (
                    self.proc_dir / f"{parent.uuid}_{child_type.value}.json"
                )
                with open(local_location, "w") as f:
                    f.write(processed.model_dump_json())

            case proc_schemas.ATDOffset:
                local_location = (
                    self.proc_dir / f"{parent.uuid}_{child_type.value}.json"
                )
                with open(local_location, "w") as f:
                    f.write(processed.model_dump_json())

            case proc_schemas.NovatelPinFile:
                local_location = (
                    self.inter_dir / f"{parent.uuid}_{child_type.value}.txt"
                )
                processed.location = local_location
                processed.write(dir=local_location.parent)

            case _:
                is_processed = False
                local_location = None
                pass

        if pd.isna(parent.timestamp) and child_timestamp is not None:
            parent.timestamp = child_timestamp
            response += f"Discovered timestamp: {child_timestamp} for parent {parent.type} uuid {parent.uuid}\n"

        processed_meta = {
            "uuid": child_uuid,
            "network": parent.network,
            "station": parent.station,
            "survey": parent.survey,
            "local_location": str(local_location),
            "type": child_type.value,
            "timestamp": child_timestamp,
            "source_uuid": parent.uuid,
            "processed": is_processed,
        }
        logger.info(f"Successful Processing: {str(processed_meta)}")
        return processed_meta,dict(parent),response
        # return None,None,None

    def _process_data_link(self,
                           network:str,
                           station:str,
                           survey:str,
                           target:Union[FILE_TYPE,DATA_TYPE],
                           source:List[FILE_TYPE],
                           override:bool=False,
                           update_timestamp:bool=False,
                           show_details:bool=False) -> pd.DataFrame:
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
            & (self.catalog_data.local_location.notna())
            & (self.catalog_data.type.isin([x.value for x in source]))
        ]

        if parent_entries.shape[0] < 1:
            response = f"No unprocessed data found in catalog for types {[x.value for x in source]}"
            logger.error(response)
            # print(response)
            return
        child_entries = self.catalog_data[
            (self.catalog_data.network == network)
            & (self.catalog_data.station == station)
            & (self.catalog_data.survey == survey)
            & (self.catalog_data.type == target.value)
        ]
        processed_parents_filter = parent_entries.uuid.isin(child_entries.source_uuid)
        to_process_filter = np.logical_or(~processed_parents_filter,override)
        parent_entries_to_process = parent_entries[to_process_filter]

        if parent_entries_to_process.shape[0] > 0:
            logger.info(f"Processing {parent_entries_to_process.shape[0]} Parent Files to {target.value} Data")
            process_func_partial = partial(self._process_targeted,child_type=target,show_details=show_details)

            meta_data_list = []
    
            with multiprocessing.Pool() as pool:
                results = pool.imap_unordered(process_func_partial,parent_entries_to_process.to_dict(orient="records"))
                for meta_data,parent,response in tqdm(results,total=parent_entries_to_process.shape[0],desc=f"Processing {parent_entries_to_process.type.unique()} To {target.value}"):
                    if show_details:
                        print(response)
                        logger.info(response)

                    self.update_entry(parent)
                    self.add_entry(meta_data)
                    meta_data_list.append(meta_data)
       
                # with concurrent.futures.ThreadPoolExecutor() as executor:
                #     futures = [executor.submit(
                #         process_func_partial, parent) 
                #         for parent in parent_entries_to_process.to_dict(orient="records")]
                #     for future in tqdm(
                #         concurrent.futures.as_completed(futures),
                #         total=len(futures),
                #         desc=f"Processing {parent_entries_to_process.type.unique()} To {target.value}"):
                #         meta_data,parent,response = future.result()

                #         if show_details:
                #             print(response)
                #             logger.info(response)

                #         self.update_entry(parent)
                #         self.update_entry(meta_data)
                #         meta_data_list.append(meta_data)

            parent_entries_processed = parent_entries_to_process[
                parent_entries_to_process.uuid.isin(
                    [x["source_uuid"] for x in meta_data_list]
                )
            ]
            logger.info(f"Processed {len(meta_data_list)} Out of {parent_entries_processed.shape[0]} For {target.value} Files from {parent_entries_processed.shape[0]} Parent Files")
            # self.consolidate_entries()
            return parent_entries_processed

    def _process_data_graph(self, 
                            network: str, 
                            station: str, 
                            survey: str,
                            child_type:Union[FILE_TYPE,DATA_TYPE],
                            override:bool=False,
                            show_details:bool=False,
                            update_timestamp:bool=False):
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
                    processed_parents:pd.DataFrame = self._process_data_link(
                        network,
                        station,
                        survey,
                        target=child,
                        source=[parent],
                        override=override,show_details=show_details,update_timestamp=update_timestamp)
                    # Check if all children of this parent have been processed
                    if processed_parents is not None:
                        # self.load_catalog_from_csv()
                        for entry in processed_parents.itertuples(index=True):
                            self.catalog_data.at[entry.Index, "processed"] = self.catalog_data[
                                (self.catalog_data.source_uuid == entry.uuid)
                                & (self.catalog_data.type.isin([x.value for x in children.keys()]))
                            ].shape[0] == len(children)
                        # logger.info("saving data to catalog")
                        # logger.info(self.catalog_data)
                        self.catalog_data.to_csv(self.catalog,index=False)
                    else:
                        response = f"All available instances of processing type {parent.value} to type {child.value} have been processed"
                        logger.info(response)
                        # print(response)

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

    def reset_ts(self):
        for index, row in self.catalog_data.iterrows():
            if ".csv" in row["local_location"]:
                df = pd.read_csv(row["local_location"])
                for col in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        self.catalog_data.at[index, "timestamp"] = df[col].min()
                        break
                break
        self.catalog_data.to_csv(self.catalog,index=False)

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
        valid_groups = [group for name, group in time_groups] #
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

    def group_observation_session_data(self,data:pd.DataFrame,timespan:str="DAY") -> dict:
        # Create a group of dataframes for each timestamp
        assert timespan in ['HOUR','DAY'], "Timespan must be either 'HOUR' or 'DAY'"
        if timespan == 'HOUR':
            grouper = pd.Grouper(key="timestamp", freq="h")
        else:
            grouper = pd.Grouper(key="timestamp", freq="D")
        out = {}

        obs_types = [DATA_TYPE.IMU.value, DATA_TYPE.GNSS.value, DATA_TYPE.ACOUSTIC.value]
        for timestamp, group in data.groupby(grouper):
            if group.shape[0] < 1:
                continue
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
