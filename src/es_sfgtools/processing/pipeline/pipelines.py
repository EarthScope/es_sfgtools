import logging
from tqdm.auto import tqdm
from multiprocessing import Pool, cpu_count
import warnings
from functools import partial
import pandas as pd
from typing import List, Optional
from pathlib import Path
import multiprocessing
import datetime
import signal
from pydantic import BaseModel, Field, ValidationError,model_serializer,field_serializer,field_validator,validator
import yaml
import concurrent.futures

from es_sfgtools.processing.pipeline.catalog import Catalog
from es_sfgtools.processing.assets.file_schemas import AssetEntry,AssetType
from es_sfgtools.processing.operations import sv2_ops, sv3_ops, gnss_ops, site_ops
from es_sfgtools.processing.operations.gnss_ops import PridePdpConfig,rinex_to_kin,kin_to_gnssdf
from es_sfgtools.processing.operations.pride_utils import get_nav_file,get_gnss_products
from es_sfgtools.processing.assets.tiledb_temp import (
    TDBAcousticArray,
    TDBGNSSArray,
    TDBPositionArray,
    TDBShotDataArray,
)
from es_sfgtools.processing.assets.tiledb_temp import TDBAcousticArray,TDBGNSSArray,TDBPositionArray,TDBShotDataArray
from es_sfgtools.processing.operations.utils import (
    get_merge_signature_shotdata,
    merge_shotdata_gnss,
)

from es_sfgtools.utils.loggers import ProcessLogger as logger

class NovatelConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    show_details: bool = Field(False, title="Flag to Show Processing Details")
    n_processes: int = Field(default_factory=cpu_count, title="Number of Processes to Use")

class RinexConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    show_details: bool = Field(False, title="Flag to Show Processing Details")
    pride_config: PridePdpConfig = Field(default_factory=PridePdpConfig, title="Pride Configuration")
    override_products_download: bool = Field(False, title="Flag to Override Existing Products Download")
    n_processes: int = Field(default_factory=cpu_count, title="Number of Processes to Use")
    settings_path: Optional[Path] = Field("", title="Settings Path")
    class Config:
        arbitrary_types_allowed = True
    @field_serializer("settings_path")
    def _s_path(self,v):
        return str(v)
    @field_validator("settings_path")
    def _v_path(cls,v:str):
        return Path(v)

class DFOP00Config(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    show_details: bool = Field(False, title="Flag to Show Processing Details")

class PositionUpdateConfig(BaseModel):
    plot: bool = Field(False, title="Flag to Plot Data")
    override: bool = Field(False, title="Flag to Override Existing Data")

class SV3PipelineConfig(BaseModel):
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    survey: str = Field(..., title="Survey Name")
    inter_dir: Path = Field(..., title="Intermediate Directory")
    pride_dir: Path = Field(..., title="Pride Directory")
    catalog_path: Path = Field(..., title="Catalog Path")

    novatel_config: NovatelConfig = NovatelConfig()
    rinex_config: RinexConfig = RinexConfig()
    dfop00_config: DFOP00Config = DFOP00Config()
    position_update_config: PositionUpdateConfig = PositionUpdateConfig()
    shot_data_dest: TDBShotDataArray = None
    gnss_data_dest: TDBGNSSArray = None
    rangea_data_dest: Path = None

    class Config:
        title = "SV3 Pipeline Configuration"
        arbitrary_types_allowed = True

    @field_serializer("gnss_data_dest","shot_data_dest")
    def _s_shotdata(self,v):
        return str(v.uri)
    # @field_serializer("shot_data_dest")
    # def s_gnsdata(self,v):
    #     return str(v.uri)
    @validator("shot_data_dest")
    def _v_shotdata(cls,v:str|TDBShotDataArray):
        if isinstance(v,str):
            return TDBShotDataArray(Path(v))
        return v

    @field_validator("gnss_data_dest")
    def _v_gnssdata(cls,v:str|TDBGNSSArray):
        if isinstance(v,str):
            return TDBGNSSArray(Path(v))
        return v

    @field_serializer("inter_dir","pride_dir","catalog_path","rangea_data_dest")
    def _s_path(self,v):
        return str(v)

    @field_validator("inter_dir","pride_dir","catalog_path","rangea_data_dest")
    def _v_path(cls,v:str):
        return Path(v)

    def save_yaml(self,filepath:Path):
        with open(filepath,"w") as f:
            yaml.dump(self.model_dump(),f)

    @classmethod
    def load_yaml(cls,filepath:Path):
        with open(filepath) as f:
            data = yaml.load(f)
        return cls(**data)

class SV3Pipeline:
    def __init__(self,catalog:Catalog=None,config:SV3PipelineConfig=None):
        self.catalog = catalog
        self.config = config
        if self.catalog is None:
            self.catalog = Catalog(self.config.catalog_path)

    def pre_process_novatel(
        self
    ) -> None:

        print(f"Processing Novatel 770 data for {self.config.network} {self.config.station} {self.config.survey}")
        novatel_770_entries: List[AssetEntry] = self.catalog.get_assets(
            network=self.config.network,
            station=self.config.station,
            survey=self.config.survey,
            type=AssetType.NOVATEL770,
        )
        if novatel_770_entries:

            merge_signature = {
                "parent_type": AssetType.NOVATEL770.value,
                "child_type": AssetType.RANGEATDB.value,
                "parent_ids": [x.id for x in novatel_770_entries],
            }
            if self.config.novatel_config.override or not self.catalog.is_merge_complete(**merge_signature):
                gnss_ops.novb2tile(files=novatel_770_entries,rangea_tdb=self.config.rangea_data_dest,n_procs=self.config.novatel_config.n_processes)

                self.catalog.add_merge_job(**merge_signature)
                response = f"Added {len(novatel_770_entries)} Novatel 770 Entries to the catalog"
                logger.logger.info(response)
                if self.config.novatel_config.show_details:
                    print(response)
        else:
            response = f"No Novatel 770 Files Found to Process for {self.config.network} {self.config.station} {self.config.survey}"
            logger.logger.info(response)
            print(response)

        print(f"Processing Novatel 000 data for {self.config.network} {self.config.station} {self.config.survey}")
        novatel_000_entries: List[AssetEntry] = self.catalog.get_assets(
            network=self.config.network,
            station=self.config.station,
            survey=self.config.survey,
            type=AssetType.NOVATEL000,
        )
        if novatel_000_entries:

            merge_signature = {
                "parent_type": AssetType.NOVATEL000.value,
                "child_type": AssetType.RANGEATDB.value,
                "parent_ids": [x.id for x in novatel_000_entries],
            }
            if self.config.novatel_config.override or not self.catalog.is_merge_complete(**merge_signature):
                gnss_ops.nov0002tile(files=novatel_000_entries,rangea_tdb=self.config.rangea_data_dest,n_procs=self.config.novatel_config.n_processes)

                self.catalog.add_merge_job(**merge_signature)
                response = f"Added {len(novatel_000_entries)} Novatel 000 Entries to the catalog"
                logger.logger.info(response)
                if self.config.novatel_config.show_details:
                    print(response)
        else:
            response = f"No Novatel 000 Files Found to Process for {self.config.network} {self.config.station} {self.config.survey}"
            logger.logger.info(response)
            print(response)
            return

    def get_rinex_files(self):
        merge_signature = {
            "parent_type": AssetType.RANGEATDB.value,
            "child_type": AssetType.RINEX.value,
            "parent_ids": [self.config.rangea_data_dest],
        }

        if self.config.rinex_config.override or not self.catalog.is_merge_complete(**merge_signature):
            rinex_entries: List[AssetEntry] = gnss_ops.tile2rinex(
                rangea_tdb=self.config.rangea_data_dest,
                settings=self.config.rinex_config.settings_path,
                writedir=self.config.inter_dir,
                n_procs=self.config.rinex_config.n_processes,
            )
            if len(rinex_entries) == 0:
                response = f"No Rinex Files Found to Process for {self.config.network} {self.config.station} {self.config.survey}"
                logger.logger.error(response)
                return
            self.catalog.add_merge_job(**merge_signature)
            logger.logger.info(f"Generated {len(rinex_entries)} Rinex Entries spanning {rinex_entries[0].timestamp_data_start} to {rinex_entries[-1].timestamp_data_end}")
            uploadCount = 0
            for rinex_entry in rinex_entries:
                rinex_entry.network = self.config.network
                rinex_entry.station = self.config.station
                rinex_entry.survey = self.config.survey
                if self.catalog.add_entry(rinex_entry):
                    uploadCount += 1
            logger.logger.info(f"Added {uploadCount} out of {len(rinex_entries)} Rinex Entries to the catalog")

    def process_rinex(
        self
    ) -> None:
        """
        Process Rinex Data.
        Args:
            override (bool, optional): Flag to override existing data. Defaults to False.
            show_details (bool, optional): Flag to show processing details. Defaults to False.
        Raises:
            ValueError: If no Rinex files are found.
        """

        response = (
            f"Processing Rinex Data for {self.config.network} {self.config.station} {self.config.survey}"
        )
        logger.logger.info(response)
        if self.config.rinex_config.show_details:
            print(response)

        rinex_entries: List[AssetEntry] = (
            self.catalog.get_single_entries_to_process(
                network=self.config.network,
                station=self.config.station,
                survey=self.config.survey,
                parent_type=AssetType.RINEX,
                child_type=AssetType.KIN,
                override=self.config.rinex_config.override,
            )
        )
        if not rinex_entries:
            response = f"No Rinex Files Found to Process for {self.config.network} {self.config.station} {self.config.survey}"
            logger.logger.error(response)
            if self.config.rinex_config.show_details:
                print(response)
            warnings.warn(response)
            return []

        response = f"Found {len(rinex_entries)} Rinex Files to Process"
        logger.logger.info(response)
        if self.config.rinex_config.show_details:
            print(response)

        get_nav_file_partial = partial(
            get_nav_file, override=self.config.rinex_config.override_products_download
        )
        get_gnss_products_partial = partial(
            get_gnss_products, pride_dir=self.config.pride_dir, override=self.config.rinex_config.override_products_download
        )

        rinex_paths = [x.local_path for x in rinex_entries]
      
        with concurrent.futures.ThreadPoolExecutor() as executor:

            #for rinex_entry in tqdm(rinex_entries,total=len(rinex_entries),desc="Getting nav/obs files for Processing Rinex Files"):
            nav_files = [x for x in executor.map(get_nav_file_partial, rinex_paths)]
            gnss_products = [x for x in executor.map(get_gnss_products_partial, rinex_paths)]

        #print(gnss_products)
            
        process_rinex_partial = partial(
            rinex_to_kin,
            writedir=self.config.inter_dir,
            pridedir=self.config.pride_dir,
            site = self.config.station,
            show_details=self.config.rinex_config.show_details,
            pride_config=self.config.rinex_config.pride_config,
        )
        kin_entries = []
        resfile_entries = []
        count = 0
        uploadCount = 0

        with multiprocessing.Pool(processes=self.config.rinex_config.n_processes) as pool:

            results = pool.map(process_rinex_partial, rinex_entries)
    

            for idx, (kinfile, resfile) in enumerate(tqdm(
                results, total=len(rinex_entries), desc="Processing Rinex Files",mininterval=0.5
            )):
                if kinfile is not None:
                    kin_entries.append(kinfile)
                    if resfile is not None:
                        resfile_entries.append(resfile)

        for idx, (kinfile, resfile) in enumerate(zip(kin_entries, resfile_entries)):
            if kinfile is not None:
                count += 1
            if self.catalog.add_or_update(kinfile):
                uploadCount += 1
            
            if resfile is not None:
                count += 1
                if self.catalog.add_or_update(resfile):
                    uploadCount += 1
                    resfile_entries.append(resfile)
            rinex_entries[idx].is_processed = True
            self.catalog.add_or_update(rinex_entries[idx])
            

        response = f"Generated {count} Kin Files From {len(rinex_entries)} Rinex Files, Added {uploadCount} to the Catalog"
        logger.logger.info(response)
        if self.config.rinex_config.show_details:
            print(response)

    def process_kin(self):
        kin_entries: List[AssetEntry] = self.catalog.get_single_entries_to_process(
            network=self.config.network,
            station=self.config.station,
            survey=self.config.survey,
            parent_type=AssetType.KIN,
            override=self.config.rinex_config.override,
        )
        res_entries: List[AssetEntry] = self.catalog.get_single_entries_to_process(
            network=self.config.network,
            station=self.config.station,
            survey=self.config.survey,
            parent_type=AssetType.KINRESIDUALS,
            override=self.config.rinex_config.override,
        )
        if not kin_entries:
            response = f"No Kin Files Found to Process for {self.config.network} {self.config.station} {self.config.survey}"
            logger.logger.error(response)
            if self.config.rinex_config.show_details:
                print(response)
            warnings.warn(response)
            return

        response = f"Found {len(kin_entries)} Kin Files to Process"
        logger.logger.info(response)
        if self.config.rinex_config.show_details:
            print(response)

        count = 0
        uploadCount = 0
        for kin_entry in tqdm(kin_entries, total=len(kin_entries), desc="Processing Kin Files"):
            if not kin_entry.local_path.exists():
                self.catalog.delete_entry(kin_entry)
                continue
            gnss_df = gnss_ops.kin_to_gnssdf(kin_entry)
            if gnss_df is not None:
                count += 1
                kin_entry.is_processed = True
                self.catalog.add_or_update(kin_entry)

                self.config.gnss_data_dest.write_df(gnss_df)           

        response = f"Generated {count} GNSS Dataframes From {len(kin_entries)} Kin Files, Added {uploadCount} to the Catalog"
        logger.logger.info(response)
        if self.config.rinex_config.show_details:
            print(response)

    def process_dfop00(
        self
    ) -> None:

        # TODO need a way to mark the dfopoo files as processed
        dfop00_entries: List[AssetEntry] = self.catalog.get_single_entries_to_process(
            network=self.config.network,
            station=self.config.station,
            survey=self.config.survey,
            parent_type=AssetType.DFOP00,
            override=self.config.dfop00_config.override,
        )
        if not dfop00_entries:
            response = f"No DFOP00 Files Found to Process for {self.config.network} {self.config.station} {self.config.survey}"
            logger.logger.error(response)
            if self.config.dfop00_config.show_details:
                print(response)
            warnings.warn(response)
            return

        response = f"Found {len(dfop00_entries)} DFOP00 Files to Process"
        logger.logger.info(response)
        if self.config.dfop00_config.show_details:
            print(response)

        count = 0

        with multiprocessing.Pool() as pool:
            results = pool.imap(sv3_ops.dev_dfop00_to_shotdata, dfop00_entries)
            for shotdata_df,dfo_entry in tqdm(
                zip(results,dfop00_entries), total=len(dfop00_entries), desc="Processing DFOP00 Files"
            ):
                if shotdata_df is not None:
                    self.config.shot_data_dest.write_df(shotdata_df)
                    count += 1
                    dfo_entry.is_processed = True
                    self.catalog.add_or_update(dfo_entry)

        response = f"Generated {count} ShotData dataframes From {len(dfop00_entries)} DFOP00 Files"
        logger.logger.info(response)
        if self.config.dfop00_config.show_details:
            print(response)

    def update_shotdata(self):
        print("Updating shotdata with interpolated gnss data")
        # TODO Need to only update positions for a single shot and not each transponder
        # For each shotdata multiasset entry, update the shotdata position with gnss data
        try:
            merge_signature, dates = get_merge_signature_shotdata(
                self.config.shot_data_dest, self.config.gnss_data_dest
            )
        except Exception as e:
            print(e)
            return
        merge_job = {
            "parent_type": AssetType.GNSS.value,
            "child_type": AssetType.SHOTDATA.value,
            "parent_ids": merge_signature,
        }
        if not self.catalog.is_merge_complete(**merge_job) or self.config.position_update_config.override:
            dates.append(dates[-1]+datetime.timedelta(days=1))
            merge_shotdata_gnss(
                shotdata=self.config.shot_data_dest, gnss=self.config.gnss_data_dest, dates=dates, plot=self.config.position_update_config.plot
            )
            self.catalog.add_merge_job(**merge_job)

    def run_pipeline(self):
        self.pre_process_novatel()
        self.get_rinex_files()
        self.process_rinex()
        self.process_kin()
        self.process_dfop00()
        self.update_shotdata()

class SV2Pipeline:
    def __init__(self,catalog:Catalog=None,config:SV3PipelineConfig=None):
        self.catalog = catalog
        self.config = config
        if self.catalog is None:
            self.catalog = Catalog(self.config.catalog_path)

    def process_novatel(
        self
    ) -> None:

        print(f"Processing Novatel data for {self.config.network} {self.config.station} {self.config.survey}")
        novatel_entries: List[AssetEntry] = self.catalog.get_assets(
            network=self.config.network,
            station=self.config.station,
            survey=self.config.survey,
            type=AssetType.NOVATEL,
        )

        merge_signature = {
            "parent_type": AssetType.NOVATEL.value,
            "child_type": AssetType.RINEX.value,
            "parent_ids": [x.id for x in novatel_entries],
        }
        if self.config.novatel_config.override or not self.catalog.is_merge_complete(**merge_signature):
            rinex_entries: List[AssetEntry] = gnss_ops.novatel_to_rinex_batch(
                source=novatel_770_entries,
                writedir =self.config.inter_dir,
                show_details=self.config.novatel_config.show_details,
            )
            uploadCount = 0
            for rinex_entry in rinex_entries:
                if self.catalog.add_entry(rinex_entry):
                    uploadCount += 1
            self.catalog.add_merge_job(**merge_signature)
            response = f"Added {uploadCount} out of {len(rinex_entries)} Rinex Entries to the catalog"
            logger.logger.info(response)
            if self.config.novatel_config.show_details:
                print(response)
