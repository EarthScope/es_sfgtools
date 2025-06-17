import logging
from tqdm.auto import tqdm
from multiprocessing import Pool, cpu_count
import warnings
from functools import partial
import pandas as pd
from typing import List, Optional,Union,overload
from pathlib import Path
import multiprocessing
import datetime
import signal
from pydantic import BaseModel, Field, ValidationError,model_serializer,field_serializer,field_validator
import yaml
import concurrent.futures
import numpy as np 

from sfg_metadata.metadata.src.catalogs import Catalog

from es_sfgtools.processing.pipeline.catalog import PreProcessCatalog
from es_sfgtools.processing.assets.file_schemas import AssetEntry,AssetType
from es_sfgtools.processing.operations import sv2_ops, sv3_ops, gnss_ops, site_ops
from es_sfgtools.processing.operations.gnss_ops import PridePdpConfig, rinex_to_kin, kin_to_gnssdf
from es_sfgtools.processing.operations.pride_utils import get_nav_file,get_gnss_products
from es_sfgtools.processing.assets.tiledb import (
    TDBAcousticArray,
    TDBGNSSArray,
    TDBPositionArray,
    TDBShotDataArray,
    TDBGNSSObsArray
)
from es_sfgtools.modeling.garpos_tools.schemas import InversionParams
from es_sfgtools.processing.assets.tiledb import TDBAcousticArray, TDBGNSSArray, TDBPositionArray, TDBShotDataArray, TDBGNSSObsArray
from es_sfgtools.processing.operations.utils import get_merge_signature_shotdata, merge_shotdata_gnss
from es_sfgtools.utils.loggers import ProcessLogger as logger, GNSSLogger as gnss_logger   


class NovatelConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    n_processes: int = Field(default_factory=cpu_count, title="Number of Processes to Use")

class RinexConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    n_processes: int = Field(default_factory=cpu_count, title="Number of Processes to Use")
    settings_path: Optional[Path] = Field("", title="Settings Path")
    time_interval: Optional[int] = Field(1, title="Tile to Rinex Time Interval [h]")
    processing_year: Optional[int] = Field(default=-1,description="Processing year to query tiledb",le=2100)
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

class PositionUpdateConfig(BaseModel):
    override: bool = Field(False, title="Flag to Override Existing Data")
    lengthscale: float = Field(2.0, title="Length Scale for Interpolation in seconds")
    plot: bool = Field(False)


class SV3PipelineConfig(BaseModel):
    pride_config: PridePdpConfig = PridePdpConfig()
    novatel_config: NovatelConfig = NovatelConfig()
    rinex_config: RinexConfig = RinexConfig()
    dfop00_config: DFOP00Config = DFOP00Config()
    position_update_config: PositionUpdateConfig = PositionUpdateConfig()

    class Config:
        title = "SV3 Pipeline Configuration"
        arbitrary_types_allowed = True


    def to_yaml(self,filepath:Path):
        with open(filepath,"w") as f:
            yaml.dump(self.model_dump(),f)

    @classmethod
    def load_yaml(cls,filepath:Path):
        with open(filepath) as f:
            data = yaml.load(f)
        return cls(**data)

class PrepSiteData(BaseModel):
    network: str = Field(..., title="Network Name")
    station: str = Field(..., title="Station Name")
    campaign: str = Field(..., title="Campaign Name")
    inter_dir: Path = Field(..., title="Intermediate Directory")
    pride_dir: Path = Field(..., title="Pride Directory")
    rangea_data_dest:str|Path = Field(..., title="GNSS Data Destination")
    gnss_data_dest:str|Path = Field(..., title="GNSS Data Destination")
    shot_data_dest:str|Path = Field(..., title="Shot Data Destination")

    class Config:
        arbitrary_types_allowed = True

    @field_serializer("inter_dir","rangea_data_dest","gnss_data_dest","shot_data_dest","pride_dir")
    def _s_path(self,v):
        if isinstance(v,Path):
            return str(v)
        return v
    @field_validator("inter_dir","pride_dir")
    def _v_path(cls,v:str|Path):
        if isinstance(v,str):
            return Path(v)
        return v

class SV3Pipeline:

    def __init__(self,
                 asset_catalog:PreProcessCatalog=None,
                 data_catalog:Catalog=None,
                 config:SV3PipelineConfig=None):
        
        self.asset_catalog = asset_catalog
        self.data_catalog = data_catalog
        self.config = config

    def set_site_data(self,
                      network:str,
                      station:str,
                      campaign:str,
                      inter_dir:Path,
                      pride_dir:Path) -> None:
        """
        Set the site data for the pipeline.

        Args:
            kwargs: Keyword arguments containing site data.
        """
        self.network = network
        self.station = station
        self.campaign = campaign
        self.inter_dir = inter_dir
        self.pride_dir = pride_dir
        self.rangea_data_dest = TDBGNSSObsArray(self.data_catalog.catalog.networks[network].stations[station].gnssobsdata)
        self.gnss_data_dest = TDBGNSSArray(self.data_catalog.catalog.networks[network].stations[station].gnssdata)
        self.shot_data_dest = TDBShotDataArray(self.data_catalog.catalog.networks[network].stations[station].shotdata)
        self.shot_data_pre = TDBShotDataArray(self.data_catalog.catalog.networks[network].stations[station].shotdata_pre)

    def pre_process_novatel(
        self
    ) -> None:
        novatel_770_entries: List[AssetEntry] = self.asset_catalog.get_local_assets(
            network=self.network,
            station=self.station,
            campaign=self.campaign,
            type=AssetType.NOVATEL770,
        )
        if novatel_770_entries:
            gnss_logger.loginfo(f"Processing {len(novatel_770_entries)} Novatel 770 files for {self.network} {self.station} {self.campaign}. This may take a few minutes...")
            merge_signature = {
                "parent_type": AssetType.NOVATEL770.value,
                "child_type": AssetType.RANGEATDB.value,
                "parent_ids": [x.id for x in novatel_770_entries],
            }
            if self.config.novatel_config.override or not self.asset_catalog.is_merge_complete(**merge_signature):
                gnss_ops.novb2tile(files=novatel_770_entries,rangea_tdb=self.rangea_data_dest.uri,n_procs=self.config.novatel_config.n_processes)

                self.asset_catalog.add_merge_job(**merge_signature)
                response = f"Added merge job for {len(novatel_770_entries)} Novatel 770 Entries to the catalog"
                gnss_logger.loginfo(response)
                # if self.config.novatel_config.show_details:
                #     print(response)
            else:
                response = f"Novatel 770 Data Already Processed for {self.network} {self.station} {self.campaign}"
                gnss_logger.loginfo(response)
        else:
            gnss_logger.loginfo(f"No Novatel 770 Files Found to Process for {self.network} {self.station} {self.campaign}")

        gnss_logger.loginfo(f"Processing Novatel 000 data for {self.network} {self.station} {self.campaign}")
        novatel_000_entries: List[AssetEntry] = self.asset_catalog.get_local_assets(
            network=self.network,
            station=self.station,
            campaign=self.campaign,
            type=AssetType.NOVATEL000,
        )

        if novatel_000_entries:
            merge_signature = {
                "parent_type": AssetType.NOVATEL000.value,
                "child_type": AssetType.RANGEATDB.value,
                "parent_ids": [x.id for x in novatel_000_entries],
            }
            if self.config.novatel_config.override or not self.asset_catalog.is_merge_complete(**merge_signature):
                gnss_ops.nov0002tile(files=novatel_000_entries,rangea_tdb=self.rangea_data_dest.uri,n_procs=self.config.novatel_config.n_processes)

                self.asset_catalog.add_merge_job(**merge_signature)
                gnss_logger.loginfo(f"Added merge job for {len(novatel_000_entries)} Novatel 000 Entries to the catalog")
                # if self.config.novatel_config.show_details:
                #     print(response) # TODO: should the logger handle this?
        else:
            gnss_logger.loginfo(f"No Novatel 000 Files Found to Process for {self.network} {self.station} {self.campaign}")                             
            return

    def get_rinex_files(self) -> None:
        self.rangea_data_dest.consolidate()

        if self.config.rinex_config.processing_year != -1:
            year = self.config.rinex_config.processing_year
        else:
            year = int(self.campaign.split('_')[0])  # default to the year from the campaign name

        gnss_logger.loginfo(f"Generating Rinex Files for {self.network} {self.station} {year}. This may take a few minutes...")
        parent_ids = f"N-{self.network}|ST-{self.station}|SV-{self.campaign}|TDB-{self.rangea_data_dest.uri}|YEAR-{year}"
        merge_signature = {
            "parent_type": AssetType.RANGEATDB.value,
            "child_type": AssetType.RINEX.value,
            "parent_ids": [parent_ids],
        }

        if self.config.rinex_config.override or not self.asset_catalog.is_merge_complete(**merge_signature):
            rinex_entries: List[AssetEntry] = gnss_ops.tile2rinex(
                rangea_tdb=self.rangea_data_dest.uri,
                settings=self.config.rinex_config.settings_path,
                writedir=self.inter_dir,
                time_interval=self.config.rinex_config.time_interval,
                processing_year=year # TODO pass down

            )
            if len(rinex_entries) == 0:
                gnss_logger.loginfo(f"No Rinex Files generated for {self.network} {self.station} {self.campaign} {year}.")
                return

            self.asset_catalog.add_merge_job(**merge_signature)
            #Sort the rinex entries by date so span log is correct
            rinex_entries.sort(key=lambda entry: entry.timestamp_data_start)

            gnss_logger.loginfo(f"Generated {len(rinex_entries)} Rinex files spanning {rinex_entries[0].timestamp_data_start} to {rinex_entries[-1].timestamp_data_end}")
            uploadCount = 0
            for rinex_entry in rinex_entries:
                rinex_entry.network = self.network
                rinex_entry.station = self.station
                rinex_entry.campaign = self.campaign
                if self.asset_catalog.add_entry(rinex_entry):
                    uploadCount += 1
            gnss_logger.loginfo(f"Added {uploadCount} out of {len(rinex_entries)} Rinex files to the catalog")
        else:
            rinex_entries = self.asset_catalog.get_local_assets(self.network,self.station,self.campaign,AssetType.RINEX)
            num_rinex_entries = len(rinex_entries)
            gnss_logger.loginfo(f"RINEX files have already been generated for {self.network}, {self.station}, and {year} Found {num_rinex_entries} entries.")

    def process_rinex(self) -> None:
        """
        Process Rinex Data.

        Raises:
            ValueError: If no Rinex files are found.
        """

        response = (f"Running PRIDE-PPPAR on Rinex Data for {self.network} {self.station} {self.campaign}. This may take a few minutes...")
        gnss_logger.loginfo(response)

        rinex_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                campaign=self.campaign,
                parent_type=AssetType.RINEX,
                child_type=AssetType.KIN,
                override=self.config.pride_config.override,
            )
        )
        if not rinex_entries:
            response = f"No Rinex Files Found to Process for {self.network} {self.station} {self.campaign}"
            gnss_logger.logerr(response)
            return

        response = f"Found {len(rinex_entries)} Rinex Files to Process"
        gnss_logger.loginfo(response)

        '''
        Get the PRIDE GNSS files for each unique DOY
        '''
        get_nav_file_partial = partial(
            get_nav_file, override=self.config.pride_config.override_products_download
        )
        get_gnss_products_partial = partial(
            get_gnss_products, pride_dir=self.pride_dir, override=self.config.pride_config.override_products_download
        )

        rinex_paths = [x.local_path for x in rinex_entries]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            nav_files = [x for x in executor.map(get_nav_file_partial, rinex_paths)]
            gnss_products = [x for x in executor.map(get_gnss_products_partial, rinex_paths)]

        process_rinex_partial = partial(
            rinex_to_kin,
            writedir=self.inter_dir,
            pridedir=self.pride_dir,
            site = self.station,
            pride_config=self.config.pride_config,
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
                    count += 1
                if self.asset_catalog.add_or_update(kinfile):
                    uploadCount += 1

                if resfile is not None:
                    count += 1
                    if self.asset_catalog.add_or_update(resfile):
                        uploadCount += 1
                        resfile_entries.append(resfile)
                rinex_entries[idx].is_processed = True
                self.asset_catalog.add_or_update(rinex_entries[idx])

        response = f"Generated {count} Kin Files From {len(rinex_entries)} Rinex Files, Added {uploadCount} to the Catalog"
        gnss_logger.loginfo(response)

    def process_kin(self):
        gnss_logger.loginfo(f"Looking for Kin Files to Process for {self.network} {self.station} {self.campaign}")
        kin_entries: List[AssetEntry] = self.asset_catalog.get_single_entries_to_process(
            network=self.network,
            station=self.station,
            campaign=self.campaign,
            parent_type=AssetType.KIN,
            override=self.config.rinex_config.override,
        )
        res_entries: List[AssetEntry] = self.asset_catalog.get_single_entries_to_process(
            network=self.network,
            station=self.station,
            campaign=self.campaign,
            parent_type=AssetType.KINRESIDUALS,
            override=self.config.rinex_config.override,
        )
        if not kin_entries:
            response = f"No Kin Files Found to Process for {self.network} {self.station} {self.campaign}"
            gnss_logger.logerr(response)
            return

        response = f"Found {len(kin_entries)} Kin Files to Process: processing"
        gnss_logger.loginfo(response)

        count = 0
        uploadCount = 0
        for kin_entry in tqdm(kin_entries, total=len(kin_entries), desc="Processing Kin Files"):
            if not kin_entry.local_path.exists():
                self.asset_catalog.delete_entry(kin_entry)
                continue
            gnss_df = gnss_ops.kin_to_gnssdf(kin_entry)
            if gnss_df is not None:
                count += 1
                kin_entry.is_processed = True
                self.asset_catalog.add_or_update(kin_entry)
                self.gnss_data_dest.write_df(gnss_df)           

        response = f"Generated {count} GNSS Dataframes From {len(kin_entries)} Kin Files, Added {uploadCount} to the Catalog"
        gnss_logger.loginfo(response)

    def process_dfop00(self) -> None:

        # TODO need a way to mark the dfopoo files as processed
        dfop00_entries: List[AssetEntry] = self.asset_catalog.get_single_entries_to_process(
            network=self.network,
            station=self.station,
            campaign=self.campaign,
            parent_type=AssetType.DFOP00,
            override=self.config.dfop00_config.override,
        )
        if not dfop00_entries:
            response = f"No DFOP00 Files Found to Process for {self.network} {self.station} {self.campaign}"
            logger.logerr(response)
            return

        response = f"Found {len(dfop00_entries)} DFOP00 Files to Process"
        logger.loginfo(response)
        count = 0

        with multiprocessing.Pool() as pool:
            results = pool.imap(sv3_ops.dev_dfop00_to_shotdata, dfop00_entries)
            for shotdata_df,dfo_entry in tqdm(
                zip(results,dfop00_entries), total=len(dfop00_entries), desc="Processing DFOP00 Files"
            ):
                if shotdata_df is not None and not shotdata_df.empty:
                    self.shot_data_pre.write_df(shotdata_df) # write to pre-shotdata
                    count += 1
                    dfo_entry.is_processed = True
                    self.asset_catalog.add_or_update(dfo_entry)
                    logger.logdebug(f"Processed {dfo_entry.local_path}")
                else:
                    logger.logerr(f"Failed to Process {dfo_entry.local_path}")

        response = f"Generated {count} ShotData dataframes From {len(dfop00_entries)} DFOP00 Files"
        logger.loginfo(response)

    def update_shotdata(self):
        logger.loginfo("Updating shotdata with interpolated gnss data")
        # TODO Need to only update positions for a single shot and not each transponder
        # For each shotdata multiasset entry, update the shotdata position with gnss data
        try:
            merge_signature, dates = get_merge_signature_shotdata(
                self.shot_data_pre, self.gnss_data_dest
            )
        except Exception as e:
            logger.logerr(e)
            return
        merge_job = {
            "parent_type": AssetType.GNSS.value,
            "child_type": AssetType.SHOTDATA.value,
            "parent_ids": merge_signature,
        }
        if not self.asset_catalog.is_merge_complete(**merge_job) or self.config.position_update_config.override:
            dates.append(dates[-1]+datetime.timedelta(days=1))
            merge_shotdata_gnss(
                shotdata_pre=self.shot_data_pre,shotdata=self.shot_data_dest, gnss=self.gnss_data_dest, dates=dates, lengthscale=self.config.position_update_config.lengthscale,plot=self.config.position_update_config.plot
            )
            self.asset_catalog.add_merge_job(**merge_job)

    def run_pipeline(self):
        self.pre_process_novatel()
        self.get_rinex_files()
        self.process_rinex()
        self.process_kin()
        self.process_dfop00()
        self.update_shotdata()

class SV2Pipeline:
    #TODO this doesnt not work yet
    def __init__(self,catalog:PreProcessCatalog=None,config:SV3PipelineConfig=None):
        self.catalog = catalog
        self.config = config
        if self.catalog is None:
            self.catalog = PreProcessCatalog(self.config.catalog_path)

    def process_novatel(self) -> None:

        logger.loginfo(f"Processing Novatel data for {self.network} {self.station} {self.campaign}")
        novatel_entries: List[AssetEntry] = self.catalog.get_assets(
            network=self.network,
            station=self.station,
            campaign=self.campaign,
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
                writedir =self.inter_dir,
                show_details=self.config.novatel_config.show_details,
            )
            uploadCount = 0
            for rinex_entry in rinex_entries:
                if self.catalog.add_entry(rinex_entry):
                    uploadCount += 1
            self.catalog.add_merge_job(**merge_signature)
            response = f"Added {uploadCount} out of {len(rinex_entries)} Rinex Entries to the catalog"
            logger.loginfo(response)
            # if self.config.novatel_config.show_details:
            #     print(response)

