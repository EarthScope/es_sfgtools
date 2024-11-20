import logging
from tqdm.auto import tqdm
from multiprocessing import Pool, cpu_count
import warnings
from functools import partial
import pandas as pd
from typing import List
from pathlib import Path
import multiprocessing
import datetime

from es_sfgtools.processing.pipeline.catalog import Catalog
from es_sfgtools.processing.assets.file_schemas import AssetEntry,AssetType
from es_sfgtools.processing.operations import sv2_ops, sv3_ops, gnss_ops, site_ops
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

logger = logging.getLogger(__name__)

class SV3Pipeline:
    def __init__(self,catalog:Catalog):
        self.catalog = catalog

    def process_novatel(
        self, network:str,station:str,survey:str,writedir:Path,override: bool = False, show_details: bool = False
    ) -> List[AssetEntry]:

        print(f"Processing Novatel 770 data for {network} {station} {survey}")
        novatel_770_entries: List[AssetEntry] = self.catalog.get_assets(
            network=network,
            station=station,
            survey=survey,
            type=AssetType.NOVATEL770,
        )

        merge_signature = {
            "parent_type": AssetType.NOVATEL770.value,
            "child_type": AssetType.RINEX.value,
            "parent_ids": [x.id for x in novatel_770_entries],
        }
        if override or not self.catalog.is_merge_complete(**merge_signature):
            rinex_entries: List[AssetEntry] = gnss_ops.novatel_to_rinex_batch(
                source=novatel_770_entries,
                writedir =writedir,
                show_details=show_details,
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

    def process_rinex(
        self,
        network: str,
        station: str,
        survey: str,
        inter_dir: Path,
        pride_dir: Path,
        override: bool = False,
        show_details: bool = False,
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
            f"Processing Rinex Data for {network} {station} {survey}"
        )
        logger.info(response)
        if show_details:
            print(response)

        rinex_entries: List[AssetEntry] = (
            self.catalog.get_single_entries_to_process(
                network=network,
                station=station,
                survey=survey,
                parent_type=AssetType.RINEX,
                child_type=AssetType.KIN,
                override=override,
            )
        )
        if not rinex_entries:
            response = f"No Rinex Files Found to Process for {network} {station} {survey}"
            logger.error(response)
            if show_details:
                print(response)
            warnings.warn(response)
            return []

        response = f"Found {len(rinex_entries)} Rinex Files to Process"
        logger.info(response)
        if show_details:
            print(response)
        
        for rinex_entry in tqdm(rinex_entries,total=len(rinex_entries),desc="Getting nav/obs files for Processing Rinex Files"):
            nav_file: Path = get_nav_file(rinex_path=rinex_entry.local_path, override=override)
            product_status: dict = get_gnss_products(rinex_path=rinex_entry.local_path, pride_dir=pride_dir, override=override)
            if show_details:
                print(f"\nProduct Status: {product_status}\n")
                print(f"\nNav File: {str(nav_file)}\n")

        process_rinex_partial = partial(
            gnss_ops.rinex_to_kin,
            writedir=inter_dir,
            pridedir=pride_dir,
            show_details=show_details,
        )
        kin_entries = []
        resfile_entries = []
        count = 0
        uploadCount = 0
        with multiprocessing.Pool() as pool:
            results = pool.imap(process_rinex_partial, rinex_entries)
            for idx,(kinfile, resfile) in enumerate(tqdm(
                results, total=len(rinex_entries), desc="Processing Rinex Files"
            )):
                if kinfile is not None:
                    count += 1
                    if self.catalog.add_or_update(kinfile):
                        uploadCount += 1
                    kin_entries.append(kinfile)
                    if resfile is not None:
                        count += 1
                        if self.catalog.add_or_update(resfile):
                            uploadCount += 1
                        resfile_entries.append(resfile)
                    rinex_entries[idx].is_processed = True
                    self.catalog.add_or_update(rinex_entries[idx])
                   
        response = f"Generated {count} Kin Files From {len(rinex_entries)} Rinex Files, Added {uploadCount} to the Catalog"
        logger.info(response)
        if show_details:
            print(response)

    def process_kin(self, network:str,station:str,survey:str,gnss_tdb: TDBGNSSArray,override: bool = False, show_details: bool = False):
        kin_entries: List[AssetEntry] = self.catalog.get_single_entries_to_process(
            network=network,
            station=station,
            survey=survey,
            parent_type=AssetType.KIN,
            override=override,
        )
        res_entries: List[AssetEntry] = self.catalog.get_single_entries_to_process(
            network=network,
            station=station,
            survey=survey,
            parent_type=AssetType.KINRESIDUALS,
            override=override,
        )
        if not kin_entries:
            response = f"No Kin Files Found to Process for {network} {station} {survey}"
            logger.error(response)
            if show_details:
                print(response)
            warnings.warn(response)
            return

        
        response = f"Found {len(kin_entries)} Kin Files to Process"
        logger.info(response)
        if show_details:
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

                gnss_tdb.write_df(gnss_df)           

        response = f"Generated {count} GNSS Dataframes From {len(kin_entries)} Kin Files, Added {uploadCount} to the Catalog"
        logger.info(response)
        if show_details:
            print(response)

    def process_dfop00(
        self, network:str,station:str,survey:str,shotdatadest:TDBShotDataArray, override: bool = False, show_details: bool = False
    ) -> None:
        

        #TODO need a way to mark the dfopoo files as processed
        dfop00_entries: List[AssetEntry] = self.catalog.get_single_entries_to_process(
            network=network,
            station=station,
            survey=survey,
            parent_type=AssetType.DFOP00,
            override=override,
        )
        if not dfop00_entries:
            response = f"No DFOP00 Files Found to Process for {network} {station} {survey}"
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
            for shotdata_df,dfo_entry in tqdm(
                zip(results,dfop00_entries), total=len(dfop00_entries), desc="Processing DFOP00 Files"
            ):
                if shotdata_df is not None:
                    shotdatadest.write_df(shotdata_df)
                    count += 1
                    dfo_entry.is_processed = True
                    self.catalog.add_or_update(dfo_entry)

        response = f"Generated {count} ShotData dataframes From {len(dfop00_entries)} DFOP00 Files"
        logger.info(response)
        if show_details:
            print(response)

    def update_shotdata(self,shotdatasource:TDBShotDataArray,gnssdatasource:TDBGNSSArray, plot: bool = False,override:bool=False):
        print("Updating shotdata with interpolated gnss data")
        # TODO Need to only update positions for a single shot and not each transponder
        # For each shotdata multiasset entry, update the shotdata position with gnss data
        try:
            merge_signature, dates = get_merge_signature_shotdata(
                shotdatasource, gnssdatasource
            )
        except Exception as e:
            print(e)
            return
        merge_job = {
            "parent_type": AssetType.GNSS.value,
            "child_type": AssetType.SHOTDATA.value,
            "parent_ids": merge_signature,
        }
        if not self.catalog.is_merge_complete(**merge_job) or override:
            dates.append(dates[-1]+datetime.timedelta(days=1))
            merge_shotdata_gnss(
                shotdata=shotdatasource, gnss=gnssdatasource, dates=dates, plot=plot
            )
            self.catalog.add_merge_job(**merge_job)
