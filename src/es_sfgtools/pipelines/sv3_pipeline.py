# External Imports
import datetime
from tqdm.auto import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial
import pandas as pd
from typing import List, Optional
from pathlib import Path
import concurrent.futures
from sfg_metadata.metadata.src.catalogs import Catalog

# Local imports
from ..data_mgmt.catalog import PreProcessCatalog
from ..data_mgmt.file_schemas import AssetEntry, AssetType
from ..sonardyne_tools import sv3_operations as sv3_ops
from ..novatel_tools import novatel_binary_operations as novb_ops,novatel_ascii_operations as nova_ops
from ..tiledb_tools.tiledb_operations import tile2rinex
from ..pride_tools import (
    PrideCLIConfig,
    rinex_to_kin,
    kin_to_gnssdf,
    get_nav_file,
    get_gnss_products,
    rinex_utils
)
from ..tiledb_tools.tiledb_schemas import (
    TDBGNSSArray,
    TDBShotDataArray,
    TDBGNSSObsArray,
)
from ..data_mgmt.utils import (
    get_merge_signature_shotdata,
    merge_shotdata_gnss,
)
from ..logging import ProcessLogger as logger

from .config import SV3PipelineConfig

def rinex_to_kin_wrapper(
    rinex_prideconfig_path: tuple[AssetEntry, Path],
    writedir: Path,
    pridedir: Path,
    site: str,
    pride_config: PrideCLIConfig,
) -> tuple[Optional[AssetEntry], Optional[AssetEntry]]:
        
    """
    Wrapper function to convert a RINEX file to KIN format using PRIDE configuration.
    This function takes a tuple containing an AssetEntry for the RINEX file and the path to the PRIDE configuration file,
    along with directories for writing output and PRIDE processing, the site name, and a PRIDE CLI configuration object.
    It updates the PRIDE configuration with the provided config file path, then calls `rinex_to_kin` to perform the conversion.
    If successful, it returns AssetEntry objects for the generated KIN file and its residuals file; otherwise, returns (None, None).
    Args:
        rinex_prideconfig_path (tuple[AssetEntry, Path]): Tuple containing the RINEX AssetEntry and PRIDE config file path.
        writedir (Path): Directory where output files should be written.
        pridedir (Path): Directory for PRIDE processing.
        site (str): Name of the site/station.
        pride_config (PrideCLIConfig): PRIDE CLI configuration object.
    Returns:
        tuple[Optional[AssetEntry], Optional[AssetEntry]]:
            AssetEntry for the generated KIN file and AssetEntry for the residuals file,
            or (None, None) if conversion fails.
    """

    rinex_entry, pride_config_path = rinex_prideconfig_path
    pride_config = pride_config.model_copy()
    pride_config.pride_configfile_path = pride_config_path

    kinfile, resfile = rinex_to_kin(
        source=rinex_entry.local_path,
        writedir=writedir,
        pridedir=pridedir,
        site=site,
        pride_cli_config=pride_config,
    )
    if kinfile is None:
        return None, None

    kin_entry = AssetEntry(
        local_path=kinfile,
        network=rinex_entry.network,
        station=rinex_entry.station,
        campaign=rinex_entry.campaign,
        timestamp_data_start=rinex_entry.timestamp_data_start,
        timestamp_data_end=rinex_entry.timestamp_data_end,
        type=AssetType.KIN,
        timestamp_created=datetime.datetime.now()
    )
    resfile_entry = AssetEntry(
        local_path=resfile,
        network=rinex_entry.network,
        station=rinex_entry.station,
        campaign=rinex_entry.campaign,
        timestamp_data_start=rinex_entry.timestamp_data_start,
        timestamp_data_end=rinex_entry.timestamp_data_end,
        type=AssetType.KINRESIDUALS,
        timestamp_created=datetime.datetime.now()
    )
    return kin_entry, resfile_entry

class SV3Pipeline:

    def __init__(
        self,
        asset_catalog: PreProcessCatalog = None,
        data_catalog: Catalog = None,
        config: SV3PipelineConfig = None,
    ):
        """
        Initializes the SV3Pipeline instance with the provided asset catalog, data catalog, and configuration.

        Args:
            asset_catalog (PreProcessCatalog, optional): Catalog containing preprocessed assets. Defaults to None.
            data_catalog (Catalog, optional): Catalog containing data for processing. Defaults to None.
            config (SV3PipelineConfig, optional): Configuration settings for the pipeline. Defaults to None.
        """
        self.asset_catalog = asset_catalog
        self.data_catalog = data_catalog
        self.config = config

    def set_site_data(
        self,
        network: str,
        station: str,
        campaign: str,
        inter_dir: Path,
        pride_dir: Path,
    ) -> None:
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
        self.rangea_data_dest = TDBGNSSObsArray(
            self.data_catalog.catalog.networks[network].stations[station].gnssobsdata
        )
        self.gnss_data_dest = TDBGNSSArray(
            self.data_catalog.catalog.networks[network].stations[station].gnssdata
        )
        self.shot_data_dest = TDBShotDataArray(
            self.data_catalog.catalog.networks[network].stations[station].shotdata
        )
        self.shot_data_pre = TDBShotDataArray(
            self.data_catalog.catalog.networks[network].stations[station].shotdata_pre
        )
        self.position_data_dest = TDBGNSSArray(
            self.data_catalog.catalog.networks[network].stations[station].positiondata
        )

    def pre_process_novatel(self) -> None:
        """
        Processes Novatel 770 and Novatel 000 asset entries for the current network, station, and campaign.
        This method performs the following steps:
        1. Retrieves Novatel 770 asset entries from the asset catalog.
        2. If entries are found, checks if processing should be overridden or if merge is incomplete.
           - If so, processes the files using `novb_ops.novatel_770_2tile`, updates the asset catalog, and logs the operation.
           - Otherwise, logs that the data has already been processed.
        3. Logs if no Novatel 770 files are found.
        4. Retrieves Novatel 000 asset entries from the asset catalog.
        5. If entries are found, checks if processing should be overridden or if merge is incomplete.
           - If so, processes the files using `novb_ops.novatel_000_2tile`, updates the asset catalog, and logs the operation.
        6. Logs if no Novatel 000 files are found.
        Logging is performed throughout to provide status updates.
        """
        novatel_770_entries: List[AssetEntry] = self.asset_catalog.get_local_assets(
            network=self.network,
            station=self.station,
            campaign=self.campaign,
            type=AssetType.NOVATEL770,
        )

        if novatel_770_entries:
            logger.loginfo(
                f"Processing {len(novatel_770_entries)} Novatel 770 files for {self.network} {self.station} {self.campaign}. This may take a few minutes..."
            )
            merge_signature = {
                "parent_type": AssetType.NOVATEL770.value,
                "child_type": AssetType.RANGEATDB.value,
                "parent_ids": [x.id for x in novatel_770_entries],
            }
            if (
                self.config.novatel_config.override
                or not self.asset_catalog.is_merge_complete(**merge_signature)
            ):
                novb_ops.novatel_770_2tile(
                    files=[x.local_path for x in novatel_770_entries],
                    rangea_tdb=self.rangea_data_dest.uri,
                    n_procs=self.config.novatel_config.n_processes,
                )

                self.asset_catalog.add_merge_job(**merge_signature)
                response = f"Added merge job for {len(novatel_770_entries)} Novatel 770 Entries to the catalog"
                logger.loginfo(response)
                # if self.config.novatel_config.show_details:
                #     print(response)
            else:
                response = f"Novatel 770 Data Already Processed for {self.network} {self.station} {self.campaign}"
                logger.loginfo(response)
        else:
            logger.loginfo(
                f"No Novatel 770 Files Found to Process for {self.network} {self.station} {self.campaign}"
            )

        logger.loginfo(
            f"Processing Novatel 000 data for {self.network} {self.station} {self.campaign}"
        )
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
            if (
                self.config.novatel_config.override
                or not self.asset_catalog.is_merge_complete(**merge_signature)
            ):
                novb_ops.novatel_000_2tile(
                    files=[x.local_path for x in novatel_000_entries],
                    rangea_tdb=self.rangea_data_dest.uri,
                    position_tdb=self.position_data_dest.uri,
                    n_procs=self.config.novatel_config.n_processes,
                )

                self.asset_catalog.add_merge_job(**merge_signature)
                logger.loginfo(
                    f"Added merge job for {len(novatel_000_entries)} Novatel 000 Entries to the catalog"
                )
                # if self.config.novatel_config.show_details:
                #     print(response) # TODO: should the logger handle this?
        else:
            logger.loginfo(
                f"No Novatel 000 Files Found to Process for {self.network} {self.station} {self.campaign}"
            )
            return

    def get_rinex_files(self) -> None:
        """
        Generates and catalogs daily RINEX files for the specified network, station, and campaign year.

        1. Consolidates the rangea data in the destination TDB array.
        2. Determines the processing year based on the configuration or campaign name.
        3. Checks if RINEX files need to be generated.
        4. If generation is required, it invokes the `tile2rinex` function to create RINEX files from the GNSS observation TileDB array.
        5. For each generated RINEX file, it creates an `AssetEntry` and adds it to the asset catalog.

        Returns:
            None
        """


        self.rangea_data_dest.consolidate()

        if self.config.rinex_config.processing_year != -1:
            year = self.config.rinex_config.processing_year
        else:
            year = int(
                self.campaign.split("_")[0]
            )  # default to the year from the campaign name

        logger.loginfo(
            f"Generating Rinex Files for {self.network} {self.station} {year}. This may take a few minutes..."
        )
        parent_ids = f"N-{self.network}|ST-{self.station}|SV-{self.campaign}|TDB-{self.rangea_data_dest.uri}|YEAR-{year}"
        merge_signature = {
            "parent_type": AssetType.RANGEATDB.value,
            "child_type": AssetType.RINEX.value,
            "parent_ids": [parent_ids],
        }

        if (
            self.config.rinex_config.override
            or not self.asset_catalog.is_merge_complete(**merge_signature)
        ):
            rinex_paths: List[Path] = tile2rinex(
                rangea_tdb=self.rangea_data_dest.uri,
                settings=self.config.rinex_config.settings_path,
                writedir=self.inter_dir,
                time_interval=self.config.rinex_config.time_interval,
                processing_year=year,  # TODO pass down
            )
            if len(rinex_paths) == 0:
                logger.loginfo(
                    f"No Rinex Files generated for {self.network} {self.station} {self.campaign} {year}."
                )
                return
            rinex_entries: List[AssetEntry] = []
            uploadCount = 0
            for rinex_path in rinex_paths:
                rinex_time_start, rinex_time_end = rinex_utils.rinex_get_time_range(rinex_path)
                rinex_entry = AssetEntry(
                    local_path=rinex_path,
                    network=self.network,
                    station=self.station,
                    campaign=self.campaign,
                    timestamp_data_start=rinex_time_start,
                    timestamp_data_end=rinex_time_end,
                    type=AssetType.RINEX,
                    timestamp_created=datetime.datetime.now()
                )
                if self.asset_catalog.add_entry(rinex_entry):
                    uploadCount += 1

            self.asset_catalog.add_merge_job(**merge_signature)

            logger.loginfo(
                f"Generated {len(rinex_entries)} Rinex files spanning {rinex_entries[0].timestamp_data_start} to {rinex_entries[-1].timestamp_data_end}"
            )
            logger.loginfo(
                f"Added {uploadCount} out of {len(rinex_entries)} Rinex files to the catalog"
            )
        else:
            rinex_entries = self.asset_catalog.get_local_assets(
                self.network, self.station, self.campaign, AssetType.RINEX
            )
            num_rinex_entries = len(rinex_entries)
            logger.loginfo(
                f"RINEX files have already been generated for {self.network}, {self.station}, and {year} Found {num_rinex_entries} entries."
            )

    def process_rinex(self) -> None:
        """
        Generates PRIDE-PPP Kinematic (KIN) files and Residual (RES) files from RINEX files for the specified network, station, and campaign.
        This method performs the following steps:
        1. Retrieves RINEX asset entries from the asset catalog that need processing.
        2. For each RINEX entry found:
            - Downloads or retrieves the necessary PRIDE GNSS product files (i.e. SP3,OBX,ATT).
            - Converts the RINEX file to KIN format using the `rinex_to_kin_wrapper`.
            - If successful, adds the KIN file and its residuals file to the asset catalog
        Raises:
            ValueError: If no Rinex files are found.
        """

        response = f"Running PRIDE-PPPAR on Rinex Data for {self.network} {self.station} {self.campaign}. This may take a few minutes..."
        logger.loginfo(response)

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
            logger.logerr(response)
            return

        response = f"Found {len(rinex_entries)} Rinex Files to Process"
        logger.loginfo(response)

        """
        Get the PRIDE GNSS files for each unique DOY
        """
        get_nav_file_partial = partial(
            get_nav_file, override=self.config.pride_config.override_products_download
        )
        get_pride_config_partial = partial(
            get_gnss_products,
            pride_dir=self.pride_dir,
            override=self.config.pride_config.override_products_download,
        )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            nav_files = [
                x
                for x in executor.map(
                    get_nav_file_partial, [x.local_path for x in rinex_entries]
                )
            ]
            pride_configs = [
                x
                for x in executor.map(
                    get_pride_config_partial, [x.local_path for x in rinex_entries]
                )
            ]

        rinex_prideconfigs = [
            (rinex_entry, pride_config_path)
            for rinex_entry, pride_config_path in zip(rinex_entries, pride_configs)
            if pride_config_path is not None
        ]

        process_rinex_partial = partial(
            rinex_to_kin_wrapper,
            writedir=self.inter_dir,
            pridedir=self.pride_dir,
            site=self.station,
            pride_config=self.config.pride_config,
        )
        kin_entries = []
        resfile_entries = []
        count = 0
        uploadCount = 0

        with Pool(
            processes=self.config.rinex_config.n_processes
        ) as pool:

            results = pool.map(process_rinex_partial, rinex_prideconfigs)

            for idx, (kinfile, resfile) in enumerate(
                tqdm(
                    results,
                    total=len(rinex_entries),
                    desc="Processing Rinex Files",
                    mininterval=0.5,
                )
            ):
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
        logger.loginfo(response)

    def process_kin(self):
        """
        Generates GNSS dataframes from KIN files for the specified network, station, and campaign.

        This method searches for KIN and KINRESIDUALS asset entries to process. For each KIN entry found:
        - Attempts to convert the KIN file to a GNSS dataframe using `kin_to_gnssdf`.
        - If successful, marks the entry as processed, updates the asset catalog, and writes the dataframe to the destination.
        - Logs errors encountered during processing.

        Logs the number of KIN files found and processed.

        Returns:
            None
        """
        logger.loginfo(
            f"Looking for Kin Files to Process for {self.network} {self.station} {self.campaign}"
        )
        kin_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                campaign=self.campaign,
                parent_type=AssetType.KIN,
                override=self.config.rinex_config.override,
            )
        )
        res_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                campaign=self.campaign,
                parent_type=AssetType.KINRESIDUALS,
                override=self.config.rinex_config.override,
            )
        )
        if not kin_entries:
            logger.loginfo(
                f"No Kin Files Found to Process for {self.network} {self.station} {self.campaign}"
            )
            return

        logger.loginfo(f"Found {len(kin_entries)} Kin Files to Process: processing")

        processed_count = 0
        for kin_entry in tqdm(kin_entries, desc="Processing Kin Files"):
            try:
                gnss_df = kin_to_gnssdf(kin_entry.local_path)
                if gnss_df is not None:
                    processed_count += 1
                    kin_entry.is_processed = True
                    self.asset_catalog.add_or_update(kin_entry)
                    self.gnss_data_dest.write_df(gnss_df)
            except Exception as e:
                logger.logerr(f"Error processing {kin_entry.local_path}: {e}")

        logger.loginfo(
            f"Generated {processed_count} GNSS Dataframes From {len(kin_entries)} Kin Files"
        )

    def process_dfop00(self) -> None:
        """
        Generates Acoustic ping-reply shotdata sequences from Sonardyne DFOP00 files for the specified network, station, and campaign.

        1. Retrieves DFOP00 asset entries from the asset catalog that need processing.
        2. For each DFOP00 entry found:
            - Converts the DFOP00 file to a ShotData dataframe using `sv3_ops.dfop00_to_shotdata`.
            - If successful, writes the dataframe to the pre-shotdata storage.
            - Marks the DFOP00 entry as processed and updates it in the asset catalog.
   
        Returns:
            None
        """


        # TODO need a way to mark the dfopoo files as processed
        dfop00_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                campaign=self.campaign,
                parent_type=AssetType.DFOP00,
                override=self.config.dfop00_config.override,
            )
        )
        if not dfop00_entries:
            response = f"No DFOP00 Files Found to Process for {self.network} {self.station} {self.campaign}"
            logger.logerr(response)
            return

        response = f"Found {len(dfop00_entries)} DFOP00 Files to Process"
        logger.loginfo(response)
        count = 0

        with Pool() as pool:
            results = pool.imap(sv3_ops.dfop00_to_shotdata, [x.local_path for x in dfop00_entries])
            for shotdata_df, dfo_entry in tqdm(
                zip(results, dfop00_entries),
                total=len(dfop00_entries),
                desc="Processing DFOP00 Files",
            ):
                if shotdata_df is not None and not shotdata_df.empty:
                    self.shot_data_pre.write_df(shotdata_df)  # write to pre-shotdata
                    count += 1
                    dfo_entry.is_processed = True
                    self.asset_catalog.add_or_update(dfo_entry)
                    logger.logdebug(f" Processed {dfo_entry.local_path}")
                else:
                    logger.logerr(f"Failed to Process {dfo_entry.local_path}")

        response = f"Generated {count} ShotData dataframes From {len(dfop00_entries)} DFOP00 Files"
        logger.loginfo(response)

    def update_shotdata(self):
        """
        Refines acoustic ping-reply sequences in the shotdata_pre tiledb array with interpolated GNSS data.

        Steps:
            1. Retrieves the merge signature and relevant dates for shotdata and GNSS data.
            2. Checks if the merge job is complete or if override is enabled.
            3. Extends the date range and performs the merge using GNSS data.
            4. Records the merge job in the asset catalog.
    
        """

        logger.loginfo("Updating shotdata with interpolated gnss data")

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
        if (
            not self.asset_catalog.is_merge_complete(**merge_job)
            or self.config.position_update_config.override
        ):
            dates.append(dates[-1] + datetime.timedelta(days=1))
            merge_shotdata_gnss(
                shotdata_pre=self.shot_data_pre,
                shotdata=self.shot_data_dest,
                gnss=self.gnss_data_dest,
                dates=dates,
                lengthscale=self.config.position_update_config.lengthscale,
                plot=self.config.position_update_config.plot,
            )
            self.asset_catalog.add_merge_job(**merge_job)

    def run_pipeline(self):
        """
        Executes the complete SV3 data processing pipeline.
        This method runs a sequence of processing steps required for SV3 pipeline:
        1. Pre-processes Novatel data.
        2. Retrieves RINEX files.
        3. Processes RINEX files.
        4. Processes kinematic data.
        5. Processes DFOP00 data.
        6. Updates shot data with processed results.
        Each step corresponds to a dedicated method that handles a specific part of the pipeline.
        """

        self.pre_process_novatel()
        self.get_rinex_files()
        self.process_rinex()
        self.process_kin()
        self.process_dfop00()
        self.update_shotdata()
