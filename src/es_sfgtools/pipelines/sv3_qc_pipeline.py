# External Imports
import datetime
from tqdm.auto import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial
import pandas as pd
from typing import List, Optional
from pathlib import Path
import concurrent.futures
from es_sfgtools.data_models.metadata import MetaDataCatalog as MetaDataCatalog
import sys 
from numpy import datetime64

# Local imports
from ..data_mgmt.catalog import PreProcessCatalog
from ..data_mgmt.file_schemas import AssetEntry, AssetType
from ..sonardyne_tools import sv3_operations as sv3_ops

from ..novatel_tools import novatel_binary_operations as novb_ops,novatel_ascii_operations as nova_ops
from ..tiledb_tools.tiledb_operations import tile2rinex
from ..pride_tools import (
    PrideCLIConfig,
    rinex_to_kin,
    kin_to_kin_position_df,
    get_nav_file,
    get_gnss_products,
    rinex_utils
)
from ..tiledb_tools.tiledb_schemas import (
    TDBKinPositionArray,
    TDBShotDataArray,
    TDBGNSSObsArray,
)
from ..data_mgmt.utils import (
    get_merge_signature_shotdata,
    merge_shotdata_kinposition,
)
from ..logging import ProcessLogger as logger

from .config import SV3QCPipelineConfig 
from .sv3_pipeline import rinex_to_kin_wrapper

class SV3QCPipeline:

    def __init__(
        self,
        asset_catalog: PreProcessCatalog = None,
        data_catalog: MetaDataCatalog = None,
        config: SV3QCPipelineConfig = None,
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
        self.gnss_obs_data_dest = TDBGNSSObsArray(
            self.data_catalog.catalog.networks[network].stations[station].gnssobsdata
        )
        self.gnss_obs_data_dest_secondary = TDBGNSSObsArray(
            self.data_catalog.catalog.networks[network].stations[station].gnssobsdata_secondary
        )
        self.qc_kin_position_data_dest = TDBKinPositionArray(
            self.data_catalog.catalog.networks[network].stations[station].qckinpositiondata
        )
        self.qcdata_dest = TDBShotDataArray(
            self.data_catalog.catalog.networks[network].stations[station].qcdata
        )
        self.qcdata_pre = TDBShotDataArray(
            self.data_catalog.catalog.networks[network].stations[station].qcdata_pre
        )
        self.imu_position_data_dest = TDBKinPositionArray(
            self.data_catalog.catalog.networks[network].stations[station].imupositiondata
        )

    def process_pin(self) -> None:
        """
        Generates Acoustic ping-reply shotdata sequences from Sonardyne qc pin files for the specified network, station, and campaign.

        1. Retrieves pin asset entries from the asset catalog that need processing.
        2. For each pin entry found:
            - Converts the pin file to a ShotData dataframe using `sv3_ops.dfop00_to_shotdata`.
            - If successful, writes the dataframe to the pre-shotdata storage.
            - Marks the QCPIN entry as processed and updates it in the asset catalog.
   
        Returns:
            None
        """


        qcpin_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                campaign=self.campaign,
                parent_type=AssetType.QCPIN,
                override=self.config.qcpin_config.override,
            )
        )
        if not qcpin_entries:
            response = f"No QCPIN Files Found to Process for {self.network} {self.station} {self.campaign}"
            logger.logerr(response)
            return

        response = f"Found {len(qcpin_entries)} QCPIN Files to Process"
        logger.loginfo(response)
        count = 0

        with Pool() as pool:
            results = pool.imap(sv3_ops.qc_pin_to_shotdata, [x.local_path for x in qcpin_entries])
            for shotdata_df, qcpin_entry in tqdm(
                zip(results, qcpin_entries),
                total=len(qcpin_entries),
                desc="Processing QCPIN Files",
            ):
                if shotdata_df is not None and not shotdata_df.empty:
                    self.qcdata_pre.write_df(shotdata_df)  # write to qcdata_pre
                    count += 1
                    qcpin_entry.is_processed = True
                    self.asset_catalog.add_or_update(qcpin_entry)
                    logger.logdebug(f" Processed {qcpin_entry.local_path}")
                else:
                    logger.logerr(f"Failed to Process {qcpin_entry.local_path}")
        
        self.qcdata_pre.consolidate()

        response = f"Generated {count} ShotData dataframes From {len(qcpin_entries)} QCPIN Files"
        logger.loginfo(response)

    def parse_rangea_logs_from_pin(self):
        """generate ascii files containing RANGEA lines from pin files 
        """
        qcpin_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                campaign=self.campaign,
                parent_type=AssetType.QCPIN,
                override=self.config.qcpin_config.override,
            )
        )
        if not qcpin_entries:
            response = f"No QCPIN Files Found to Process for {self.network} {self.station} {self.campaign}"
            logger.logerr(response)
            return

        response = f"Found {len(qcpin_entries)} QCPIN Files to Process"
        logger.loginfo(response)
        count = 0
        with Pool() as pool:
            params = [(x.local_path,self.inter_dir) for x in qcpin_entries]
            results = pool.starmap(nova_ops.qcpin_to_novatelpin, params)
            for path, qcpin_entry in tqdm(
                zip(results, qcpin_entries),
                total=len(qcpin_entries),
                desc="Processing QCPIN Files",
            ):
            #add path to catalog
                if path is not None and path.exists():
                    asset_entry = AssetEntry(
                    local_path=path,
                    network=self.network,
                    station=self.station,
                    campaign=self.campaign,
                    type=AssetType.NOVATELPIN,
                    timestamp_created=datetime.datetime.now()
                    )
                
                    if self.asset_catalog.add_entry(asset_entry):
                        count += 1
                    qcpin_entry.is_processed = True
                    self.asset_catalog.add_or_update(qcpin_entry)
                    logger.logdebug(f" Processed {qcpin_entry.local_path}")
                else:
                    logger.logerr(f"Failed to Process {qcpin_entry.local_path}")
        logger.loginfo(f"Processed {len(qcpin_entries)} and added {count} novatel_pin files to the catalog")

    def process_rangea_logs(self):
        novatel_pin_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                campaign=self.campaign,
                parent_type=AssetType.NOVATELPIN,
                override=self.config.qcpin_config.override,
            )
        )
        if not novatel_pin_entries:
            response = f"No NOVATELPIN Files Found to Process for {self.network} {self.station} {self.campaign}"
            logger.logerr(response)
            return
        
        novatel_pin_files = [x.local_path for x in novatel_pin_entries]
        #split into batches of 20
        batch_size = 20
        batches = [novatel_pin_files[i:i + batch_size] for i in range(0, len(novatel_pin_files), batch_size)]
        for batch in batches:
            nova_ops.novatel_ascii_2tile(files=batch,gnss_obs_tdb=self.gnss_obs_data_dest_secondary.uri)

    def get_rinex_files(self) -> None:
        """
        Generates and catalogs daily RINEX files for the specified network, station, and campaign year.

        1. Consolidates the range data in the destination TDB array.
        2. Determines the processing year based on the configuration or campaign name.
        3. Checks if RINEX files need to be generated.
        4. If generation is required, it invokes the `tile2rinex` function to create RINEX files from the GNSS observation TileDB array.
        5. For each generated RINEX file, it creates an `AssetEntry` and adds it to the asset catalog.

        Returns:
            None
        """
        
        logger.loginfo(
            f"Using secondary GNSS data for RINEX generation for {self.network} {self.station} {self.campaign}"
        )
        gnss_obs_data_dest = self.gnss_obs_data_dest_secondary

        gnss_obs_data_dest.consolidate()

        if self.config.rinex_config.processing_year != -1:
            year = self.config.rinex_config.processing_year
        else:
            year = int(
                self.campaign.split("_")[0]
            )  # default to the year from the campaign name

        logger.loginfo(
            f"Generating Rinex Files for {self.network} {self.station} {year}. This may take a few minutes..."
        )
        parent_ids = f"N-{self.network}|ST-{self.station}|SV-{self.campaign}|TDB-{gnss_obs_data_dest.uri}|YEAR-{year}"
        merge_signature = {
            "parent_type": AssetType.GNSSOBSTDB.value,
            "child_type": AssetType.RINEX.value,
            "parent_ids": [parent_ids],
        }

        if (
            self.config.rinex_config.override
            or not self.asset_catalog.is_merge_complete(**merge_signature)
        ):
            try:
                rinex_paths: List[Path] = tile2rinex(
                    gnss_obs_tdb=gnss_obs_data_dest.uri,
                    settings=self.config.rinex_config.settings_path,
                    writedir=self.inter_dir,
                    time_interval=self.config.rinex_config.time_interval,
                processing_year=year,  # TODO pass down
                )
                if len(rinex_paths) == 0:
                    logger.logwarn(
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
                    rinex_entries.append(rinex_entry)
                    if self.asset_catalog.add_entry(rinex_entry):
                        uploadCount += 1

                self.asset_catalog.add_merge_job(**merge_signature)

                logger.loginfo(
                    f"Generated {len(rinex_entries)} Rinex files spanning {rinex_entries[0].timestamp_data_start} to {rinex_entries[-1].timestamp_data_end}"
                )
                logger.logdebug(
                    f"Added {uploadCount} out of {len(rinex_entries)} Rinex files to the catalog"
                )
            except Exception as e:
                if (message := logger.logerr(f"Error generating RINEX files: {e}")) is not None:
                    print(message)
                sys.exit(1)
        else:
            rinex_entries = self.asset_catalog.get_local_assets(
                self.network, self.station, self.campaign, AssetType.RINEX
            )
            num_rinex_entries = len(rinex_entries)
            logger.logdebug(
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
        Generates KinPosition dataframes from KIN files for the specified network, station, and campaign.

        This method searches for KIN and KINRESIDUALS asset entries to process. For each KIN entry found:
        - Attempts to convert the KIN file to a KinPosition dataframe using `kin_to_kin_position_df`.
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
                kin_position_df = kin_to_kin_position_df(kin_entry.local_path)
                if kin_position_df is not None:
                    processed_count += 1
                    kin_entry.is_processed = True
                    self.asset_catalog.add_or_update(kin_entry)
                    self.qc_kin_position_data_dest.write_df(kin_position_df)
            except Exception as e:
                logger.logerr(f"Error processing {kin_entry.local_path}: {e}")

        logger.loginfo(
            f"Generated {processed_count} KinPosition Dataframes From {len(kin_entries)} Kin Files"
        )

    # def merge_qc_positions(
    #     qcdata_pre: TDBShotDataArray,
    #     qcdata: TDBShotDataArray, 
    #     qc_kin_position: TDBKinPositionArray,
    #     dates:List[datetime64],
    #   #  lengthscale:float=0.1,
    #     plot:bool=False) -> TDBShotDataArray:

    #     """
    #     Merge the qcdata and qc_kin_position data

    #     Args:
    #         shotdata_pre (TDBShotDataArray): the DFOP00 data
    #         shotdata (TDBShotDataArray): The shotdata array to write to
    #         kin_position (TDBKinPositionArray): The TileDB KinPosition array
    #         dates (List[datetime64]): The dates to merge
    #         plot (bool, optional): Plot the interpolated values. Defaults to False.

    #     """

    #     logger.loginfo("Merging qcdata and qc_kin_position data")
    #     for start,end in zip(dates,dates[1:]):
    #         logger.loginfo(f"Merging qcdata for date {str(start)}")

    #         qcdata_df = qcdata_pre.read_df(start=start,end=end)
    #         qc_kin_position_df = qc_kin_position.read_df(start=start, end=end)

    #         if qcdata_df.empty or qc_kin_position_df.empty:
    #             continue

    #         qc_kin_position_df.time = qc_kin_position_df.time.apply(lambda x:x.timestamp())

            

    #         # use the qc_kin ENU values for ping send, and then the delta ENU from qcdata_pre to calculate position at ping reply
            
            
            
    #         # qcdata_df_updated = interpolate_enu_radius_regression(
    #         #     kin_position_df=kin_position_df,
    #         #     shotdata_df=shotdata_df.copy(),
    #         #     lengthscale=lengthscale
    #         # )


    #         qcdata.write_df(qcdata_df_updated,validate=False)



    def update_shotdata(self):
        """
        Refines acoustic ping-reply sequences in the qcdata_pre tiledb array with interpolated QCKinPosition data.

        Steps:
            1. Retrieves the merge signature and relevant dates for qcdata and QCKinPosition data.
            2. Checks if the merge job is complete or if override is enabled.
            3. Extends the date range and performs the merge using QCKinPosition data.
            4. Records the merge job in the asset catalog.
    
        """

        logger.loginfo("Updating qcdata with interpolated QCKinPosition data")

        try:
            merge_signature, dates = get_merge_signature_shotdata(
                self.qcdata_pre, self.qc_kin_position_data_dest
            )
        except Exception as e:
            logger.logerr(e)
            return
        merge_job = {
            "parent_type": AssetType.KINPOSITION.value,
            "child_type": AssetType.SHOTDATA.value,
            "parent_ids": merge_signature,
        }
        if (
            not self.asset_catalog.is_merge_complete(**merge_job)
            or self.config.position_update_config.override
        ):
            dates.append(dates[-1] + datetime.timedelta(days=1))
            merge_shotdata_kinposition(
                shotdata_pre=self.qcdata_pre,
                shotdata=self.shot_data_dest,
                kin_position=self.qc_kin_position_data_dest,
                dates=dates,
                lengthscale=self.config.position_update_config.lengthscale,
                plot=self.config.position_update_config.plot,
            )
            self.asset_catalog.add_merge_job(**merge_job)

    # def run_pipeline(self):
    #     """
    #     Executes the complete SV3 data processing pipeline.
    #     This method runs a sequence of processing steps required for SV3 pipeline:
    #     1. Pre-processes Novatel data.
    #     2. Retrieves RINEX files.
    #     3. Processes RINEX files.
    #     4. Processes kinematic data.
    #     5. Processes DFOP00 data.
    #     6. Updates shot data with processed results.
    #     Each step corresponds to a dedicated method that handles a specific part of the pipeline.
    #     """

    #     self.pre_process_novatel()
    #     self.get_rinex_files()
    #     self.process_rinex()
    #     self.process_kin()
    #     self.process_dfop00()
    #     self.update_shotdata()
