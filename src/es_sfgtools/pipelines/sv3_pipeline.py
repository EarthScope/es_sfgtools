# External Imports
import datetime
from tqdm.auto import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial
import pandas as pd
from typing import List, Optional
from pathlib import Path
import concurrent.futures
import sys 
import json

# Local imports
from ..data_mgmt.catalog import PreProcessCatalog
from ..data_mgmt.directory_handler import DirectoryHandler,NetworkDir,StationDir,CampaignDir
from es_sfgtools.data_models.metadata.site import Site
from ..data_mgmt.file_schemas import AssetEntry, AssetType
from ..sonardyne_tools import sv3_operations as sv3_ops
from ..novatel_tools import novatel_binary_operations as novb_ops,novatel_ascii_operations as nova_ops
from ..novatel_tools.utils import get_metadata,get_metadatav2
from ..seafloor_site_tools.soundspeed_operations import (
    CTD_to_svp_v1 ,
    seabird_to_soundvelocity,
    CTD_to_svp_v2
)
from .pipeline import SV3PipelineConfig, SV3Pipeline
from ..logging import ProcessLogger
from ..configuration import SV3PipelineConfig

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
    TDBIMUPositionArray
)
from ..data_mgmt.utils import (
    get_merge_signature_shotdata,
)
from .shotdata_gnss_refinement import merge_shotdata_kinposition
from ..logging import ProcessLogger,change_all_logger_dirs,PRIDELogger

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

    try:
        kin_entry = AssetEntry(
            local_path=kinfile,
            network=rinex_entry.network,
            station=rinex_entry.station,
            campaign=rinex_entry.campaign,
        timestamp_data_start=rinex_entry.timestamp_data_start,
        timestamp_data_end=rinex_entry.timestamp_data_end,
        type=AssetType.KIN,
        timestamp_created=datetime.datetime.now(),
        parent_id=rinex_entry.id
    )   
        if resfile is None:
            return kin_entry, None
        
        resfile_entry = AssetEntry(
            local_path=resfile,
            network=rinex_entry.network,
            station=rinex_entry.station,
            campaign=rinex_entry.campaign,
            timestamp_data_start=rinex_entry.timestamp_data_start,
            timestamp_data_end=rinex_entry.timestamp_data_end,
            type=AssetType.KINRESIDUALS,
            timestamp_created=datetime.datetime.now(),
            parent_id=rinex_entry.id
        )
    except Exception as e:
        ProcessLogger.logerr(f"Error creating AssetEntry for KIN or RES file: {e}")
        return None, None
    return kin_entry, resfile_entry

class SV3Pipeline:

    def __init__(
        self,
        directory_handler: DirectoryHandler = None,
        config: SV3PipelineConfig = None,
    ):
        """
        Initializes the SV3Pipeline instance with the provided asset catalog, data catalog, and configuration.

        Args:
            asset_catalog (PreProcessCatalog, optional): Catalog containing preprocessed assets. Defaults to None.
            data_catalog (Catalog, optional): Catalog containing data for processing. Defaults to None.
            config (SV3PipelineConfig, optional): Configuration settings for the pipeline. Defaults to None.
        """

        self.directory_handler = directory_handler
        self.config = config if config is not None else SV3PipelineConfig()
        self.asset_catalog = PreProcessCatalog(self.directory_handler.asset_catalog_db_path)

        self.currentNetwork: str = None
        self.currentStation: str = None
        self.currentCampaign: str = None

        self.currentNetworkDir: NetworkDir = None
        self.currentStationDir: StationDir = None
        self.currentCampaignDir: CampaignDir = None

        self.shotDataPreTDB: TDBShotDataArray = None
        self.kinPositionTDB: TDBKinPositionArray = None
        self.imuPositionTDB: TDBIMUPositionArray = None
        self.shotDataFinalTDB: TDBShotDataArray = None

    def setNetworkStationCampaign(
        self,
        network: str,
        station: str,
        campaign: str,
    
    ) -> None:
        """
        Set the site data for the pipeline.

        Args:
            kwargs: Keyword arguments containing site data.
        """
        # Reset current attributes
        self.currentNetwork = None
        self.currentStation = None
        self.currentCampaign = None

        self.currentNetworkDir = None
        self.currentStationDir = None
        self.currentCampaignDir = None

        self.shotDataPreTDB = None
        self.kinPositionTDB = None
        self.imuPositionTDB = None
        self.shotDataFinalTDB = None

        # Make sure there are files to process
        dtype_counts = self.asset_catalog.get_dtype_counts(network, station, campaign)
        if dtype_counts == {}:
            ProcessLogger.logwarn(f"No local files found for {network}/{station}/{campaign}")
            return

        networkDir, stationDir, campaignDir, _ = self.directory_handler.build_station_directory(
            network_name=network, station_name=station, campaign_name=campaign
        )
        self.currentNetworkDir = networkDir
        self.currentStationDir = stationDir
        self.currentCampaignDir = campaignDir

        self.currentNetwork = network
        self.currentStation = station
        self.currentCampaign = campaign

        ProcessLogger.set_dir(campaignDir.log_directory)

        for dtype, count in dtype_counts.items():
            ProcessLogger.loginfo(f"Found {count} local files of type {dtype} for {network}/{station}/{campaign}")

        shotDataPreURI = self.currentStationDir.tiledb_directory.shot_data_pre
        kinematicDataURI = self.currentStationDir.tiledb_directory.kin_position_data
        shotDataFinalURI = self.currentStationDir.tiledb_directory.shot_data
        positionDataURI = self.currentStationDir.tiledb_directory.imu_position_data

        self.shotDataPreTDB = TDBShotDataArray(shotDataPreURI)
        self.kinPositionTDB = TDBKinPositionArray(kinematicDataURI)
        self.imuPositionTDB = TDBIMUPositionArray(positionDataURI)
        self.shotDataFinalTDB = TDBShotDataArray(shotDataFinalURI)

        self.gnssObsTDBURI = self.currentStationDir.tiledb_directory.gnss_obs_data
        self.gnssObsTDB_secondaryURI = self.currentStationDir.tiledb_directory.gnss_obs_data_secondary

        self._build_rinex_meta()

    def _build_rinex_meta(self) -> None:
        """
        Build the RINEX metadata for a station.
        Args:
            station_dir (Path): The station directory to build the RINEX metadata for.
        """
        # Get the RINEX metadata
        rinex_metav2 = (
            self.currentCampaignDir.location
            / "rinex_metav2.json"
        )
        rinex_metav1 = (
            self.currentStationDir.location
            / "rinex_metav1.json"
        )
        if not rinex_metav2.exists():
            with open(rinex_metav2, "w") as f:
                json.dump(get_metadatav2(site=self.currentStation), f)

        if not rinex_metav1.exists():
            with open(rinex_metav1, "w") as f:
                json.dump(get_metadata(site=self.currentStation), f)

        self.config.rinex_config.settings_path = rinex_metav2

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
            network=self.currentNetwork,
            station=self.currentStation,
            campaign=self.currentCampaign,
            type=AssetType.NOVATEL770,
        )

        if novatel_770_entries:
            ProcessLogger.loginfo(
                f"Processing {len(novatel_770_entries)} Novatel 770 files for {self.currentNetwork} {self.currentStation} {self.currentCampaign}. This may take a few minutes..."
            )
            merge_signature = {
                "parent_type": AssetType.NOVATEL770.value,
                "child_type": AssetType.GNSSOBSTDB.value,
                "parent_ids": [x.id for x in novatel_770_entries],
            }
            if (
                self.config.novatel_config.override
                or not self.asset_catalog.is_merge_complete(**merge_signature)
            ):  
                try:
                    novb_ops.novatel_770_2tile(
                        files=[x.local_path for x in novatel_770_entries],
                        gnss_obs_tdb=self.gnssObsTDBURI,
                        n_procs=self.config.novatel_config.n_processes,
                    )

                    self.asset_catalog.add_merge_job(**merge_signature)
                    response = f"Added merge job for {len(novatel_770_entries)} Novatel 770 Entries to the catalog"
                    ProcessLogger.loginfo(response)
                except Exception as e:
                    if (message := ProcessLogger.logerr(f"Error processing Novatel 770 files: {e}")) is not None:
                        print(message)
                    sys.exit(1)
            else:
                response = f"Novatel 770 Data Already Processed for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
                ProcessLogger.loginfo(response)
        else:
           ProcessLogger.loginfo(
                f"No Novatel 770 Files Found to Process for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
            )

        ProcessLogger.loginfo(
            f"Processing Novatel 000 data for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
        )
        novatel_000_entries: List[AssetEntry] = self.asset_catalog.get_local_assets(
            network=self.currentNetwork,
            station=self.currentStation,
            campaign=self.currentCampaign,
            type=AssetType.NOVATEL000,
        )

        if novatel_000_entries:
            merge_signature = {
                "parent_type": AssetType.NOVATEL000.value,
                "child_type": AssetType.GNSSOBSTDB.value,
                "parent_ids": [x.id for x in novatel_000_entries],
            }
            if (
                self.config.novatel_config.override
                or not self.asset_catalog.is_merge_complete(**merge_signature)
            ):
                try:
                    novb_ops.novatel_000_2tile(
                        files=[x.local_path for x in novatel_000_entries],
                        gnss_obs_tdb=self.gnssObsTDB_secondaryURI,
                        position_tdb=self.imuPositionTDB.uri,
                        n_procs=self.config.novatel_config.n_processes,
                    )

                    self.asset_catalog.add_merge_job(**merge_signature)
                    ProcessLogger.loginfo(
                        f"Added merge job for {len(novatel_000_entries)} Novatel 000 Entries to the catalog"
                    )
                except Exception as e:
                    if (message := ProcessLogger.logerr(f"Error processing Novatel 000 files: {e}")) is not None:
                        print(message)
                    sys.exit(1)

        else:
            ProcessLogger.loginfo(
                f"No Novatel 000 Files Found to Process for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
            )
            return

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

        rinexDestination = self.currentCampaignDir.intermediate

        if self.config.rinex_config.use_secondary:
            ProcessLogger.loginfo(
                f"Using secondary GNSS data for RINEX generation for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
            )
            gnss_obs_data_dest = self.gnssObsTDB_secondaryURI
        else:
            gnss_obs_data_dest = self.gnssObsTDBURI

        if self.config.rinex_config.processing_year != -1:
            year = self.config.rinex_config.processing_year
        else:
            year = int(
                self.currentCampaign.split("_")[0]
            )  # default to the year from the campaign name

        ProcessLogger.loginfo(
            f"Generating Rinex Files for {self.currentNetwork} {self.currentStation} {year}. This may take a few minutes..."
        )
        parent_ids = f"N-{self.currentNetwork}|ST-{self.currentStation}|SV-{self.currentCampaign}|TDB-{gnss_obs_data_dest.uri}|YEAR-{year}"
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
                    gnss_obs_tdb=self.gnssObsTDBURI,
                    settings=self.config.rinex_config.settings_path,
                    writedir=rinexDestination,
                    time_interval=self.config.rinex_config.time_interval,
                processing_year=year,  # TODO pass down
                )
                if len(rinex_paths) == 0:
                    ProcessLogger.logwarn(
                        f"No Rinex Files generated for {self.currentNetwork} {self.currentStation} {self.currentCampaign} {year}."
                    )
                    return
                rinex_entries: List[AssetEntry] = []
                uploadCount = 0
                for rinex_path in rinex_paths:
                    rinex_time_start, rinex_time_end = rinex_utils.rinex_get_time_range(rinex_path)
                    rinex_entry = AssetEntry(
                        local_path=rinex_path,
                        network=self.currentNetwork,
                        station=self.currentStation,
                        campaign=self.currentCampaign,
                        timestamp_data_start=rinex_time_start,
                        timestamp_data_end=rinex_time_end,
                        type=AssetType.RINEX,
                        timestamp_created=datetime.datetime.now()
                    )
                    rinex_entries.append(rinex_entry)
                    if self.asset_catalog.add_entry(rinex_entry):
                        uploadCount += 1

                self.asset_catalog.add_merge_job(**merge_signature)

                ProcessLogger.loginfo(
                    f"Generated {len(rinex_entries)} Rinex files spanning {rinex_entries[0].timestamp_data_start} to {rinex_entries[-1].timestamp_data_end}"
                )
                ProcessLogger.logdebug(
                    f"Added {uploadCount} out of {len(rinex_entries)} Rinex files to the catalog"
                )
            except Exception as e:
                if (message := ProcessLogger.logerr(f"Error generating RINEX files: {e}")) is not None:
                    print(message)
                sys.exit(1)
        else:
            rinex_entries = self.asset_catalog.get_local_assets(
                self.currentNetwork, self.currentStation, self.currentCampaign, AssetType.RINEX
            )
            num_rinex_entries = len(rinex_entries)
            ProcessLogger.logdebug(
                f"RINEX files have already been generated for {self.currentNetwork}, {self.currentStation}, and {year} Found {num_rinex_entries} entries."
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

        response = f"Running PRIDE-PPPAR on Rinex Data for {self.currentNetwork} {self.currentStation} {self.currentCampaign}. This may take a few minutes..."
        ProcessLogger.loginfo(response)

        PRIDELogger.set_dir(self.currentCampaignDir.log_directory)

        prideDir = self.directory_handler.pride_directory
        intermediateDir = self.currentCampaignDir.intermediate
        rinex_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.currentNetwork,
                station=self.currentStation,
                campaign=self.currentCampaign,
                parent_type=AssetType.RINEX,
                child_type=AssetType.KIN,
                override=self.config.pride_config.override,
            )
        )
        if not rinex_entries:
            response = f"No Rinex Files Found to Process for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
            ProcessLogger.logerr(response)
            return

        response = f"Found {len(rinex_entries)} Rinex Files to Process"
        ProcessLogger.loginfo(response)

        """
        Get the PRIDE GNSS files for each unique DOY
        """
        get_nav_file_partial = partial(
            get_nav_file, override=self.config.pride_config.override_products_download
        )
        get_pride_config_partial = partial(
            get_gnss_products,
            pride_dir=prideDir,
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
            writedir=intermediateDir,
            pridedir=prideDir,
            site=self.currentStation,
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
        ProcessLogger.loginfo(response)

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
        ProcessLogger.loginfo(
            f"Looking for Kin Files to Process for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
        )

        kin_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.currentNetwork,
                station=self.currentStation,
                campaign=self.currentCampaign,
                parent_type=AssetType.KIN,
                override=self.config.rinex_config.override,
            )
        )
        res_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.currentNetwork,
                station=self.currentStation,
                campaign=self.currentCampaign,
                parent_type=AssetType.KINRESIDUALS,
                override=self.config.rinex_config.override,
            )
        )
        if not kin_entries:
            ProcessLogger.loginfo(
                f"No Kin Files Found to Process for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
            )
            return

        ProcessLogger.loginfo(f"Found {len(kin_entries)} Kin Files to Process: processing")

        processed_count = 0
        for kin_entry in tqdm(kin_entries, desc="Processing Kin Files"):
            try:
                kin_position_df = kin_to_kin_position_df(kin_entry.local_path)
                if kin_position_df is not None:
                    processed_count += 1
                    kin_entry.is_processed = True
                    self.asset_catalog.add_or_update(kin_entry)
                    self.kinPositionTDB.write_df(kin_position_df)
            except Exception as e:
               ProcessLogger.logerr(f"Error processing {kin_entry.local_path}: {e}")

        ProcessLogger.loginfo(
            f"Generated {processed_count} KinPosition Dataframes From {len(kin_entries)} Kin Files"
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

        dfop00_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.currentNetwork,
                station=self.currentStation,
                campaign=self.currentCampaign,
                parent_type=AssetType.DFOP00,
                override=self.config.dfop00_config.override,
            )
        )
        if not dfop00_entries:
            response = f"No DFOP00 Files Found to Process for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
            ProcessLogger.logerr(response)
            return

        response = f"Found {len(dfop00_entries)} DFOP00 Files to Process"
        ProcessLogger.loginfo(response)
        count = 0

        with Pool() as pool:
            results = pool.imap(sv3_ops.dfop00_to_shotdata, [x.local_path for x in dfop00_entries])
            for shotdata_df, dfo_entry in tqdm(
                zip(results, dfop00_entries),
                total=len(dfop00_entries),
                desc="Processing DFOP00 Files",
            ):
                if shotdata_df is not None and not shotdata_df.empty:
                    self.shotDataPreTDB.write_df(shotdata_df)  # write to pre-shotdata
                    count += 1
                    dfo_entry.is_processed = True
                    self.asset_catalog.add_or_update(dfo_entry)
                    ProcessLogger.logdebug(f" Processed {dfo_entry.local_path}")
                else:
                    ProcessLogger.logerr(f"Failed to Process {dfo_entry.local_path}")

        response = f"Generated {count} ShotData dataframes From {len(dfop00_entries)} DFOP00 Files"
        ProcessLogger.loginfo(response)

    def update_shotdata(self):
        """
        Refines acoustic ping-reply sequences in the shotdata_pre tiledb array with interpolated KinPosition data.

        Steps:
            1. Retrieves the merge signature and relevant dates for shotdata and KinPosition data.
            2. Checks if the merge job is complete or if override is enabled.
            3. Extends the date range and performs the merge using KinPosition data.
            4. Records the merge job in the asset catalog.
    
        """

        ProcessLogger.loginfo("Updating shotdata with interpolated KinPosition data")

        try:
            merge_signature, dates = get_merge_signature_shotdata(
                self.shotDataPreTDB.uri, self.kinPositionTDB.uri
            )
        except Exception as e:
            ProcessLogger.logerr(e)
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

            merge_shotdata_kinposition(
                shotdata_pre=self.shotDataPreTDB,
                shotdata=self.shotDataFinalTDB,
                kin_position=self.kinPositionTDB,
                position_data=self.imuPositionTDB,
                dates=dates,
             
            )
            self.asset_catalog.add_merge_job(**merge_job)

    def process_svp(self):
  

        ctd_entries: List[AssetEntry] = self.asset_catalog.get_local_assets(
            network=self.currentNetwork,
            station=self.currentStation,
            campaign=self.currentCampaign,
            type=AssetType.CTD,
        )
        seabird_entries: List[AssetEntry] = self.asset_catalog.get_local_assets(
            network=self.currentNetwork,
            station=self.currentStation,
            campaign=self.currentCampaign,
            type=AssetType.SEABIRD,
        )
        if not ctd_entries and not seabird_entries:
            response = f"No CTD or SEABIRD Files Found to Process for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
            ProcessLogger.logerr(response)
            return
        for ctd_entry in ctd_entries:
            isProcessed = False
            svp_df = pd.DataFrame()
            try:
                svp_df = CTD_to_svp_v1(ctd_entry.local_path)
                if not svp_df.empty:
                    ProcessLogger.loginfo(f"Processed SVP data from CTD file {ctd_entry.local_path} to dataframe with CTD_to_svp_v1")
                    isProcessed = True

            except Exception as e:
                ProcessLogger.logerr(f"Error processing CTD file {ctd_entry.local_path} with CTD_to_svp_v1: {e}")
            if not isProcessed:
                try:
                    svp_df = CTD_to_svp_v2(ctd_entry.local_path)
                    if not svp_df.empty:
                        ProcessLogger.loginfo(f"Processed SVP data from CTD file {ctd_entry.local_path} to dataframe with CTD_to_svp_v2")
                except Exception as e:
                    ProcessLogger.logerr(f"Error processing CTD file {ctd_entry.local_path} with CTD_to_svp_v2: {e}")
                    continue
            if not svp_df.empty:
                svp_df.to_csv(self.currentCampaignDir.svp_file, index=False)
                ctd_entry.is_processed = True
                self.asset_catalog.add_or_update(ctd_entry)
                ProcessLogger.loginfo(f"Saved SVP dataframe to {str(self.currentCampaignDir.svp_file)}")
                return
            
        for seabird_entry in seabird_entries:
            try:
                svp_df = seabird_to_soundvelocity(seabird_entry.local_path)
                if not svp_df.empty:
                    svp_df.to_csv(self.currentCampaignDir.svp_file, index=False)
                    seabird_entry.is_processed = True
                    self.asset_catalog.add_or_update(seabird_entry)
                    ProcessLogger.loginfo(f"Processed SVP data from Seabird file {seabird_entry.local_path} and saved to {str(self.currentCampaignDir.svp_file)}")
                    return
            except Exception as e:
                ProcessLogger.logerr(f"Error processing Seabird file {seabird_entry.local_path}: {e}")
                continue

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
