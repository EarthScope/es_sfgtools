# External Imports
import concurrent.futures
import datetime
import json
import sys
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import List, Optional

from tqdm.auto import tqdm

# Local Imports
from es_sfgtools.logging import ProcessLogger, change_all_logger_dirs
from es_sfgtools.data_mgmt.assetcatalog.handler import PreProcessCatalogHandler
from es_sfgtools.data_mgmt.assetcatalog.schemas import AssetEntry, AssetType
from es_sfgtools.data_mgmt.directorymgmt import (
    CampaignDir,
    DirectoryHandler,
    NetworkDir,
    StationDir,
)
from es_sfgtools.data_models.observables import ShotDataFrame
from pandera.typing import DataFrame
from es_sfgtools.data_mgmt.utils import get_merge_signature_shotdata
from es_sfgtools.novatel_tools import novatel_ascii_operations as nova_ops
from es_sfgtools.novatel_tools.utils import get_metadata, get_metadatav2
from es_sfgtools.pride_tools import (
    PrideCLIConfig,
    get_gnss_products,
    get_nav_file,
    kin_to_kin_position_df,
    rinex_to_kin,
    rinex_utils,
)
from es_sfgtools.sonardyne_tools.sv3_qc_operations import qcjson_to_shotdata
from es_sfgtools.novatel_tools.rangea_parser import GNSSEpoch, extract_rangea_from_qcpin
from es_sfgtools.tiledb_tools.tiledb_operations import tile2rinex
from es_sfgtools.tiledb_tools.tiledb_schemas import (
    TDBGNSSObsArray,
    TDBKinPositionArray,
    TDBShotDataArray,
)
from .config import QCPipelineConfig
from .exceptions import (
    NoLocalData,
    NoQCPinFound,
    NoNovatelPinFound,
    NoRinexBuilt,
    NoRinexFound,
    NoKinFound,
)
from .shotdata_gnss_refinement import merge_shotdata_kinposition
from .sv3_pipeline import rinex_to_kin_wrapper
from ..utils.protocols import WorkflowABC, validate_network_station_campaign


class QCPipeline(WorkflowABC):
    """Orchestrates the QC data processing pipeline for seafloor geodesy.

    This class manages a workflow for processing QC (Quality Control) data
    from Sonardyne equipment, including:

    1. **QC PIN File Processing**:
       - Processes QC PIN JSON files to generate preliminary shotdata
       - Extracts RANGEA logs from PIN files for GNSS processing

    2. **GNSS Data Processing**:
       - Processes NOVATEL PIN files into TileDB GNSS observation arrays
       - Generates daily RINEX files from GNSS observations

    3. **Precise Point Positioning**:
       - Downloads GNSS product files (SP3, OBX, ATT)
       - Runs PRIDE-PPPAR for high-precision positioning
       - Generates kinematic (KIN) and residual files

    4. **Kinematic Position Processing**:
       - Converts KIN files to structured dataframes
       - Stores kinematic positions in QC-specific TileDB array

    5. **Shotdata Refinement**:
       - Interpolates high-precision GNSS positions to acoustic ping times
       - Refines shotdata with improved position estimates

    The QC pipeline uses separate TileDB arrays from the normal pipeline
    to avoid data overlap.

    Attributes
    ----------
    directory_handler : DirectoryHandler
        Manages the project directory structure.
    config : QCPipelineConfig
        Configuration settings for all pipeline steps.
    asset_catalog : PreProcessCatalogHandler
        SQLite-based catalog for tracking processed assets.
    qcShotDataPreTDB : TDBShotDataArray
        QC preliminary shotdata (before position refinement).
    qcKinPositionTDB : TDBKinPositionArray
        QC high-precision kinematic positions.
    qcShotDataFinalTDB : TDBShotDataArray
        QC final shotdata (after position refinement).
    qcGnssObsTDBURI : Path
        QC GNSS observation array URI.
    """

    mid_process_workflow = False

    def __init__(
        self,
        directory_handler: Optional[DirectoryHandler] = None,
        asset_catalog: Optional[PreProcessCatalogHandler] = None,
        config: QCPipelineConfig = None,
    ):
        """Initialize the QCPipeline with directory handler and configuration.

        Parameters
        ----------
        directory_handler : DirectoryHandler, optional
            Handler for managing project directory structure. Must be provided
            and should already be built.
        asset_catalog : PreProcessCatalogHandler, optional
            Pre-configured asset catalog handler. If not provided, will be
            created automatically.
        config : QCPipelineConfig, optional
            Configuration settings for the pipeline. If None, uses default
            configuration.
        """
        super().__init__(
            directory=directory_handler.location,
            asset_catalog=asset_catalog,
            directory_handler=directory_handler,
        )

        self.config = config if config is not None else QCPipelineConfig()

        # Initialize QC-specific TileDB array objects to None
        # These will be created when set_network_station_campaign() is called
        self.qcShotDataPreTDB: TDBShotDataArray = None
        self.qcKinPositionTDB: TDBKinPositionArray = None
        self.qcShotDataFinalTDB: TDBShotDataArray = None
        self.qcGnssObsTDBURI: Path = None

    def set_network_station_campaign(
        self,
        network_id: str,
        station_id: str,
        campaign_id: str,
    ) -> None:
        """Set the current network, station, and campaign context for pipeline processing.

        This method establishes the processing context and performs several
        initialization tasks:
        1. Resets previous context and clears TileDB arrays if context changes
        2. Calls parent method to handle context switching
        3. Validates data availability
        4. Initializes TileDB arrays
        5. Configures logging
        6. Prepares RINEX metadata

        Parameters
        ----------
        network_id : str
            Network identifier (e.g., "cascadia-gorda").
        station_id : str
            Station identifier (e.g., "NCC1").
        campaign_id : str
            Campaign identifier (e.g., "2023_A_1126").
        """
        # Clear TileDB arrays if switching context to avoid stale references
        if (
            network_id != self.current_network_name
            or station_id != self.current_station_name
            or campaign_id != self.current_campaign_name
        ):
            self.qcShotDataPreTDB = None
            self.qcKinPositionTDB = None
            self.qcShotDataFinalTDB = None
            self.qcGnssObsTDBURI = None

        # Call parent method with correct parameter names
        super().set_network_station_campaign(network_id, station_id, campaign_id)

        # Make sure there are files to process
        dtype_counts = self.asset_catalog.get_dtype_counts(
            network_id, station_id, campaign_id
        )
        if dtype_counts == {}:
            message = f"No local files found for {network_id}/{station_id}/{campaign_id}. Ensure data is ingested before processing."
            ProcessLogger.logerr(message)
            raise NoLocalData(message)

        # Update all log directories
        change_all_logger_dirs(self.current_campaign_dir.log_directory)

        for dtype, count in dtype_counts.items():
            ProcessLogger.loginfo(
                f"Found {count} local files of type {dtype} for {network_id}/{station_id}/{campaign_id}"
            )

        # Initialize TileDB arrays if not already created
        self._build_tiledb_arrays()
        self._build_rinex_meta()

    def _build_tiledb_arrays(self) -> None:
        """Initialize QC-specific TileDB arrays for the current station context."""
        tiledb_dir = self.current_station_dir.tiledb_directory

        if self.qcShotDataPreTDB is None:
            self.qcShotDataPreTDB = TDBShotDataArray(tiledb_dir.qc_shot_data_pre)
        if self.qcKinPositionTDB is None:
            self.qcKinPositionTDB = TDBKinPositionArray(tiledb_dir.qc_kin_position_data)
        if self.qcShotDataFinalTDB is None:
            self.qcShotDataFinalTDB = TDBShotDataArray(tiledb_dir.qc_shot_data)
        if self.qcGnssObsTDB is None:
            self.qcGnssObsTDB = TDBGNSSObsArray(tiledb_dir.qc_gnss_obs_data)

    def _build_rinex_meta(self) -> None:
        """Build RINEX metadata files for the current campaign if they don't exist."""
        rinex_metav2 = (
            self.current_campaign_dir.metadata_directory / "rinex_metav2.json"
        )
        rinex_metav1 = (
            self.current_campaign_dir.metadata_directory / "rinex_metav1.json"
        )
        if not rinex_metav2.exists():
            with open(rinex_metav2, "w") as f:
                json.dump(get_metadatav2(site=self.current_station_name), f)

        if not rinex_metav1.exists():
            with open(rinex_metav1, "w") as f:
                json.dump(get_metadata(site=self.current_station_name), f)

        self.config.rinex_config.settings_path = rinex_metav2

    def qcpin_to_shotdata_tdb(self,source:Path) -> None:

        df: Optional[DataFrame[ShotDataFrame]] = qcjson_to_shotdata(source)
        if df is not None and not df.empty:
            self.qcShotDataPreTDB.write_df(df)

    def qcpin_to_gnssobs_tdb(self,source:Path) -> None:
        gnss_epochs: Optional[List[GNSSEpoch]] = extract_rangea_from_qcpin(source)
        if gnss_epochs is not None and len(gnss_epochs) > 0:
            self.qcGnssObsTDB.write_epochs(gnss_epochs)
    
    @validate_network_station_campaign
    def process_qcpin(self) -> None:
        """Process QC PIN files to generate preliminary shotdata.

        This method retrieves all QC PIN files from the asset catalog, converts them
        into ShotDataFrames using qcjson_to_shotdata, and stores the results
        in the QC-specific TileDB ShotData array.

        Raises
        ------
        NoQCPinFound
            If no QC PIN files are found for the current context.
        """
        qcpin_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.current_network_name,
                station=self.current_station_name,
                campaign=self.current_campaign_name,
                parent_type=AssetType.QCPIN,
                override=self.config.qcpin_config.override,
            )
        )
        if not qcpin_entries:
            response = f"No QCPIN Files Found for {self.current_network_name} {self.current_station_name} {self.current_campaign_name}"
            ProcessLogger.logerr(response)
            raise NoQCPinFound(response)

        response = f"Found {len(qcpin_entries)} QCPIN Files"
        ProcessLogger.loginfo(response)
        count = 0

        def process_single_qcpin(entry: AssetEntry) -> AssetEntry:
            try:
                self.qcpin_to_shotdata_tdb(entry.local_path)
                self.qcpin_to_gnssobs_tdb(entry.local_path)
                ProcessLogger.logdebug(f"Processed {entry.local_path}")
                entry.is_processed = True
                return entry
            except Exception as e:
                ProcessLogger.logerr(f"Error processing {entry.local_path}: {e}")
                return entry
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.qcpin_config.n_processes) as executor:
            futures = [executor.submit(process_single_qcpin, entry) for entry in qcpin_entries]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing QCPIN files"):
                result_entry = future.result()
                if result_entry.is_processed:
                    count += 1
                self.asset_catalog.add_or_update(result_entry)

        response = f"Processed {count} out of {len(qcpin_entries)} QCPIN Files"
        ProcessLogger.loginfo(response)

    @validate_network_station_campaign
    def get_rinex_files(self) -> None:
        """Generate and catalog daily RINEX files from QC GNSS data.

        This method generates RINEX files from the QC GNSS observation TileDB
        array for use in PRIDE-PPP processing.

        Raises
        ------
        NoRinexBuilt
            If no RINEX files could be generated.
        """
        rinexDestination = self.current_campaign_dir.intermediate

        if self.config.rinex_config.processing_year != -1:
            year = self.config.rinex_config.processing_year
        else:
            year = int(self.current_campaign_name.split("_")[0])

        ProcessLogger.loginfo(
            f"Generating QC Rinex Files for {self.current_network_name} {self.current_station_name} {year}. This may take a few minutes..."
        )

        parent_ids = f"N-{self.current_network_name}|ST-{self.current_station_name}|SV-{self.current_campaign_name}|TDB-{str(self.qcGnssObsTDB.uri)}|YEAR-{year}|QC"
        merge_signature = {
            "parent_type": AssetType.GNSSOBSTDB.value,
            "child_type": AssetType.RINEX2.value,
            "parent_ids": [parent_ids],
        }

        if (
            self.config.rinex_config.override
            or not self.asset_catalog.is_merge_complete(**merge_signature)
        ):
            try:
                rinex_paths: List[Path] = tile2rinex(
                    gnss_obs_tdb=self.qcGnssObsTDBURI,
                    settings=self.config.rinex_config.settings_path,
                    writedir=rinexDestination,
                    time_interval=self.config.rinex_config.time_interval,
                    processing_year=year,
                )

                if len(rinex_paths) == 0:
                    ProcessLogger.logwarn(
                        f"No QC Rinex Files generated for {self.current_network_name} {self.current_station_name} {self.current_campaign_name} {year}."
                    )
                    raise NoRinexBuilt(
                        "No QC RINEX files were built. Ensure GNSS data is available."
                    )

                rinex_entries: List[AssetEntry] = []
                uploadCount = 0
                for rinex_path in rinex_paths:
                    rinex_time_start, rinex_time_end = rinex_utils.rinex_get_time_range(
                        rinex_path
                    )
                    rinex_entry = AssetEntry(
                        local_path=rinex_path,
                        network=self.current_network_name,
                        station=self.current_station_name,
                        campaign=self.current_campaign_name,
                        timestamp_data_start=rinex_time_start,
                        timestamp_data_end=rinex_time_end,
                        type=AssetType.RINEX2,
                        timestamp_created=datetime.datetime.now(),
                    )
                    rinex_entries.append(rinex_entry)
                    if self.asset_catalog.add_entry(rinex_entry):
                        uploadCount += 1

                self.asset_catalog.add_merge_job(**merge_signature)

                ProcessLogger.loginfo(
                    f"Generated {len(rinex_entries)} QC Rinex files spanning {rinex_entries[0].timestamp_data_start} to {rinex_entries[-1].timestamp_data_end}"
                )
                ProcessLogger.logdebug(
                    f"Added {uploadCount} out of {len(rinex_entries)} Rinex files to the catalog"
                )

            except NoRinexBuilt:
                raise

            except Exception as e:
                if (
                    message := ProcessLogger.logerr(
                        f"Error generating QC RINEX files: {e}"
                    )
                ) is not None:
                    print(message)
                sys.exit(1)
        else:
            rinex_entries = self.asset_catalog.get_local_assets(
                self.current_network_name,
                self.current_station_name,
                self.current_campaign_name,
                AssetType.RINEX2,
            )
            num_rinex_entries = len(rinex_entries)
            ProcessLogger.logdebug(
                f"QC RINEX files have already been generated for {self.current_network_name}, {self.current_station_name}, and {year}. Found {num_rinex_entries} entries."
            )

    @validate_network_station_campaign
    def process_rinex(self) -> None:
        """Run PRIDE-PPP on RINEX files to generate KIN and residual files.

        Processing steps:
        1. Retrieves RINEX files needing processing
        2. Downloads GNSS product files (SP3, OBX, ATT)
        3. Runs PRIDE-PPPAR to convert RINEX to KIN format
        4. Adds KIN and residual files to asset catalog

        Raises
        ------
        NoRinexFound
            If no RINEX files are found for processing.
        """
        response = f"Running PRIDE-PPPAR on QC Rinex Data for {self.current_network_name} {self.current_station_name} {self.current_campaign_name}. This may take a few minutes..."
        ProcessLogger.loginfo(response)

        prideDir = self.directory_handler.pride_directory
        intermediateDir = self.current_campaign_dir.intermediate

        rinex_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.current_network_name,
                station=self.current_station_name,
                campaign=self.current_campaign_name,
                parent_type=AssetType.RINEX2,
                child_type=AssetType.KIN,
                override=self.config.pride_config.override,
            )
        )
        if not rinex_entries:
            response = f"No Rinex Files Found to Process for {self.current_network_name} {self.current_station_name} {self.current_campaign_name}"
            ProcessLogger.logerr(response)
            raise NoRinexFound(response)

        response = f"Found {len(rinex_entries)} Rinex Files to Process"
        ProcessLogger.loginfo(response)

        # Get PRIDE GNSS product files
        get_nav_file_partial = partial(
            get_nav_file, override=self.config.pride_config.override_products_download
        )
        get_pride_config_partial = partial(
            get_gnss_products,
            pride_dir=prideDir,
            override=self.config.pride_config.override_products_download,
        )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            nav_files = list(
                executor.map(
                    get_nav_file_partial, [x.local_path for x in rinex_entries]
                )
            )
            pride_configs = list(
                executor.map(
                    get_pride_config_partial, [x.local_path for x in rinex_entries]
                )
            )

        rinex_prideconfigs = [
            (rinex_entry, pride_config_path)
            for rinex_entry, pride_config_path in zip(rinex_entries, pride_configs)
            if pride_config_path is not None
        ]

        process_rinex_partial = partial(
            rinex_to_kin_wrapper,
            writedir=intermediateDir,
            pridedir=prideDir,
            site=self.current_station_name,
            pride_config=self.config.pride_config,
        )

        kin_count = 0
        res_count = 0
        uploadCount = 0

        with Pool(processes=self.config.rinex_config.n_processes) as pool:
            results = pool.map(process_rinex_partial, rinex_prideconfigs)

            for idx, (kinfile, resfile) in enumerate(
                tqdm(
                    results,
                    total=len(rinex_entries),
                    desc="Processing QC Rinex Files",
                    mininterval=0.5,
                )
            ):
                if kinfile is not None:
                    kin_count += 1
                    rinex_entries[idx].is_processed = True
                    self.asset_catalog.add_or_update(rinex_entries[idx])

                    if self.asset_catalog.add_or_update(kinfile):
                        uploadCount += 1

                    if resfile is not None:
                        res_count += 1
                        if self.asset_catalog.add_or_update(resfile):
                            uploadCount += 1

        response = f"Generated {kin_count} Kin Files and {res_count} Residual Files From {len(rinex_entries)} QC Rinex Files, Added {uploadCount} to the Catalog"
        ProcessLogger.loginfo(response)

    @validate_network_station_campaign
    def process_kin(self) -> None:
        """Process KIN files to generate QC kinematic position dataframes.

        Steps:
        1. Retrieves KIN files needing processing
        2. Converts each KIN file to a structured dataframe
        3. Writes dataframes to QC kinematic position TileDB array
        4. Marks files as processed in asset catalog

        Raises
        ------
        NoKinFound
            If no KIN files are found for processing.
        """
        ProcessLogger.loginfo(
            f"Looking for QC Kin Files to Process for {self.current_network_name} {self.current_station_name} {self.current_campaign_name}"
        )

        kin_entries: List[AssetEntry] = (
            self.asset_catalog.get_single_entries_to_process(
                network=self.current_network_name,
                station=self.current_station_name,
                campaign=self.current_campaign_name,
                parent_type=AssetType.KIN,
                override=self.config.rinex_config.override,
            )
        )

        if not kin_entries:
            message = f"No QC Kin Files Found to Process for {self.current_network_name} {self.current_station_name} {self.current_campaign_name}"
            ProcessLogger.loginfo(message)
            raise NoKinFound(message)

        ProcessLogger.loginfo(
            f"Found {len(kin_entries)} QC Kin Files to Process: processing"
        )

        processed_count = 0
        for kin_entry in tqdm(kin_entries, desc="Processing QC Kin Files"):
            try:
                kin_position_df = kin_to_kin_position_df(kin_entry.local_path)
                if kin_position_df is not None:
                    processed_count += 1
                    kin_entry.is_processed = True
                    self.asset_catalog.add_or_update(kin_entry)
                    self.qcKinPositionTDB.write_df(kin_position_df)
            except Exception as e:
                ProcessLogger.logerr(f"Error processing {kin_entry.local_path}: {e}")

        ProcessLogger.loginfo(
            f"Generated {processed_count} QC KinPosition Dataframes From {len(kin_entries)} Kin Files"
        )

    @validate_network_station_campaign
    def update_shotdata(self) -> None:
        """Refine QC shotdata with interpolated high-precision kinematic positions.

        Steps:
        1. Gets merge signature from preliminary shotdata and kinematic
           position arrays
        2. Checks if refinement is needed (via override or merge status)
        3. Merges shotdata with interpolated kinematic positions
        4. Writes refined shotdata to final QC TileDB array
        5. Records merge job in asset catalog
        """
        ProcessLogger.loginfo(
            "Updating QC shotdata with interpolated QCKinPosition data"
        )

        try:
            merge_signature, dates = get_merge_signature_shotdata(
                self.qcShotDataPreTDB, self.qcKinPositionTDB
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
            dates.append(dates[-1] + datetime.timedelta(days=1))
            merge_shotdata_kinposition(
                shotdata_pre=self.qcShotDataPreTDB,
                shotdata=self.qcShotDataFinalTDB,
                kin_position=self.qcKinPositionTDB,
                dates=dates,
                lengthscale=self.config.position_update_config.lengthscale,
                plot=self.config.position_update_config.plot,
            )
            self.asset_catalog.add_merge_job(**merge_job)

    @validate_network_station_campaign
    def run_pipeline(self) -> None:
        """Execute the complete QC data processing pipeline in sequence.

        Pipeline steps (in order):
        1. process_qcpin(): Process QC PIN files to generate shotdata
        2. parse_rangea_logs_from_qcpin(): Extract RANGEA logs for GNSS processing
        3. process_rangea_logs(): Process NOVATEL PIN files into TileDB
        4. get_rinex_files(): Generate RINEX files
        5. process_rinex(): Run PRIDE-PPP on RINEX
        6. process_kin(): Convert KIN files to dataframes
        7. update_shotdata(): Refine shotdata with high-precision positions

        Each step checks if processing is needed via config overrides or
        catalog status.
        """
        ProcessLogger.loginfo(
            f"Starting QC Processing Pipeline for {self.current_network_name} {self.current_station_name} {self.current_campaign_name}"
        )

        try:
            self.process_qcpin()
        except NoQCPinFound:
            pass
        
        try:
            self.get_rinex_files()
        except NoRinexBuilt:
            pass

        try:
            self.process_rinex()
        except NoRinexFound:
            pass

        try:
            self.process_kin()
        except NoKinFound:
            pass

        self.update_shotdata()

        ProcessLogger.loginfo(
            f"Completed QC Processing Pipeline for {self.current_network_name} {self.current_station_name} {self.current_campaign_name}"
        )
