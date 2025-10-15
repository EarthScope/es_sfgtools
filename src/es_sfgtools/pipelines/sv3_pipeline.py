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

# Local imports
from ..data_mgmt.assetcatalog.catalog import PreProcessCatalog
from ..data_mgmt.directorymgmt.directory_handler import (
    CampaignDir,
    DirectoryHandler,
    NetworkDir,
    StationDir,
)
from ..data_mgmt.assetcatalog.file_schemas import AssetEntry, AssetType
from ..data_mgmt.utils import (
    get_merge_signature_shotdata,
)
from ..logging import ProcessLogger, change_all_logger_dirs
from ..novatel_tools import novatel_binary_operations as novb_ops
from ..novatel_tools.utils import get_metadata, get_metadatav2
from ..pride_tools import (
    PrideCLIConfig,
    get_gnss_products,
    get_nav_file,
    kin_to_kin_position_df,
    rinex_to_kin,
    rinex_utils,
)
from ..seafloor_site_tools.soundspeed_operations import (
    CTD_to_svp_v1,
    CTD_to_svp_v2,
    seabird_to_soundvelocity,
)
from ..sonardyne_tools import sv3_operations as sv3_ops
from ..tiledb_tools.tiledb_operations import tile2rinex
from ..tiledb_tools.tiledb_schemas import (
    TDBIMUPositionArray,
    TDBKinPositionArray,
    TDBShotDataArray,
)
from .config import SV3PipelineConfig
from .shotdata_gnss_refinement import merge_shotdata_kinposition
from .exceptions import NoRinexFound, NoNovatelFound,NoRinexBuilt,NoKinFound,NoDFOP00Found,NoSVPFound

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

    :param rinex_prideconfig_path: Tuple containing the RINEX AssetEntry and PRIDE config file path.
    :type rinex_prideconfig_path: tuple[AssetEntry, Path]
    :param writedir: Directory where output files should be written.
    :type writedir: Path
    :param pridedir: Directory for PRIDE processing.
    :type pridedir: Path
    :param site: Name of the site/station.
    :type site: str
    :param pride_config: PRIDE CLI configuration object.
    :type pride_config: PrideCLIConfig
    :return: AssetEntry for the generated KIN file and AssetEntry for the residuals file, or (None, None) if conversion fails.
    :rtype: tuple[Optional[AssetEntry], Optional[AssetEntry]]
    :raises Exception: If an error occurs during AssetEntry creation for KIN or RES file.
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
    """
    Orchestrates the end-to-end processing of Sonardyne SV3 and Novatel GNSS data for seafloor geodesy.
    
    This class manages a comprehensive workflow for processing seafloor geodesy data, including:
    
    1. **GNSS Data Preprocessing**:
       - Processes Novatel 770 binary files (primary GNSS observations)
       - Processes Novatel 000 binary files (secondary GNSS + IMU positions)
       - Stores observations in TileDB arrays for efficient access
    
    2. **RINEX Generation**:
       - Converts TileDB GNSS observations to daily RINEX files
       - Manages RINEX metadata and file organization
    
    3. **Precise Point Positioning**:
       - Downloads GNSS product files (SP3, OBX, ATT)
       - Runs PRIDE-PPPAR for high-precision positioning
       - Generates kinematic (KIN) and residual files
    
    4. **Kinematic Position Processing**:
       - Converts KIN files to structured dataframes
       - Stores kinematic positions in TileDB for interpolation
    
    5. **Acoustic Data Processing**:
       - Processes Sonardyne DFOP00 files (acoustic ping-reply sequences)
       - Generates preliminary shotdata with acoustic ranges
    
    6. **Shotdata Refinement**:
       - Interpolates high-precision GNSS positions to acoustic ping times
       - Refines shotdata with improved position estimates
    
    7. **Sound Velocity Profile Processing**:
       - Processes CTD and Seabird files
       - Generates sound velocity profiles for acoustic corrections
    
    The pipeline operates on a hierarchical directory structure (network/station/campaign)
    and uses TileDB for efficient storage and retrieval of time-series data.
    
    :ivar directory_handler: Manages the project directory structure, including network, station, and campaign directories.
    :vartype directory_handler: DirectoryHandler
    :ivar config: Configuration settings for all pipeline steps, including Novatel, RINEX, PRIDE, DFOP00, and position update configs.
    :vartype config: SV3PipelineConfig
    :ivar asset_catalog: SQLite-based catalog for tracking processed assets and their relationships (parent-child, merge jobs).
    :vartype asset_catalog: PreProcessCatalog
    :ivar currentNetwork: Current network identifier (e.g., "cascadia-gorda")
    :vartype currentNetwork: str
    :ivar currentStation: Current station identifier (e.g., "NCC1")
    :vartype currentStation: str
    :ivar currentCampaign: Current campaign identifier (e.g., "2023_A_1126")
    :vartype currentCampaign: str
    :ivar currentNetworkDir: Directory object for current network.
    :vartype currentNetworkDir: NetworkDir
    :ivar currentStationDir: Directory object for current station.
    :vartype currentStationDir: StationDir
    :ivar currentCampaignDir: Directory object for current campaign.
    :vartype currentCampaignDir: CampaignDir
    :ivar shotDataPreTDB: Preliminary shotdata (before position refinement).
    :vartype shotDataPreTDB: TDBShotDataArray
    :ivar kinPositionTDB: High-precision kinematic positions.
    :vartype kinPositionTDB: TDBKinPositionArray
    :ivar imuPositionTDB: IMU-derived positions (from Novatel 000).
    :vartype imuPositionTDB: TDBIMUPositionArray
    :ivar shotDataFinalTDB: Final shotdata (after position refinement).
    :vartype shotDataFinalTDB: TDBShotDataArray
    :ivar gnssObsTDBURI: Primary GNSS observation array (from Novatel 770).
    :vartype gnssObsTDBURI: Path
    :ivar gnssObsTDB_secondaryURI: Secondary GNSS observation array (from Novatel 000).
    :vartype gnssObsTDB_secondaryURI: Path

    .. py:method:: setNetworkStationCampaign(network, station, campaign)

        Set the current processing context and initialize directories and TileDB arrays.

    .. py:method:: _build_rinex_metadata()

        Prepare metadata for RINEX file generation from GNSS observations.

    .. py:method:: pre_process_novatel()

        Preprocess Novatel 770 and 000 binary files into TileDB arrays.

    .. py:method:: get_rinex_files()

        Generate daily RINEX files from TileDB GNSS observations.

    .. py:method:: process_rinex()

        Process RINEX files using PRIDE-PPPAR to generate Kinematic files.

    .. py:method:: process_kin()

        Convert Kinematic files to structured dataframes and store in TileDB.

    .. py:method:: process_dfop00()

        Process Sonardyne DFOP00 files to generate preliminary shotdata.

    .. py:method:: update_shotdata()

        Refine shotdata by interpolating high-precision GNSS positions.

    .. py:method:: process_svp()

        Process CTD and Seabird files to generate sound velocity profiles.

    .. py:method:: run_pipeline()

        Execute the full processing pipeline in sequence.
    """

    def __init__(
        self,
        directory_handler: DirectoryHandler = None,
        config: SV3PipelineConfig = None,
    ):
        """
        Initializes the SV3Pipeline with directory handler and configuration.
        
        Sets up the pipeline with necessary infrastructure including:
        - Directory structure management
        - Asset catalog for tracking processed files
        - Configuration for all processing steps
        - Context attributes (network, station, campaign)
        
        :param directory_handler: Handler for managing project directory structure. Must be provided and should already be built.
        :type directory_handler: DirectoryHandler, optional
        :param config: Configuration settings for the pipeline. If None, uses default configuration. Defaults to None.
        :type config: Optional[SV3PipelineConfig], optional
        :raises AttributeError: If directory_handler is None or doesn't have asset_catalog_db_path
        
        .. note::
            The pipeline will not be ready for processing until :py:meth:`setNetworkStationCampaign`
            is called to establish the processing context.
        """
        # Store directory handler and configuration
        self.directory_handler = directory_handler
        self.config = config if config is not None else SV3PipelineConfig()
        
        # Initialize asset catalog from directory handler's database path
        self.asset_catalog = PreProcessCatalog(self.directory_handler.asset_catalog_db_path)

        # Initialize current processing context to None
        # These will be set when setNetworkStationCampaign() is called
        self.currentNetwork: str = None  # e.g., "cascadia-gorda"
        self.currentStation: str = None  # e.g., "NCC1"
        self.currentCampaign: str = None  # e.g., "2023_A_1126"

        # Initialize directory objects for current context
        # These provide access to subdirectories and files for the current campaign
        self.currentNetworkDir: NetworkDir = None
        self.currentStationDir: StationDir = None
        self.currentCampaignDir: CampaignDir = None

        # Initialize TileDB array objects to None
        # These will be created when setNetworkStationCampaign() is called
        self.shotDataPreTDB: TDBShotDataArray = None  # Preliminary shotdata (before refinement)
        self.kinPositionTDB: TDBKinPositionArray = None  # High-precision kinematic positions
        self.imuPositionTDB: TDBIMUPositionArray = None  # IMU positions from Novatel 000
        self.shotDataFinalTDB: TDBShotDataArray = None  # Final shotdata (after refinement)

    def setNetworkStationCampaign(
        self,
        network: str,
        station: str,
        campaign: str,
    
    ) -> None:
        """
        Set the current network, station, and campaign context for pipeline processing.
        
        This method establishes the processing context and performs several initialization tasks:
        1. Resets previous context
        2. Validates data availability
        3. Creates directory structure
        4. Initializes TileDB arrays
        5. Configures logging
        6. Prepares RINEX metadata
        
        :param network: Network identifier (e.g., "cascadia-gorda").
        :type network: str
        :param station: Station identifier (e.g., "NCC1").
        :type station: str
        :param campaign: Campaign identifier (e.g., "2023_A_1126").
        :type campaign: str
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

        # Build the campaign directory structure, this changes the logger directory as well
        networkDir, stationDir, campaignDir, _ = self.directory_handler.build_station_directory(
            network_name=network, station_name=station, campaign_name=campaign
        )
        self.currentNetworkDir = networkDir
        self.currentStationDir = stationDir
        self.currentCampaignDir = campaignDir

        self.currentNetwork = network
        self.currentStation = station
        self.currentCampaign = campaign

        # Update all log directories
        change_all_logger_dirs(campaignDir.log_directory)

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
        Build RINEX metadata files for the current campaign if they don't exist.
        
        Creates two metadata files:
        - rinex_metav2.json: Updated format with metadata
        - rinex_metav1.json: Legacy format for backward compatibility
        
        These files contain station-specific information needed for RINEX generation.
        """

        # Get the RINEX metadata
        rinex_metav2 = (
            self.currentCampaignDir.metadata_directory
            / "rinex_metav2.json"
        )
        rinex_metav1 = (
            self.currentCampaignDir.metadata_directory
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
        Preprocess Novatel 770 and 000 binary files for the current context.
        
        :rtype: None
        :raises Exception: If no Novatel 770 or 000 files are found.

        Processing steps:
        1. **Novatel 770**: Extracts GNSS observations to primary TileDB array
        2. **Novatel 000**: Extracts GNSS observations to secondary array + IMU positions
        
        Both steps check if processing is needed (via override config or merge status)
        and update the asset catalog upon completion.


        """

        """
        Process Novatel 770 files
        1. Query asset catalog for Novatel 770 files for current context
        2. If files exist, check if processing is needed (override or not merged)
        3. Call novatel_770_2tile to process files into TileDB GNSS observation array
        4. Update asset catalog with merge job
        """
        found_novatel_770 = False
        found_novatel_000 = False


        novatel_770_entries: List[AssetEntry] = self.asset_catalog.get_local_assets(
            network=self.currentNetwork,
            station=self.currentStation,
            campaign=self.currentCampaign,
            type=AssetType.NOVATEL770,
        )

        if novatel_770_entries:
            found_novatel_770 = True
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

        """
        Process Novatel 000 files
        1. Query asset catalog for Novatel 000 files for current context
        2. If files exist, check if processing is needed (override or not merged)
        3. Call novatel_000_2tile to process files into TileDB GNSS observation array + IMU positions
        4. Update asset catalog with merge job
        
        """
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
            found_novatel_000 = True
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
        
        if not found_novatel_770 and not found_novatel_000:
            raise NoNovatelFound(f"No Novatel 770 or 000 files found for {self.currentNetwork} {self.currentStation} {self.currentCampaign}. Cannot proceed with GNSS processing.")

    def get_rinex_files(self) -> None:
        """
        Generate and catalog daily RINEX files for the current campaign.
        
        Steps:
        1. Consolidates GNSS observation data
        2. Determines processing year from config or campaign name
        3. Invokes tile2rinex to generate daily RINEX files
        4. Creates AssetEntry for each RINEX file
        5. Updates asset catalog with merge job
        
        :raises ValueError: If a processing year cannot be determined from the campaign name.
        :raises Exception: If an error occurs during RINEX file generation.
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
        parent_ids = f"N-{self.currentNetwork}|ST-{self.currentStation}|SV-{self.currentCampaign}|TDB-{gnss_obs_data_dest}|YEAR-{year}"
        merge_signature = {
            "parent_type": AssetType.GNSSOBSTDB.value,
            "child_type": AssetType.RINEX.value,
            "parent_ids": [parent_ids],
        }

        if (
            self.config.rinex_config.override
            or not self.asset_catalog.is_merge_complete(**merge_signature)
        ):
            """
            Process GNSS observation data into RINEX format.
            1. Calls tile2rinex to generate daily RINEX files
            2. Creates AssetEntry for each RINEX file
            3. Adds RINEX files to asset catalog
            4. Updates asset catalog with merge job
            5. Logs summary information
            """
            try:
                rinex_paths: List[Path] = tile2rinex(
                    gnss_obs_tdb=self.gnssObsTDBURI,
                    settings=self.config.rinex_config.settings_path,
                    writedir=rinexDestination,  # where to write the RINEX files
                    time_interval=self.config.rinex_config.time_interval, # seconds
                processing_year=year,  
                )

                if len(rinex_paths) == 0:
                    ProcessLogger.logwarn(
                        f"No Rinex Files generated for {self.currentNetwork} {self.currentStation} {self.currentCampaign} {year}."
                    )
                    raise NoRinexBuilt("No RINEX files were built. Try running self.pre_process_novatel() to ensure GNSS data is available.")
                    
                rinex_entries: List[AssetEntry] = []
                uploadCount = 0
                for rinex_path in rinex_paths:
                    # Get the start and end time from the RINEX file for metadata
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

            except NoRinexBuilt as e:
                raise e
            
            except Exception as e:
                if (message := ProcessLogger.logerr(f"Error generating RINEX files: {e}")) is not None:
                    print(message)
            
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
        Run PRIDE-PPP on RINEX files to generate KIN and residual files.
        
        Processing steps:
        1. Retrieves RINEX files needing processing
        2. Downloads GNSS product files (SP3, OBX, ATT) for each unique DOY
        3. Runs PRIDE-PPPAR in parallel to convert RINEX to KIN format
        4. Adds KIN and residual files to asset catalog
        
        Uses multiprocessing for efficient parallel processing of multiple RINEX files.
        """

        response = f"Running PRIDE-PPPAR on Rinex Data for {self.currentNetwork} {self.currentStation} {self.currentCampaign}. This may take a few minutes..."
        ProcessLogger.loginfo(response)

        # Get the PRIDE directory and intermediate directory
        prideDir = self.directory_handler.pride_directory
        intermediateDir = self.currentCampaignDir.intermediate


        # Get the Rinex files to process
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
            raise NoRinexFound(response)

        response = f"Found {len(rinex_entries)} Rinex Files to Process"
        ProcessLogger.loginfo(response)

        """
        Get the PRIDE GNSS product files for each unique DOY

        1, Build partial functions for getting nav files and pride config files
        2. Use ThreadPoolExecutor to download files in parallel
        3. Create list of tuples (rinex_entry, pride_config_path) for processing
        4. Filter out any entries where pride_config_path is None
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
        """
        Now process the Rinex files in parallel
        1. Convert each Rinex to KIN using the appropriate PRIDE config
        2. Create AssetEntry for each KIN and residual file
        3. Add to the catalog
        4. Mark Rinex as processed
   
        """

        # Build the partial function for multi processing Rinex to KIN
        process_rinex_partial = partial(
            rinex_to_kin_wrapper,
            writedir=intermediateDir,
            pridedir=prideDir,
            site=self.currentStation,
            pride_config=self.config.pride_config,
        )
        kin_entries = []
        resfile_entries = []
        kin_count = 0
        res_count = 0
        uploadCount = 0

        with Pool(
            processes=self.config.rinex_config.n_processes
        ) as pool:

            # Map the processing function over the Rinex files
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
                    kin_count += 1
                    rinex_entries[idx].is_processed = True
                    self.asset_catalog.add_or_update(rinex_entries[idx])

                    if self.asset_catalog.add_or_update(kinfile):
                        uploadCount += 1

                    if resfile is not None:
                        res_count += 1
                        
                        if self.asset_catalog.add_or_update(resfile):
                            uploadCount += 1
                            resfile_entries.append(resfile)


        response = f"Generated {kin_count} Kin Files and {res_count} Residual Files From {len(rinex_entries)} Rinex Files, Added {uploadCount} to the Catalog"
        ProcessLogger.loginfo(response)

    def process_kin(self):
        """
        Process KIN files to generate kinematic position dataframes.
        
        Steps:
        1. Retrieves KIN files needing processing
        2. Converts each KIN file to a structured dataframe
        3. Writes dataframes to kinematic position TileDB array
        4. Marks files as processed in asset catalog
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
            message = f"No Kin Files Found to Process for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
            ProcessLogger.loginfo(message)
            raise NoKinFound(message)

        ProcessLogger.loginfo(f"Found {len(kin_entries)} Kin Files to Process: processing")

        # Process KIN files to generate kinematic position dataframes
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
        Process Sonardyne DFOP00 files to generate preliminary shotdata.
        
        Steps:
        1. Retrieves DFOP00 files needing processing
        2. Converts each file to shotdata dataframe (acoustic ping-reply sequences)
        3. Writes dataframes to preliminary shotdata TileDB array
        4. Marks files as processed in asset catalog
        
        Uses multiprocessing for efficient parallel processing.
        """

        # 1. Get the DFOP00 files to process
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
            raise NoDFOP00Found(response)

        response = f"Found {len(dfop00_entries)} DFOP00 Files to Process"
        ProcessLogger.loginfo(response)
        count = 0

        # 2. Process DFOP00 files to generate shotdata dataframes
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
                    dfo_entry.is_processed = True # mark as processed
                    self.asset_catalog.add_or_update(dfo_entry)
                    ProcessLogger.logdebug(f" Processed {dfo_entry.local_path}")
                else:
                    ProcessLogger.logerr(f"Failed to Process {dfo_entry.local_path}")

        response = f"Generated {count} ShotData dataframes From {len(dfop00_entries)} DFOP00 Files"
        ProcessLogger.loginfo(response)

    def update_shotdata(self):
        """
        Refine shotdata with interpolated high-precision kinematic positions.
        
        Steps:
        1. Gets merge signature from preliminary shotdata and kinematic position arrays
        2. Checks if refinement is needed (via override or merge status)
        3. Merges shotdata with interpolated kinematic positions
        4. Writes refined shotdata to final TileDB array
        5. Records merge job in asset catalog
        
        This step significantly improves position accuracy by replacing GNSS positions
        with interpolated PRIDE-PPP solutions.
        """

        ProcessLogger.loginfo("Updating shotdata with interpolated KinPosition data")

        # 1. Get the merge signature
        try:
            merge_signature, dates = get_merge_signature_shotdata(
                self.shotDataPreTDB, self.kinPositionTDB
            )
        except Exception as e:
            ProcessLogger.logerr(e)
            return
        merge_job = {
            "parent_type": AssetType.KINPOSITION.value,
            "child_type": AssetType.SHOTDATA.value,
            "parent_ids": merge_signature,
        }
        # 2. Check if processing is needed
        if (
            not self.asset_catalog.is_merge_complete(**merge_job)
            or self.config.position_update_config.override
        ):
            # 3. Merge shotdata with interpolated kinematic positions
            merge_shotdata_kinposition(
                shotdata_pre=self.shotDataPreTDB,
                shotdata=self.shotDataFinalTDB,
                kin_position=self.kinPositionTDB,
                position_data=self.imuPositionTDB,
                dates=dates,
             
            )
            self.asset_catalog.add_merge_job(**merge_job)

    def process_svp(self, override: bool = False) -> None:
        """
        Process CTD and Seabird files to generate sound velocity profiles (SVP).
        
        :param override: If True, forces reprocessing even if SVP file exists. Default is False.
        :type override: bool, optional

        Processing order:
        1. Tries CTD files with CTD_to_svp_v2
        2. If that fails, tries CTD_to_svp_v1
        3. If still no success, tries Seabird files
        
        The first successful SVP is saved to the campaign directory and processing stops.
        """
        svp_df_destination = self.currentCampaignDir.svp_file
        if svp_df_destination.exists() and not override:
            return
        
        # Get the CTD and Seabird files to process
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
            raise NoSVPFound(response)
        
        ctd_processing_functions = [CTD_to_svp_v2, CTD_to_svp_v1]

        # Try processing CTD files first
        for ctd_entry in ctd_entries:
            for function in ctd_processing_functions:
                try:
                    svp_df = function(ctd_entry.local_path)
                    if not svp_df.empty:
                        svp_df.to_csv(svp_df_destination, index=False)
                        ctd_entry.is_processed = True
                        self.asset_catalog.add_or_update(ctd_entry) # mark as processed
                        ProcessLogger.loginfo(f"Processed SVP data from CTD file {ctd_entry.local_path} to dataframe with {function.__name__}")
                        ProcessLogger.loginfo(f"Saved SVP dataframe to {str(svp_df_destination)}")
                        return
                except Exception as e:
                    ProcessLogger.logerr(f"Error processing CTD file {ctd_entry.local_path} with {function.__name__}: {e}")
                    continue
        
        # If no CTD files produced SVP, try Seabird files
        for seabird_entry in seabird_entries:
            try:
                svp_df = seabird_to_soundvelocity(seabird_entry.local_path)
                if not svp_df.empty:
                    svp_df.to_csv(svp_df_destination, index=False)
                    seabird_entry.is_processed = True
                    self.asset_catalog.add_or_update(seabird_entry)
                    ProcessLogger.loginfo(f"Processed SVP data from Seabird file {seabird_entry.local_path} and saved to {str(svp_df_destination)}")
                    return
            except Exception as e:
                ProcessLogger.logerr(f"Error processing Seabird file {seabird_entry.local_path}: {e}")
                continue

    def run_pipeline(self):
        """
        Execute the complete SV3 data processing pipeline in sequence.
        
        Pipeline steps (in order):
        1. pre_process_novatel(): Process Novatel GNSS data
        2. get_rinex_files(): Generate RINEX files
        3. process_rinex(): Run PRIDE-PPP on RINEX
        4. process_kin(): Convert KIN files to dataframes
        5. process_dfop00(): Process acoustic data
        6. update_shotdata(): Refine shotdata with high-precision positions
        7. process_svp(): Generate sound velocity profile
        
        Each step checks if processing is needed via config overrides or catalog status.
        """
        if (
            self.currentNetwork is None
            or self.currentStation is None
            or self.currentCampaign is None
        ):
            ProcessLogger.logerr(
                "Pipeline context not set. Please call setNetworkStationCampaign() before running the pipeline."
            )
            return

        ProcessLogger.loginfo(
            f"Starting SV3 Processing Pipeline for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
        )
        try:
            self.pre_process_novatel()
        except NoNovatelFound as e:
            pass

        try:
            self.get_rinex_files()
        except NoRinexBuilt as e:
            pass
        
        try:
            self.process_rinex()
        except NoRinexFound as e:
            pass

        try:
            self.process_kin()
        except NoKinFound as e:
            pass

        try:
            self.process_dfop00()
        except NoDFOP00Found as e:
            pass

        self.update_shotdata()

        try:
            self.process_svp()
        except NoSVPFound as e:
            pass

        ProcessLogger.loginfo(
            f"Completed SV3 Processing Pipeline for {self.currentNetwork} {self.currentStation} {self.currentCampaign}"
        )