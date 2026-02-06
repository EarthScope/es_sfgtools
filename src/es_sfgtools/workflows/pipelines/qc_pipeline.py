# External Imports
import concurrent.futures
import datetime
import json
import sys
from functools import partial
from pathlib import Path
from typing import List, Optional

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
from es_sfgtools.tiledb_tools.tiledb_schemas import (
    TDBShotDataArray,
)
from es_sfgtools.sonardyne_tools.sv3_qc_operations import qcjson_to_shotdata
from es_sfgtools.workflows.pipelines.exceptions import NoLocalData
from ..utils.protocols import WorkflowABC,validate_network_station_campaign


class QCPipeline(WorkflowABC):
    def __init__(
        self,
        directory_handler: Optional[DirectoryHandler] = None,
        asset_catalog: Optional[PreProcessCatalogHandler] = None,
        config: dict = None,
    ):
        super().__init__(
            directory=directory_handler.location,
            asset_catalog=asset_catalog,
            directory_handler=directory_handler,
        )

        self.shotDataTDB: TDBShotDataArray = None
        self.config = config if config is not None else {}

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
            self.shotDataTDB = None
        

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

    def _build_tiledb_arrays(self) -> None:
        tiledb_dir = self.current_station_dir.tiledb_directory
        if self.shotDataTDB is None:
            self.shotDataTDB = TDBShotDataArray(
                tiledb_dir.location / "qc_shot_data"
            )

    def process_qc_files(self) -> None:
        """Process all QC files for the current network/station/campaign context.

        This method retrieves all QC files from the asset catalog, converts them
        into ShotDataFrames using the `qcjson_to_shotdata` function, and stores
        the results in the TileDB ShotData array.
        """
        qc_file_entries: List[AssetEntry] = self.asset_catalog.get_local_assets(
            self.current_network_name,
            self.current_station_name,
            self.current_campaign_name,
            AssetType.QCPIN,
        )
        if not qc_file_entries:
            ProcessLogger.logwarn(
                f"No QC files found for {self.current_network_name}/{self.current_station_name}/{self.current_campaign_name}"
            )
            return

        # Limit the number of files to process for debugging
        qc_file_entries = qc_file_entries[:10]

        to_process = [x for x in qc_file_entries if not (self.config.get("override", False) or x.is_processed)]
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=20) as pool:
            results = pool.map(
                qcjson_to_shotdata,
                [entry.local_path for entry in to_process],
            )
            for shotdata_df,asset_entry in zip(results, to_process):
                if shotdata_df is not None and not shotdata_df.empty:
                    self.shotDataTDB.write_df(shotdata_df)
                    asset_entry.is_processed = True
                    self.asset_catalog.add_or_update(asset_entry)