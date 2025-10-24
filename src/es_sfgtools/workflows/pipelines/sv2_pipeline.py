"""


NOT YET IMPLEMENTED
"""



# # External Imports
# import datetime
# from tqdm.auto import tqdm
# from multiprocessing import Pool, cpu_count
# from functools import partial
# import pandas as pd
# from typing import List, Optional
# from pathlib import Path
# import concurrent.futures


# from es_sfgtools.data_models.metadata import MetaDataCatalog as Catalog

# # Local imports
# from ..data_mgmt.catalog import PreProcessCatalog
# from ..data_mgmt.data_handler import DataHandler
# from ..data_mgmt.file_schemas import AssetEntry, AssetType
# from ..sonardyne_tools import sv3_operations as sv3_ops
# from ..novatel_tools import (
#     novatel_binary_operations as novb_ops,
#     novatel_ascii_operations as nova_ops,
# )
# from ..tiledb_tools.tiledb_operations import tile2rinex
# from ..pride_tools import (
#     PrideCLIConfig,
#     rinex_to_kin,
#     kin_to_gnssdf,
#     get_nav_file,
#     get_gnss_products,
#     rinex_utils,
# )
# from ..tiledb_tools.tiledb_schemas import (
#     TDBGNSSArray,
#     TDBShotDataArray,
#     TDBGNSSObsArray,
# )
# from ..data_mgmt.utils import (
#     get_merge_signature_shotdata,
#     merge_shotdata_gnss,
# )
# from ..logging import ProcessLogger as logger, PRIDELogger as gnss_logger

# from .config import SV3PipelineConfig

# class SV2Pipeline:
#     # TODO this doesnt not work yet
#     def __init__(
#         self, catalog: PreProcessCatalog = None, config: SV3PipelineConfig = None
#     ):
#         self.catalog = catalog
#         self.config = config
#         if self.catalog is None:
#             self.catalog = PreProcessCatalog(self.config.catalog_path)

#     def process_novatel(self) -> None:

#         logger.loginfo(
#             f"Processing Novatel data for {self.network} {self.station} {self.campaign}"
#         )
#         novatel_entries: List[AssetEntry] = self.catalog.get_assets(
#             network=self.network,
#             station=self.station,
#             campaign=self.campaign,
#             type=AssetType.NOVATEL,
#         )

#         merge_signature = {
#             "parent_type": AssetType.NOVATEL.value,
#             "child_type": AssetType.RINEX2.value,
#             "parent_ids": [x.id for x in novatel_entries],
#         }
#         if self.config.novatel_config.override or not self.catalog.is_merge_complete(
#             **merge_signature
#         ):
#             rinex_entries: List[AssetEntry] = gnss_ops.novatel_to_rinex_batch(
#                 source=novatel_770_entries,
#                 writedir=self.inter_dir,
#                 show_details=self.config.novatel_config.show_details,
#             )
#             uploadCount = 0
#             for rinex_entry in rinex_entries:
#                 if self.catalog.add_entry(rinex_entry):
#                     uploadCount += 1
#             self.catalog.add_merge_job(**merge_signature)
#             response = f"Added {uploadCount} out of {len(rinex_entries)} Rinex Entries to the catalog"
#             logger.loginfo(response)
#             # if self.config.novatel_config.show_details:
#             #     print(response)
