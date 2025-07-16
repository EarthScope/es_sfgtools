"""

Develop/Test ways to download GNSS related products and vet their integrity.
Also, develop ways to downsample RINEX PPP processing should higher frequencies fail.

"""

"""
warning: PrepareProducts: failed to download satellite attitude product: WUM0MGXRAP_20221260000_01D_30S_ATT.OBX.gz

2025-07-01 14:02:10,447 - gnss_ops.py:711 - ERROR - NCC11260.22o: gunzip: WUM0MGXRAP_20221260000_01D_30S_ATT.OBX.gz: unexpected end of file
2025-07-01 14:02:10,447 - gnss_ops.py:711 - ERROR - NCC11260.22o: gunzip: WUM0MGXRAP_20221260000_01D_30S_ATT.OBX.gz: uncompress failed
execution failed
"""

# from es_sfgtools.processing.operations.gnss_ops import (
#     rinex_to_kin,
#     get_gnss_products,
#     get_nav_file,
#     PridePdpConfig,
# )
from es_sfgtools.pride_tools.pride_utils import uncompress_file
from es_sfgtools.data_mgmt.data_handler import DataHandler
from pathlib import Path
import os
from es_sfgtools.utils.loggers import BaseLogger,GNSSLogger
from es_sfgtools.pride_tools.config import PRIDEPPPConfig,parse_pride_config

from es_sfgtools.pride_tools.pride_utils import get_gnss_products
bad_rinex = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/2024_A_1126/intermediate/NCC12790.24o"
)
pride_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/Pride")

uncompressed_dest_dir = pride_dir / "2024" / "126"
config_file_path = get_gnss_products(
    rinex_path=bad_rinex,
    pride_dir=pride_dir,
    override=False,
    source="all"
)
print(f"Config file path: {config_file_path}")
# # convert to paths
# for k,v in gnss_product_status.items():
#     gnss_product_status[k] = Path(v).resolve()

# decompressed_files = {}
# # attemp decompression
# for k, v in gnss_product_status.items():
#     if v.suffix == ".gz":
#         try:
#             # Attempt to uncompress the file
#             decompressed_files[k] = uncompress_file(v, uncompressed_dest_dir)
#         except Exception as e:
#             GNSSLogger.error(f"Failed to decompress {v}: {e}")

config_path = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/Pride/2022/126/config_template")
test_config_out = config_path.parent / "test_config"

with config_path.open() as f:
    config = parse_pride_config(f.read())

config.write_config_file(test_config_out)
config_2 = PRIDEPPPConfig.read_config_file(test_config_out)

assert config == config_2, "Config objects do not match after writing and reading."
