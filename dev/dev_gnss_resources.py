from pathlib import Path
import logging
import sys
root = logging.getLogger()
filemode = logging.FileHandler('dev_gnss_resources.log',mode='w')
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
root.addHandler(handler)
root.addHandler(filemode)
from es_sfgtools.processing.operations.pride_utils import get_nav_file,get_gnss_products,download
from es_sfgtools.processing.operations.gnss_resources import WuhanIGS,CDDIS,CLSIGS,GSSC

pride_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3/Pride")
rinex_path = Path(
            "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NCL1/NCB/NCB1/2023/intermediate"
        )/ "NCB11750.23o"


get_nav_file(rinex_path,override=True,mode='test')
get_gnss_products(rinex_path,pride_dir,override=True,mode='test')
