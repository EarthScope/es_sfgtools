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

pride_dir = Path("/Users/gottlieb/working/GIT/es_sfgtools/data/Pride")
rinex_path = Path(
            "/Users/gottlieb/working/GIT/seafloor_geodesy_notebooks/notebooks/data/cascadia-gorda/NCC1/2022_A_1065/raw"
        )/ "NCC11250.22o"


get_nav_file(rinex_path,override=True,mode='test')
#get_gnss_products(rinex_path,pride_dir,override=True,mode='test')
