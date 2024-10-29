from pathlib import Path
from es_sfgtools.processing.operations.pride_utils import get_nav_file,get_gnss_products,download
from es_sfgtools.processing.operations.gnss_resources import WuhanIGS,CDDIS,CLSIGS,GSSC

pride_dir = "" # pride directory
rinex_path = "" # daily rinex file

pride_dir = Path(pride_dir)
rinex_path = Path(rinex_path)

get_nav_file(rinex_path,pride_dir,override=True)
get_gnss_products(rinex_path,pride_dir,override=True)