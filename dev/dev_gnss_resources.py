from pathlib import Path
import logging
import sys
import datetime
root = logging.getLogger()
filemode = logging.FileHandler('dev_gnss_resources.log',mode='w')
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
root.addHandler(handler)
root.addHandler(filemode)
from es_sfgtools.pride_tools.pride_utils import get_nav_file,get_gnss_products,download,update_source
from es_sfgtools.pride_tools.gnss_product_schemas import WuhanIGS,CLSIGS,GSSC

pride_dir = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NFL1/Pride"
)
rinex_path = (
    Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NFL1/Cascadia/NFL1/2023/intermediate"
    )
    / "NFL11560.23o"
)


# get_nav_file(rinex_path,override=True,mode='process')
# get_gnss_products(rinex_path,pride_dir,override=True)
with open(rinex_path) as f:
    files = f.readlines()
    for line in files:
        if "TIME OF FIRST OBS" in line:
            time_values = line.split("GPS")[0].strip().split()
            start_date = datetime.date(
                year=int(time_values[0]),
                month=int(time_values[1]),
                day=int(time_values[2]),
            )
            break

year = str(start_date.year)
common_product_dir = pride_dir/year/"product"/"common"
common_product_dir.mkdir(exist_ok=True,parents=True)

sp3 = WuhanIGS.get_product_sp3(start_date)
clk = WuhanIGS.get_product_clk(start_date)
erp = WuhanIGS.get_product_erp(start_date)

rnx_3 = WuhanIGS.get_rinex_3_nav(start_date)

rnx_3_update = update_source(rnx_3)
sp3_update = update_source(sp3)
clk_update = update_source(clk)
erp_update = update_source(erp)

sp3_local_path = common_product_dir/sp3_update.file_name
clk_local_path = common_product_dir/clk_update.file_name
erp_local_path = common_product_dir/erp_update.file_name
rnx_3_local_path = rinex_path.parent/rnx_3_update.file_name

dl_rnx_3_local_path = get_nav_file(rinex_path,override=False)
if not rnx_3_local_path.exists():
    download(rnx_3_update,rnx_3_local_path)
if not sp3_local_path.exists():
    download(sp3_update,sp3_local_path)
if not clk_local_path.exists():
    download(clk_update,clk_local_path)
if not erp_local_path.exists():
    download(erp_update,erp_local_path)

get_gnss_products(rinex_path,pride_dir,override=True,source='cligs')
