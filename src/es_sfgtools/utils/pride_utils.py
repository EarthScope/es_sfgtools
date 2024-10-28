import datetime
from pathlib import Path
from ftplib import FTP
from typing import IO,Dict,Optional,List
import wget
import gzip
import tempfile
import logging
from pydantic import BaseModel,field_validator
import shutil
import warnings

from es_sfgtools.utils.gnss_resources import RemoteResource,WuhanIGS,CLSIGS,CDDIS,GSSC
logger = logging.getLogger(__name__)    
# TODO use ftplib to download files
# https://docs.python.org/3/library/ftplib.html


# POTSDAM_SP3 = RemoteResource(
#     ftpserver="ftp://isdcftp.gfz-potsdam.de",
#     directory=["gnss","products","final"],
# )
def download(source:RemoteResource,dest:Path) ->Path:
    
    with FTP(source.ftpserver.replace("ftp://","")) as ftp:
        ftp.login()
        ftp.cwd("/" + source.directory)
        with open(dest,"wb") as f:
            ftp.retrbinary(f"RETR {source.file}",f.write)
    return dest

def uncompressed_file(file_path:Path) ->Path:
    """
    Decompresses a file using zlib and returns the path of the decompressed file.
    Args:
        file (Path): The path of the compressed file.
    Returns:
        Path: The path of the decompressed file.
    Raises:
        FileNotFoundError: If the file does not exist.
    Examples:
        >>> file = Path("data/brdc1500.21n.gz")
        >>> uncompressed_file(file)
        Path("data/brdc1500.21n")
    """
    # Ensure the file exists
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")

    # Define the output file path by removing the .gz extension
    out_file_path = file_path.with_suffix("")

    # Open the .gz file and read the decompressed data
    with gzip.open(file_path, "rb") as f_in:
        with open(out_file_path, "wb") as f_out:
            f_out.write(f_in.read())
    file_path.unlink()
    return out_file_path


def get_daily_rinex_url(date:datetime.date) ->Dict[str,Dict[str,RemoteResource]]:
    """
    This function returns the url for the IGS rinex observation file for a given date.
    url config docs at https://igs.org/products-access/#gnss-broadcast-ephemeris

    Args:
        date (datetime.date): The date for which the rinex observation file is required.
    Returns:
        Dict[str,Dict[str,RemoteResource]]: A dictionary containing the urls for the rinex 2 and 3 observation files.
    Examples:
        >>> date = datetime.date(2021,1,1)
        >>> urls = get_daily_rinex_url(date)
        >>> str(urls["rinex_2"]["wuhan_gps"])
        "ftp://igs.gnsswhu.cn/pub/gps/data/daily/21/001/21n/brdc0010.21n.Z"
        >>> str(urls["rinex_2"]["wuhan_glonass"])
        "ftp://igs.gnsswhu.cn/pub/gps/data/daily/21/001/21g/brdc0010.21g.Z"
    """

    

    urls = {
        "rinex_2": {
            "wuhan":{
                "glonass": WuhanIGS.get_rinex_2_nav(date,constellation="glonass"),
                "gps":WuhanIGS.get_rinex_2_nav(date,constellation="gps")
                },
            "cdds":{
                "glonass": CDDIS.get_rinex_2_nav(date,constellation="glonass"),
                "gps":CDDIS.get_rinex_2_nav(date,constellation="gps")
            },
        
            "gssc":{
                "glonass":GSSC.get_rinex_2_nav(date,constellation="glonass"),
                "gps":GSSC.get_rinex_2_nav(date,constellation="gps")
            }
        },
        "rinex_3": {
            "wuhan_gps": WuhanIGS.get_rinex_3_nav(date),
            "igs_gnss": CLSIGS.get_rinex_3_nav(date),
            "cddis_gnss": CDDIS.get_rinex_3_nav(date),
            "gssc_gnss": GSSC.get_rinex_3_nav(date),
        }
    }
    return urls

def get_gnss_common_products(date:datetime.date) ->Dict[str,Dict[str,Path]]:
    urls = {
        "sp3": {
            "wuhan": WuhanIGS.get_product_sp3(date),
            "cligs": CLSIGS.get_product_sp3(date),
        },
        "clk": {
            "wuhan": WuhanIGS.get_product_clk(date),
            "cligs": CLSIGS.get_product_clk(date),
        },
        "bias": {
            "wuhan": WuhanIGS.get_product_bias(date),
            "cligs": CLSIGS.get_product_bias(date),
        },
        "obx": {
            "wuhan": WuhanIGS.get_product_obx(date),
        },
        "erp": {
            "wuhan": WuhanIGS.get_product_erp(date),
        },
    }
    return urls

def merge_broadcast_files(brdn:Path, brdg:Path, output_folder:Path) ->Path:
    """
    Merge GPS and GLONASS broadcast ephemerides into a single BRDM file.
    Functionality inspired by https://github.com/PrideLab/PRIDE-PPPAR/blob/master/scripts/merge2brdm.py

    Args:
        brdn (Path): Path to the GPS broadcast ephemerides file.
        brdg (Path): Path to the GLONASS broadcast ephemerides file.
        output_folder (Path): Path to the output folder where the merged file will be saved.
    Returns:
        Path: Path to the merged BRDM file.
    Raises:
        FileNotFoundError: If either brdn or brdg file does not exist.
        Exception: If an unexpected error occurs while reading or writing the files.
    Examples:
        >>> brdn = Path("data/brdc1500.21n")
        >>> brdg = Path("data/brdc1500.21g")
        >>> output_folder = Path("data")
        >>> merged_brdm = merge_broadcast_files(brdn, brdg, output_folder)
        >>> merged_brdm
        Path("data/brdm1500.21p")
    """

    def write_data(file:Path, prefix:str, fm:IO):
        """
        Writes data from a file to a given output stream.

        Args:
            file (Path): The path to the file to be read.
            prefix (str): The prefix to be added to each line of data.
            fm (IO): The output stream to write the data to.

        Returns:
            None

        Raises:
            None
        """
        try:
            fn = open(file)
            lines = fn.readlines()
            in_header = True
        except Exception as e:
            print(f"***ERROR: unable to open or read file {file}: {e}")
            return

        i = 1
        while i <= len(lines):
            try:
                if not in_header:
                    line = lines[i].replace("D", "e")
                    prn = int(line[0:2])
                    yyyy = int(line[3:5]) + 2000
                    mm = int(line[6:8])
                    dd = int(line[9:11])
                    hh = int(line[12:14])
                    mi = int(line[15:17])
                    ss = round(float(line[18:22]))
                    num2 = eval(line[22:41])
                    num3 = eval(line[41:60])
                    num4 = eval(line[60:79])
                    fm.write(
                        f"{prefix}{prn:02d} {yyyy:04d} {mm:02d} {dd:02d} {hh:02d} {mi:02d} {ss:02d} {num2:.12e} {num3:.12e} {num4:.12e}\n"
                    )

                    for t in range(1, 7):
                        line = lines[i + t].replace("D", "e")
                        num1 = eval(line[3:22])
                        num2 = eval(line[22:41])
                        num3 = eval(line[41:60])
                        num4 = eval(line[60:79])
                        fm.write(
                            f"    {num1:.12e} {num2:.12e} {num3:.12e} {num4:.12e}\n"
                        )
                    line = lines[i + 7].replace("D", "e")
                    num1 = eval(line[3:22])
                    num2 = eval(line[22:41])
                    fm.write(f"    {num1:.12e} {num2:.12e}\n")
                    i += 8
                    if i >= len(lines):
                        break
                else:
                    if "END OF HEADER" in lines[i][60:73]:
                        in_header = False
                    fm.write(lines[i])
                    i += 1
            except Exception as e:
                print(
                    f"***ERROR: unexpected ERROR occurred at line {i} of file {file}: {e}"
                )
                print(lines[i])
                break

        fn.close()

    DDD = brdn.name[4:7]
    YY = brdn.name[9:11]
    if brdg.name[4:7] != DDD or brdg.name[9:11] != YY:
        print("***ERROR: inconsistent file name:")
        print(f"  {brdn} {brdg}")
        return

    brdm = output_folder / f"brdm{DDD}0.{YY}p"
    fm = open(brdm, "w")
    fm.write(
        "     3.04           NAVIGATION DATA     M (Mixed)           RINEX VERSION / TYPE\n"
    )
    write_data(brdn, "G", fm)
    write_data(brdg, "R", fm)
    fm.close()

    if brdm.exists():
        print(f"Files merged into {brdm}")
        return True
    return False


def get_nav_file(rinex_path:Path) -> Path:
    """
    Attempts to build a navigation file for a given RINEX file by downloading the necessary files from the IGS FTP server.

    Args:
        rinex_path (Path): The path to the RINEX file.
    Returns:
        brdm_path (Path): The path to the navigation file.
    Raises:
        Warning: If the navigation file cannot be built or located.

    Examples:
        >>> rinex_path = Path("data/NCB11750.23o")
        >>> brdm_path = get_nav_file(rinex_path)
        Attempting to build nav file for data/NCB11750.23o
        >>> brdm_path
        Path("data/brdm1750.23p")
    """

    response = f"\nAttempting to build nav file for {str(rinex_path)}"
    logger.info(response)
    print(response)
    with open(rinex_path) as f:
        files = f.readlines()
        for line in files:
            if "TIME OF FIRST OBS" in line:
                time_values = line.split("GPS")[0].strip().split()
                start_date = datetime.date(
                    year=int(time_values[0]),
                    month=int(time_values[1]),
                    day=int(time_values[2]))
                break
    if start_date is None:
        response = "No TIME OF FIRST OBS found in RINEX file."
        logger.error(response)
        print(response)
        return
    year = str(start_date.year)
    doy = str(start_date.timetuple().tm_yday)
    brdm_path = rinex_path.parent/f"brdm{doy}0.{year:2}p"
    if brdm_path.exists():
        response = f"{brdm_path} already exists.\n"
        logger.info(response)
        print(response)
        return brdm_path
    remote_resource_dict: Dict[str,RemoteResource] = get_daily_rinex_url(start_date)
    for source,remote_resource in remote_resource_dict["rinex_3"].items():
        response = f"Attemping to download {source} - {str(remote_resource)}"
        logger.info(response)
        print(response)
        local_path = rinex_path.parent /remote_resource.file
        try:
            download(remote_resource,local_path)
        except Exception as e:
            response = f"Failed to download {str(remote_resource)} | {e}"
            logger.error(response)
            print(response)
            continue
        if local_path.exists():
            response = f"Succesfully downloaded {str(remote_resource)} to {str(local_path)}"
            logger.info(response)
            print(response)
            local_path = uncompressed_file(local_path)
            local_path.rename(local_path.parent/brdm_path)
            response = f"Successfully built {brdm_path}"
            logger.info(response)
            print(response)
            return brdm_path

    with tempfile.TemporaryDirectory() as tempdir:
        # If rinex 3 nav file pathway is not found, try rinex 2
        for source,constellations in remote_resource_dict["rinex_2"].items():

            gps_url:RemoteResource = constellations["gps"]
            glonass_url:RemoteResource = constellations["glonass"]
            gps_local_name = gps_url.file+".Z"
            glonass_local_name = glonass_url.file+".Z"

            gps_dl_path = Path(tempdir)/gps_local_name
            glonass_dl_path = Path(tempdir)/glonass_local_name
            response = f"Attemping to download {source} - {str(gps_url)}"
            logger.info(response)
            print(response)
            try:
                if not gps_dl_path.exists():
                    download(gps_url,gps_dl_path)

                if not glonass_dl_path.exists():
                    download(glonass_url,glonass_dl_path)

            except Exception as e:
                response = f"Failed to download {str(gps_url)} or {str(glonass_url)}"
                logger.error(response)
                print(response)
                continue
            if gps_dl_path.exists() and glonass_dl_path.exists():
                gps_dl_path = uncompressed_file(gps_dl_path)
                glonass_dl_path = uncompressed_file(glonass_dl_path)
                if merge_broadcast_files(gps_dl_path,glonass_dl_path,rinex_path.parent):
                    response = f"Successfully built {brdm_path}"
                    logger.info(response)
                    print(response)
                    return brdm_path
            else:
                response = f"Failed to download {str(gps_url)} or {str(glonass_url)}"
                logger.error(response)
                print(response)
    response = f"Failed to build or locate {brdm_path}"
    logger.error(response)
    warnings.warn(response)

def get_gnss_products(rinex_path:Path,pride_dir:Path) ->None:
    """
    Retrieves GNSS products associated with the given RINEX file.

    # SP3, CLK, BIAS,SUM,OBX,ERP
    # SP3 - Satellite Position
    # CLK - Satellite Clock
    # BIAS - Phase Bias
    # SUM - Satellite Summary
    # OBX - Quaternions
    # ERP - Earth Rotation

    Args:
        rinex_path (Path): The path to the RINEX file.
        pride_dir (Path): The directory where the GNSS products will be stored.
    Returns:
        None
    Raises:
        Exception: If there is an error while downloading the GNSS products.
        Warning: If the GNSS products cannot be downloaded.
    """

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
    if start_date is None:
        print("No TIME OF FIRST OBS found in RINEX file.")
        return
    year = str(start_date.year)
    common_product_dir = pride_dir/year/"product"/"common"
    common_product_dir.mkdir(exist_ok=True,parents=True)
    remote_resource_dict: Dict[str,RemoteResource] = get_gnss_common_products(start_date)
    for product_type,sources in remote_resource_dict.items():
        response = f"Attempting to download {product_type} products"
        logger.info(response)
        print(response)
        for _,remote_resource in sources.items():
            local_path = common_product_dir/remote_resource.file
            if local_path.exists() and local_path.stat().st_size > 0:
                response = f"Found {local_path}"
                logger.info(response)
                print(response)
                break
            try:
                download(remote_resource,local_path)
            except Exception as e:
                response = f"Failed to download {str(remote_resource)} | {e}"
                logger.error(response)
                print(response)
                local_path.unlink()
                continue
            if local_path.exists():
                response = f"Succesfully downloaded {str(remote_resource)} to {str(local_path)}"
                logger.info(response)
                print(response)
                break
        if not local_path.exists():
            response = f"Failed to download {product_type} products"
            logger.error(response)
            warnings.warn(response)
    

if __name__ == '__main__':
    test_rinex = (
        Path(
            "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NCL1/NCB/NCB1/2023/intermediate"
        )
        / "NCB11750.23o"
    )
    get_nav_file(test_rinex)
    pride_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3/Pride")
    get_gnss_products(test_rinex,pride_dir)
