import datetime
from pathlib import Path
from ftplib import FTP
from typing import IO,Dict,Optional,List,Literal
import gzip
import tempfile
import logging
from pydantic import BaseModel,field_validator
import shutil
import warnings
import re

from es_sfgtools.processing.operations.gnss_resources import RemoteQuery,RemoteResource,WuhanIGS,CLSIGS,GSSC #,CDDIS
logger = logging.getLogger(__name__)    

def update_source(source:RemoteResource) -> RemoteResource:
    
    """
    Get the contents of the directory on a remote FTP server and return the first file that matches the sorted remote query.
    Args:
        source (RemoteResource): An object containing the FTP server details, directory to list, and the remote query for matching files.
    Returns:
        RemoteResource: The updated source object with the file_name attribute set to the first matching file in the sorted order, or None if no match is found.
    Raises:
        ftplib.all_errors: Any FTP-related errors encountered during the connection, login, or directory listing process.

    Example:
        >>> remote_resource = WuhanIGS.get_rinex_3_nav(datetime.date(2021,1,1))
        >>> remote_resource.file_name
        None
        >>> updated_remote_resource = update_source(remote_resource)
        >>> updated_remote_resource.file_name
        "BRDC20210010000.rnx.gz"
    """
    # List the contents of the directory and return the first file that matches the sorted remote query
    assert isinstance(source.remote_query,RemoteQuery), f"Remote query not set for {source}"

    try:
        with FTP(source.ftpserver.replace("ftp://",""),timeout=60) as ftp:
            ftp.set_pasv(True)
            ftp.login()
            ftp.cwd("/" + source.directory)
            dir_list = ftp.nlst()
    except Exception as e:
        logger.error(f"Failed to list directory {source.directory} on {source.ftpserver} | {e}")
        return source

    remote_query = source.remote_query
    
    dir_match = [d for d in dir_list if remote_query.pattern.search(d)]
    if len(dir_match) == 0:
        logger.error(f"No match found for {remote_query.pattern}")
        return source
    
    sorted_match = []
    if remote_query.sort_order is not None:
        for prod_type in remote_query.sort_order:
            for idx,d in enumerate(dir_match):
                if prod_type in d:
                    sorted_match.append(dir_match.pop(idx))
    sorted_match.extend(dir_match)
    source.file_name = sorted_match[0]
    logger.info(f"Match found for {remote_query.pattern} : {source.file_name}")
    return source 


def download(source:RemoteResource,dest:Path) ->Path:

    """
    Downloads a file from a remote FTP server to a local destination.
    Args:
        source (RemoteResource): An object containing the FTP server details, directory, and file name.
        dest (Path): The local path where the file will be saved.
    Returns:
        Path: The local path where the file has been saved.
    Raises:
        ftplib.all_errors: If any FTP-related error occurs during the download process.
    Example:
        >>> source = WuhanIGS.get_rinex_3_nav(datetime.date(2021,1,1))
        >>> updated_source = update_source(source)
        >>> dest_dir = Path("dest/to/data")
        >>> dest = dest_dir/updated_source.file_name
        >>> download(source, dest)
        "Downloading ftp://igs.gnsswhu.cn/pub/gps/data/daily/2021/001/21p/BRDC20210010000.rnx.gz" to dest/to/data/BRDC20210010000.rnx.gz"
        >>> dest.exists()
        True
    """

    logger.info(f"\nAttempting Download of {str(source)} to {str(dest)}\n")
    with FTP(source.ftpserver.replace("ftp://",""),timeout=60) as ftp:
        ftp.set_pasv(True)
        ftp.login()
        ftp.cwd("/" + source.directory)
        with open(dest,"wb") as f:
            ftp.retrbinary(f"RETR {source.file_name}",f.write)
    logger.info(f"\nDownloaded {str(source)} to {str(dest)}\n")
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
    This function returns the 'RemoteResource' for the IGS rinex observation file for a given date.
    url config docs at https://igs.org/products-access/#gnss-broadcast-ephemeris

    Args:
        date (datetime.date): The date for which the rinex observation file is required.
    Returns:
        Dict[str,Dict[str,RemoteResource]]: A dictionary containing the urls for the rinex 2 and 3 observation files.
    Examples:
        >>> date = datetime.date(2021,1,1)
        >>> urls = get_daily_rinex_url(date)
        >>> str(urls["rinex_2"]["wuhan_gps"])
        "ftp://igs.gnsswhu.cn/pub/gps/data/daily/21/001/21n/"
        >>> str(urls["rinex_2"]["wuhan_glonass"])
        "ftp://igs.gnsswhu.cn/pub/gps/data/daily/21/001/21g/"
    Note:
        Until the remote resorces are updated with 'update_source', the file_name attribute will be None.
    """

    urls = {
        "rinex_2": {
            "wuhan": {
                "glonass": WuhanIGS.get_rinex_2_nav(date, constellation="glonass"),
                "gps": WuhanIGS.get_rinex_2_nav(date, constellation="gps"),
            },
            "gssc": {
                "glonass": GSSC.get_rinex_2_nav(date, constellation="glonass"),
                "gps": GSSC.get_rinex_2_nav(date, constellation="gps"),
            },
        },
        "rinex_3": {
            "igs_gnss": CLSIGS.get_rinex_3_nav(date),
            "wuhan_gps": WuhanIGS.get_rinex_3_nav(date),
            "gssc_gnss": GSSC.get_rinex_3_nav(date),
        },
    }
    return urls

def get_gnss_common_products(date:datetime.date) ->Dict[str,Dict[str,Path]]:

    """
    Retrieve GNSS common products for a given date.
    This function fetches various GNSS products (sp3, clk, bias, obx, erp) from 
    different sources (WuhanIGS and CLSIGS) for the specified date.
    Args:
        date (datetime.date): The date for which to retrieve the GNSS products.
    Returns:
        Dict[str, Dict[str, Path]]: A dictionary containing the GNSS products 
        categorized by product type and source. The structure of the dictionary is:
            {   "sp3": {
                    "wuhan": Path to WuhanIGS sp3 product,
                    "cligs": Path to CLSIGS sp3 product,
                    }.
                "clk": {
                    "wuhan": Path to WuhanIGS clk product,
                    "cligs": Path to CLSIGS clk product,
                    },
                "bias": {
                    "wuhan": Path to WuhanIGS bias product,
                    "cligs": Path to CLSIGS bias product,
                    },
                "obx": {
                    "wuhan": Path to WuhanIGS obx product,
                    "cligs": Path to CLSIGS obx product,
                    },
                "erp": {
                    "wuhan": Path to WuhanIGS erp product,
                    "cligs": Path to CLSIGS erp product,
                    }
            }
    Note:
        Until the remote resorces are updated with 'update_source', the file_name attribute will be None.
    """

    urls = {
        "sp3": {
            "cligs": CLSIGS.get_product_sp3(date),
            "wuhan": WuhanIGS.get_product_sp3(date),
        },
        "clk": {
            "cligs": CLSIGS.get_product_clk(date),
            "wuhan": WuhanIGS.get_product_clk(date),
        },
        "bias": {
            "cligs": CLSIGS.get_product_bias(date),
            "wuhan": WuhanIGS.get_product_bias(date),
        },
        "obx": {
            "cligs": CLSIGS.get_product_obx(date),
            "wuhan": WuhanIGS.get_product_obx(date),
        },
        "erp": {
            "cligs": CLSIGS.get_product_erp(date),
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
    logger.info(f"Merging {brdn} and {brdg} into a single BRDM file.")

    def write_brdn(file:Path, prefix:str, fm:IO):
        """
        Writes data from a brdn file to a given output stream.

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
        while i < len(lines):
            try:
                #print(i, lines[i])
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
                    print(f"{prefix}{prn:02d} {yyyy:04d} {mm:02d} {dd:02d} {hh:02d} {mi:02d} {ss:02d} {num2:.12e} {num3:.12e} {num4:.12e}\n")
                    fm.write(
                        f"{prefix}{prn:02d} {yyyy:04d} {mm:02d} {dd:02d} {hh:02d} {mi:02d} {ss:02d} {num2:.12e} {num3:.12e} {num4:.12e}\n"
                    )

                    for t in range(1, 4):
                        line = lines[i + t].replace("D", "e")
                        num1 = eval(line[3:22])
                        num2 = eval(line[22:41])
                        num3 = eval(line[41:60])
                        num4 = eval(line[60:79])
                        print(f"{t}    {num1} {num2} {num3} {num4}\n")
                        print(f"    {num1:.12e} {num2:.12e} {num3:.12e} {num4:.12e}\n")
                        fm.write(
                            f"    {num1:.12e} {num2:.12e} {num3:.12e} {num4:.12e}\n"
                        )
                    print('here')
                    line = lines[i + 7].replace("D", "e")
                    num1 = eval(line[3:22])
                    num2 = eval(line[22:41])
                    fm.write(f"    {num1:.12e} {num2:.12e}\n")
                    i += 8
                    if i >= len(lines):
                        break
                else:
                    if ("PGM / RUN BY / DATE" == lines[i][60:79]):
                        fm.write(lines[i])
                    if ("LEAP SECONDS"        == lines[i][60:72]):
                        leap_n = int(lines[i][1:6])
                        fm.write(lines[i])
                    if ("END OF HEADER"       == lines[i][60:73]):
                        inHeader = False
                        fm.write(lines[i])
                    i = i + 1
            except Exception as e:
                print(
                    f"***ERROR: unexpected ERROR occurred at line {i} of file {file}: {e}"
                )
                #print(lines[i])
                break

        fn.close()

    def write_brdg(file:Path, prefix:str, fm:IO):
        """
        Writes data from a brdg file to a given output stream.

        Args:
            file (Path): The path to the file to be read.
            prefix (str): The prefix to be added to each line of data.
            fm (IO): The output stream to write the data to.

        Returns:
            None

        Raises:
            Exception: If an unexpected error occurs while reading or writing the files.
        """
        try:
            fg = open(file)
            lines = fg.readlines()
            inHeader = True
        except Exception as e:
            print(f"***ERROR: unable to open or read file {file}: {e}")
            return
        i = 1
        while (i < len(lines)):
            try:
                if (not inHeader):
                    line = lines[i].replace("D","e")
                    prn  = int(line[ 0: 2])
                    yyyy = int(line[ 3: 5]) + 2000
                    mm   = int(line[ 6: 8])
                    dd   = int(line[ 9:11])
                    hh   = int(line[12:14])
                    mi   = int(line[15:17])
                    ss   = round(float(line[18:22]))
                    num2 = eval(line[22:41])
                    num3 = eval(line[41:60])
                    num4 = eval(line[60:79])
                    fm.write("R{:02d} {:04d} {:02d} {:02d} {:02d} {:02d} {:02d}{: .12e}{: .12e}{: .12e}\n".format(
                        prn, yyyy, mm, dd, hh, mi, int(ss), num2, num3, num4))
                    for t in range(1,4):
                        line = lines[i+t].replace("D","e")
                        num1 = eval(line[ 3:22])
                        num2 = eval(line[22:41])
                        num3 = eval(line[41:60])
                        num4 = eval(line[60:79])
                        fm.write("    {: .12e}{: .12e}{: .12e}{: .12e}\n".format(num1, num2, num3, num4))
                    i = i + 4
                    if (i >= len(lines)):
                        break
                else:
                    if ("LEAP SECONDS"  == lines[i][60:72]):
                        leap_g = int(lines[i][1:6])
                    if ("END OF HEADER" == lines[i][60:73]):
                        inHeader = False
                    i = i + 1
            except Exception as e:
                print(f"***ERROR: unexpected ERROR occurred at line {i} of file {file}: {e}")
                #print(lines[i])
                break
        fg.close()

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
    write_brdn(brdn, "G", fm)
    write_brdg(brdg, "R", fm)
    fm.close()

    if brdm.exists():
        logger.info(f"Files merged into {str(brdm)}")
        return brdm
    logger.error(f"Failed to merge files into {str(brdm)}")
    return None


def get_nav_file(rinex_path:Path,override:bool=False) -> Path:
    """
    Attempts to build a navigation file for a given RINEX file by downloading the necessary files from the IGS FTP server.

    Args:
        rinex_path (Path): The path to the RINEX file.
        override (bool): If True, the function will attempt to download the navigation file even if it already exists.
        mode (Literal['process','test']): The mode in which the function is running. Test mode attempt downloads from all resources
    Returns:
        brdm_path (Path): The path to the navigation file.
    Raises:
        Warning: If the navigation file cannot be built or located.

    Examples:
        >>> rinex_path = Path("data/NCB11750.23o")
        >>> nav_path = get_nav_file(rinex_path)
        Attempting to build nav file for data/NCB11750.23o
        >>> nav_path
        Path("data/BRDC00IGS_R_20231750000_01D_MN.rnx.gz")
    """
  
    response = f"\nAttempting to build nav file for {str(rinex_path)}"
    logger.info(response)

    start_date = None
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

        return
    year = str(start_date.year)
    doy = str(start_date.timetuple().tm_yday)
    brdc_pattern = re.compile(rf"BRDC.*{year}{doy}.*rnx.*")
    brdm_pattern = re.compile(rf"brdm{doy}0.{year[-2:]}p")

    found_nav_files = [x for x in rinex_path.parent.glob("*") if brdc_pattern.search(x.name) or brdm_pattern.search(x.name)]

    for nav_file in found_nav_files:
        if nav_file.stat().st_size > 0 and not override:
            response = f"{nav_file} already exists."
            logger.info(response)
            return nav_file

    remote_resource_dict: Dict[str,RemoteResource] = get_daily_rinex_url(start_date)
    for source,remote_resource in remote_resource_dict["rinex_3"].items():

        remote_resource_updated = update_source(remote_resource)
        if remote_resource_updated.file_name is None:
            continue

        response = f"Attemping to download {source} - {str(remote_resource)}"
        logger.info(response)

        local_path = rinex_path.parent /remote_resource.file_name
        try:
            download(remote_resource,local_path)
        except Exception as e:
            logger.error(f"Failed to download {str(remote_resource)} | {e}")

            continue
        if local_path.exists():

            logger.info(
                f"Succesfully downloaded {str(remote_resource)} to {str(local_path)}"
            )
            logger.info(f"Successfully built {str(local_path)} From {str(remote_resource)}")
            return local_path

    with tempfile.TemporaryDirectory() as tempdir:
        # If rinex 3 nav file pathway is not found, try rinex 2
        for source,constellations in remote_resource_dict["rinex_2"].items():
            gps_url:RemoteResource = constellations["gps"]
            glonass_url:RemoteResource = constellations["glonass"]

            gps_url_updated = update_source(gps_url)
            glonass_url_updated = update_source(glonass_url)
            if gps_url_updated.file_name is None or glonass_url_updated.file_name is None:
                continue

            gps_local_name = gps_url.file_name
            glonass_local_name = glonass_url.file_name

            gps_dl_path = Path(tempdir)/gps_local_name
            glonass_dl_path = Path(tempdir)/glonass_local_name

            logger.info(f"Attemping to download {source} From {str(gps_url)}")

            try:

                download(gps_url,gps_dl_path)

                download(glonass_url,glonass_dl_path)

            except Exception as e:

                logger.error(
                    f"Failed to download {str(gps_url)} To {str(gps_dl_path.name)} or {str(glonass_url)} To {str(glonass_dl_path.name)} | {e}"
                )

                continue
            if gps_dl_path.exists() and glonass_dl_path.exists():
                gps_dl_path = uncompressed_file(gps_dl_path)
                glonass_dl_path = uncompressed_file(glonass_dl_path)
                if (brdm_path := merge_broadcast_files(gps_dl_path,glonass_dl_path,rinex_path.parent)) is not None:

                    logger.info(f"Successfully built {brdm_path}")

                    return brdm_path
            else:
                response = f"Failed to download {str(gps_url)} or {str(glonass_url)}"
                logger.error(response)
                print(response)
    response = f"Failed to build or locate {brdm_path}"
    logger.error(response)
    warnings.warn(response)


def get_gnss_products(
    rinex_path: Path,
    pride_dir: Path,
    override: bool = False,
    source: Literal["all","wuhan", "cligs"] = "all"
) -> dict:

    """
    Retrieves GNSS products associated with the given RINEX file.

    ### The following GNSS products are retrieved:
    #### SP3 - Satellite Position
    #### CLK - Satellite Clock
    #### BIAS - Phase Bias
    #### SUM - Satellite Summary
    #### OBX - Quaternions
    #### ERP - Earth Rotation

    Args:
        rinex_path (Path): The path to the RINEX file.
        pride_dir (Path): The directory where the GNSS products will be stored.
        override (bool): If True, the function will attempt to download the GNSS products even if they already exist.
        source (Literal['all','wuhan', 'cligs']): The source of the GNSS products to download. (default: "all")
    Returns:
        product_status (dict): A dictionary containing the status of the GNSS products downloaded.
    Raises:
        Exception: If there is an error while downloading the GNSS products.
    
    """
    assert source in ["all","wuhan", "cligs"], f"Invalid source {source}"

    start_date = None
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
        logger.error("No TIME OF FIRST OBS found in RINEX file.")
        return
    year = str(start_date.year)
    common_product_dir = pride_dir/year/"product"/"common"
    common_product_dir.mkdir(exist_ok=True,parents=True)
    cp_dir_list = list(common_product_dir.glob("*"))
    remote_resource_dict: Dict[str,Dict[str,RemoteResource]] = get_gnss_common_products(start_date)

    product_status = {}

    for product_type,sources in remote_resource_dict.items():
        logger.info(f"Attempting to download {product_type} products")
        if product_type not in product_status:
            product_status[product_type] = 'False'

        for dl_source,remote_resource in sources.items():
            if source != "all" and dl_source != source:
                continue
            # check if file already exists
            found_files = [f for f in cp_dir_list if remote_resource.remote_query.pattern.match(f.name)]
            if found_files and not override:
                logger.info(f"Found {found_files[0]} for product {product_type}")
                product_status[product_type] = str(found_files[0])
                break

            remote_resource_updated = update_source(remote_resource)
            if remote_resource_updated.file_name is None:
                continue

            local_path = common_product_dir/remote_resource.file_name
            if local_path.exists() and local_path.stat().st_size > 0 and not override:
                product_status[product_type] = str(local_path)
                logger.info(f"Found {str(local_path)} for product {product_type}")
                break
            try:
                logger.info(f"Attempting to download {product_type} product from {str(remote_resource)}")
                download(remote_resource,local_path)
                logger.info(f"\n Succesfully downloaded {product_type} FROM {str(remote_resource)} TO {str(local_path)}\n")
                product_status[product_type] = str(local_path)
                break
            except Exception as e:
                logger.error(f"Failed to download {str(remote_resource)} | {e}")
                if local_path.exists() and local_path.stat().st_size == 0:
                    local_path.unlink()
                continue

    for product_type,product_path in product_status.items():
        logger.info(f"{product_type} : {product_path}")

    return product_status
