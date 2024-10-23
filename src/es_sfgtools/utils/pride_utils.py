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

logger = logging.getLogger(__name__)    
# TODO use ftplib to download files
# https://docs.python.org/3/library/ftplib.html


class RemoteResource(BaseModel):
    ftpserver:str
    directory:List[str]
    file:Optional[str]=None
    
    @field_validator("directory",mode="before")
    def _proc_dir(cls,v):
        if isinstance(v,str):
            return [v]
        return v
    
    def set(self,directory:List[str]|str="",file:str=None):
        if isinstance(directory,str):
            directory = [directory]
        directory = self.directory + directory
        return self.model_copy(update={"directory":directory,"file":file},deep=True)
    
    def __str__(self):
        pathlist = [self.ftpserver]+self.directory + [self.file]
        return "/".join(pathlist)


WUHAN_GPS_DAILY = RemoteResource(ftpserver="ftp://igs.gnsswhu.cn",directory=["pub","gps","data","daily"])
NASA_GPS_DAILY = RemoteResource(ftpserver="ftp://cddis.gsfc.nasa.gov",directory=["gnss","data","daily"])
CDDIS_GNSS_DAILY = RemoteResource(ftpserver="https://cddis.gsfc.nasa.gov",directory=["archive","gnss","data","daily"])
IGS_GNSS_DATA = RemoteResource(ftpserver="ftp://igs.ensg.ign.fr",directory=["pub","igs","data"])
GSSC_GNSS_DATA = RemoteResource(ftpserver="ftp://gssc.esa.int",directory=["gnss","data","daily"])
SIO_GNSS_DATA = RemoteResource(ftpserver="ftp://lox.ucsd.edu",directory=["rinex"])

def download(source:RemoteResource,dest:Path) ->Path:
    
    with FTP(source.ftpserver.replace("ftp://","")) as ftp:
        ftp.login()
        ftp.cwd("/".join(source.directory))
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
        >>> file = Path("data/brdc1500.21n.Z")
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


def get_daily_rinex_url(date:datetime.date) ->Dict[str,Dict[str,Path]]:
    """
    This function returns the url for the IGS rinex observation file for a given date.
    url config docs at https://igs.org/products-access/#gnss-broadcast-ephemeris

    Args:
        date (datetime.date): The date for which the rinex observation file is required.
    Returns:
        Dict[str,Dict[str,Path]]: A dictionary containing urls for rinex 2 and rinex 3 observation files.
    Examples:
        >>> date = datetime.date(2021,1,1)
        >>> urls = get_daily_rinex_url(date)
        >>> urls["rinex_2"]["wuhan_gps"]
        Path("ftp://igs.gnsswhu.cn/pub/gps/data/daily/21/001/21n/brdc0010.21n.Z")
        >>> urls["rinex_2"]["wuhan_glonass"]
        Path("ftp://igs.gnsswhu.cn/pub/gps/data/daily/21/001/21g/brdc0010.21g.Z")
    """

    year = str(date.year)
    doy = date.timetuple().tm_yday
    if doy < 10:
        doy = f"00{doy}"
    elif doy < 100:
        doy = f"0{doy}"
    doy = str(doy)
    auto_rinex_2_gps = f"auto{doy}0.{year[2:]}n.Z"
    brcd_rinex_2_gps = f"brdc{doy}0.{year[2:]}n.Z"
    brcd_rinex_2_glonass = f"brdc{doy}0.{year[2:]}g.Z"
    brcd_rinex_3 = f"BRDC00IGS_R_{year}{doy}0000_01D_MN.rnx.gz"

    urls = {
        "rinex_2": {
            "wuhan":{
                "glonass": WUHAN_GPS_DAILY.set(directory=[year,doy,(year[2:] + 'g')],file=brcd_rinex_2_glonass),
                "gps":WUHAN_GPS_DAILY.set(directory=[year,doy,(year[2:] + 'n')],file=brcd_rinex_2_gps),
                },
            "cdds":{
                "glonass": CDDIS_GNSS_DAILY.set(directory=[year,doy,(year[2:] + 'g')],file=brcd_rinex_2_glonass),
                "gps":CDDIS_GNSS_DAILY.set(directory=[year,doy,(year[2:] + 'n')],file=brcd_rinex_2_gps),
            },
            "igs":{
                "glonass":IGS_GNSS_DATA.set(file=brcd_rinex_2_glonass),
                "gps":IGS_GNSS_DATA.set(file=brcd_rinex_2_gps)
            },
            "gssc":{
                "glonass":GSSC_GNSS_DATA.set(file=brcd_rinex_2_glonass),
                "gps":GSSC_GNSS_DATA.set(file=brcd_rinex_2_gps)
            }
        },
        "rinex_3": {
            "wuhan_gps": WUHAN_GPS_DAILY.set(directory=[year,doy,(year[2:] + 'p')],file=brcd_rinex_3),
            "igs_gnss": IGS_GNSS_DATA.set(directory=[year,doy],file=brcd_rinex_3),
            "cddis_gnss": CDDIS_GNSS_DAILY.set(directory=[year,doy,(year[2:] + 'p')],file=brcd_rinex_3),
            "gssc_gnss": GSSC_GNSS_DATA.set(file=brcd_rinex_3)
        }
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


def get_nav_file(rinex_path:Path) -> None:
    print(f"\nAttempting to build nav file for {str(rinex_path)}")
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
        print("No TIME OF FIRST OBS found in RINEX file.")
        return
    year = str(start_date.year)
    doy = str(start_date.timetuple().tm_yday)
    brdm_path = rinex_path.parent/f"brdm{doy}0.{year:2}p"
    if brdm_path.exists():
        print(f"{brdm_path} already exists.\n")
        return
    urls = get_daily_rinex_url(start_date)
    for source,url in urls["rinex_3"].items():
        print(f"Attemping to download {source} - {str(url)}")
        local_path = rinex_path.parent /url.file
        try:
           
            download(url,local_path)
        except Exception as e:
            print(f"Failed to download {str(url)} | {e}")
            continue
        if local_path.exists():
            print(f"Succesfully downloaded {str(url)} to {str(local_path)}")
            local_path = uncompressed_file(local_path)
            local_path.rename(local_path.parent/brdm_path)
            print(f"Successfully built {brdm_path}")
            return

    with tempfile.TemporaryDirectory() as tempdir:
        # If rinex 3 nav file pathway is not found, try rinex 2
        for source,constellations in urls["rinex_2"].items():

            gps_url:RemoteResource = constellations["gps"]
            glonass_url:RemoteResource = constellations["glonass"]
            gps_local_name = gps_url.file+".Z"
            glonass_local_name = glonass_url.file+".Z"

            gps_dl_path = Path(tempdir)/gps_local_name
            glonass_dl_path = Path(tempdir)/glonass_local_name
            print(f"Attemping to download {source} - {str(gps_url)}")
            try:
                if not gps_dl_path.exists():
                    download(gps_url,gps_dl_path)
                    
                if not glonass_dl_path.exists():
                    download(glonass_url,glonass_dl_path)
                    
            except Exception as e:
                print(f"Failed to download {str(gps_url)} or {str(glonass_url)}")
                continue
            if gps_dl_path.exists() and glonass_dl_path.exists():
                gps_dl_path = uncompressed_file(gps_dl_path)
                glonass_dl_path = uncompressed_file(glonass_dl_path)
                if merge_broadcast_files(gps_dl_path,glonass_dl_path,rinex_path.parent):
                    print(f"Successfully built {brdm_path}")
                    return
            else:
                print(f"Failed to download {str(gps_url)} or {str(glonass_url)}")

if __name__ == '__main__':
    test_rinex = (
        Path(
            "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NCL1/NCB/NCB1/2023/intermediate"
        )
        / "NCB11750.23o"
    )
    get_nav_file(test_rinex)
