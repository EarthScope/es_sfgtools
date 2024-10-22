import datetime
from pathlib import Path
from typing import IO,Dict
import wget
import zlib
import tempfile

WUHAN_GPS_DAILY = Path("ftp://igs.gnsswhu.cn/pub/gps/data/daily/")
NASA_GPS_DAILY = Path("ftp://cddis.gsfc.nasa.gov/gnss/data/daily/")
CDDIS_GNSS_DAILY = Path("https://cddis.gsfc.nasa.gov/archive/gnss/data/daily/")
IGS_GNSS_DATA = Path("ftp://igs.ensg.ign.fr/pub/igs/data/")
GSSC_GNSS_DATA = Path("ftp://gssc.esa.int/gnss/data/daily/")
SIO_GNSS_DATA = Path("ftp://lox.ucsd.edu/rinex/")

def uncompressed_file(file:Path) ->Path:
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
    if not file.exists():
        raise FileNotFoundError(f"File {file} does not exist.")
    out_file = file.with_suffix("")
    with open(file, "rb") as f:
        compressed_data = f.read()
    decompressed_data = zlib.decompress(compressed_data).decode("utf-8")
    with open(out_file, "w") as f:
        f.write(decompressed_data)
    file.unlink()
    return out_file

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
    auto_rinex_2_gps = f"auto{doy}0.{year[2:]}n.Z"
    brcd_rinex_2_gps = f"brdc{doy}0.{year[2:]}n.Z"
    brcd_rinex_2_glonass = f"brdc{doy}0.{year[2:]}g.Z"
    brcd_rinex_3 = f"BRDC00IGS_R_${year}${doy}0000_01D_MN.rnx.gz"

    urls = {
        "rinex_2": {
            "wuhan":{
                "glonass":WUHAN_GPS_DAILY/year/doy/(year[2:] + "g")/brcd_rinex_2_glonass,
                "gps":WUHAN_GPS_DAILY/year/doy/(year[2:] + "n")/brcd_rinex_2_gps/year/doy/(year[2:] + "n")/brcd_rinex_2_gps
                },
            "cdds":{
                "glonass":CDDIS_GNSS_DAILY/year/doy/ (year[2:] + "g")/brcd_rinex_2_glonass,
                "gps":CDDIS_GNSS_DAILY/year/doy/(year[2:] + "n")/brcd_rinex_2_gps
            },
            "igs":{
                "glonass":IGS_GNSS_DATA / brcd_rinex_2_glonass,
                "gps":IGS_GNSS_DATA / brcd_rinex_2_gps
            },
            "gssc":{
                "glonass":GSSC_GNSS_DATA / brcd_rinex_2_glonass,
                "gps":GSSC_GNSS_DATA / brcd_rinex_2_gps
            }
        },
        "rinex_3": {
            "wuhan_gps": WUHAN_GPS_DAILY / year / doy / (year[2:] + "p") / brcd_rinex_3,
            "igs_gnss": IGS_GNSS_DATA / year / doy / brcd_rinex_3,
            "igs_gnss": IGS_GNSS_DATA / brcd_rinex_3,
            "nasa_gps": NASA_GPS_DAILY / year / doy / brcd_rinex_3,
            "cddis_gnss": CDDIS_GNSS_DAILY
            / year
            / doy
            / (year[2:] + "p")
            / brcd_rinex_3,
            "gssc_gnss": GSSC_GNSS_DATA / brcd_rinex_3,
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


def get_nav_file(rinex_path:Path) -> None:
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
    brdm_path = f"brdm{doy}0.{year:2}p"
    urls = get_daily_rinex_url(start_date)
    for source,url in urls["rinex_3"].items():
        print(f"Attemping to download {source} - {str(url)}")
        local_path = rinex_path.parent / (url.name+".gz")
        wget.download(str(url),str(local_path))
        if local_path.exists():
            print(f"Succesfully downloaded {str(url)} to {str(local_path)}")
            local_path = uncompressed_file(local_path)
            local_path.rename(local_path.parent/brdm_path)
            print(f"Successfully built {brdm_path}")
            return
    # If rinex 3 nav file pathway is not found, try rinex 2
    for source,constellations in urls["rinex_2"].items():
        
        gps_url:Path = constellations["gps"]
        glonass_url:Path = constellations["glonass"]
        gps_local_name = gps_url.name+".Z"
        glonass_local_name = glonass_url.name+".Z"

        with tempfile.TemporaryDirectory() as tempdir:
            gps_dl_path = Path(tempdir)/gps_local_name
            glonass_dl_path = Path(tempdir)/glonass_local_name
            wget.download(str(gps_url),str(gps_dl_path))
            wget.download(str(glonass_url),str(glonass_dl_path))
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
