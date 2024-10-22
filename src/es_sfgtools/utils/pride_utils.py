import datetime
from pathlib import Path
from typing import IO,Dict

WUHAN_GPS_DAILY = Path("ftp://igs.gnsswhu.cn/pub/gps/data/daily/")
NASA_GPS_DAILY = Path("ftp://cddis.gsfc.nasa.gov/gnss/data/daily/")
CDDIS_GNSS_DAILY = Path("https://cddis.gsfc.nasa.gov/archive/gnss/data/daily/")
IGS_GNSS_DATA = Path("ftp://igs.ensg.ign.fr/pub/igs/data/")
GSSC_GNSS_DATA = Path("ftp://gssc.esa.int/gnss/data/daily/")
SIO_GNSS_DATA = Path("ftp://lox.ucsd.edu/rinex/")

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
            "wuhan_glonass": WUHAN_GPS_DAILY
            / year
            / doy
            / (year[2:] + "g")
            / brcd_rinex_2_glonass,
            "wuhan_gps": WUHAN_GPS_DAILY
            / year
            / doy
            / (year[2:] + "n")
            / brcd_rinex_2_gps,
            "cdds_glonass": CDDIS_GNSS_DAILY
            / year
            / doy
            / (year[2:] + "g")
            / brcd_rinex_2_glonass,
            "cdds_gps": CDDIS_GNSS_DAILY
            / year
            / doy
            / (year[2:] + "n")
            / brcd_rinex_2_gps,
            "igs_glonass": IGS_GNSS_DATA / brcd_rinex_2_glonass,
            "igs_gps": IGS_GNSS_DATA / brcd_rinex_2_gps,
            "gssc_glonass": GSSC_GNSS_DATA / brcd_rinex_2_glonass,
            "gssc_gps": GSSC_GNSS_DATA / brcd_rinex_2_gps,
            "sio_gps": SIO_GNSS_DATA / year / doy / auto_rinex_2_gps,
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
    print(f"Files merged into {brdm}")
    return brdm
