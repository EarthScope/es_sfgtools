from pydantic import BaseModel
import datetime
from typing import Literal,Tuple
from enum import Enum

GNSS_START_TIME = datetime.datetime(1980, 1, 6, tzinfo=datetime.timezone.utc)  # GNSS start time

class RemoteResource(BaseModel):
    ftpserver:str
    directory:str
    file:str

    def __str__(self):
        return "/".join([self.ftpserver, self.directory, self.file])


def _parse_date(date:datetime.date | datetime.datetime)-> Tuple[str,str]:
    if isinstance(date,datetime.datetime):
        date = date.date()
    year = str(date.year)
    doy = date.timetuple().tm_yday
    if doy < 10:
        doy = f"00{doy}"
    elif doy < 100:
        doy = f"0{doy}"
    doy = str(doy)
    return year,doy

def _date_to_gps_week(date:datetime.date | datetime.datetime)-> int:
    # get the number of weeks since the start of the GPS epoch

    if isinstance(date,datetime.datetime):
        date = date.date()
    time_since_epoch = date - GNSS_START_TIME.date()
    gps_week = time_since_epoch.days // 7
    return gps_week

class WuhanIGS:
    ftpserver = "ftp://igs.gnsswhu.cn"
    daily_gps_dir = "pub/gps/data/daily"
    daily_product_dir = "pub/whu/phasebias"

    constellation_tag = {
        "gps":'n',
        "glonass":'g',
    }

    @classmethod
    def get_rinex_2_nav(cls,date:datetime.date,constellation:Literal["gps","glonass"]="gps")->RemoteResource:
        assert constellation in cls.constellation_tag.keys(),f"Constellation {constellation} not recognized"
        const_tag = cls.constellation_tag[constellation]
        year,doy = _parse_date(date)
        dir_extension = f"{year}/{doy}/{year[2:]}{const_tag}"
        file_name = f"brdc{doy}0.{year[2:]}{const_tag}.gz"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_rinex_3_nav(cls,date:datetime) -> RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/{doy}/{year[2:]}p"
        file_name = f"BRDC00IGS_R_{year}{doy}0000_01D_MN.rnx.gz"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_product_sp3(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/orbit"
        file_name = f"WMC0DEMFIN_{year}{doy}0000_01D_05M_ORB.SP3.gz"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_product_obx(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/orbit"
        file_name = f"WMC0DEMFIN_{year}{doy}0000_01D_30S_ATT.OBX.gz"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_product_clk(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/clock"
        file_name = f"WMC0DEMFIN_{year}{doy}0000_01D_30S_CLK.CLK.gz"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_product_sum(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/clock"
        file_name = f"WMC0DEMFIN_{year}{doy}0000_01D_01D_CLS.SUM.gz"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_product_bias(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/bias"
        file_name = f"WMC0DEMFIN_{year}{doy}0000_01D_01D_OSB.BIA.gz"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_product_erp(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{doy}/orbit"
        file_name = f"COD0R03FIN_{year}{doy}0000_01D_01D_ERP.ERP.gz"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

class CDDIS:
    ftpserver = "ftp://cddis.gsfc.nasa.gov"
    daily_gps_dir = "gnss/data/daily"

    constellation_tag = {
        "gps": "n",
        "glonass": "g",
    }

    @classmethod
    def get_rinex_2_nav(
        cls, date: datetime.date, constellation: Literal["gps", "glonass"] = "gps"
    ) -> RemoteResource:
        assert (
            constellation in cls.constellation_tag.keys()
        ), f"Constellation {constellation} not recognized"
        const_tag = cls.constellation_tag[constellation]
        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}/{year[2:]}{const_tag}"
        file_name = f"brdc{doy}0.{year[2:]}{const_tag}.gz"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        return RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, file=file_name
        )

    @classmethod
    def get_rinex_3_nav(cls, date: datetime) -> RemoteResource:
        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}/{year[2:]}p"
        file_name = f"BRDC00IGS_R_{year}{doy}0000_01D_MN.rnx.gz"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        return RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, file=file_name
        )

class GSSC:
    ftpserver = "ftp://gssc.esa.int"
    daily_gps_dir = "gnss/data/daily"

    constellation_tag = {
        "gps": "n",
        "glonass": "g",
    }

    @classmethod
    def get_rinex_2_nav(
        cls, date: datetime.date, constellation: Literal["gps", "glonass"] = "gps"
    ) -> RemoteResource:
        assert (
            constellation in cls.constellation_tag.keys()
        ), f"Constellation {constellation} not recognized"
        const_tag = cls.constellation_tag[constellation]
        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}"
        file_name = f"brdc{doy}0.{year[2:]}{const_tag}.gz"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        return RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, file=file_name
        )

    @classmethod
    def get_rinex_3_nav(cls, date: datetime) -> RemoteResource:
        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}/{year[2:]}p"
        file_name = f"BRDC00IGS_R_{year}{doy}0000_01D_MN.rnx.gz"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        return RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, file=file_name
        )

class CLSIGS:
    ftpserver = "ftp://igs.ign.fr"
    daily_products_dir = "pub/igs/products/mgex"

    @classmethod
    def get_rinex_3_nav(cls, date: datetime) -> RemoteResource:
        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        file_name = f"BRDC00IGN_R_${year}${doy}0000_01D_MN.rnx"
        return RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, file=file_name
        )
    @classmethod
    def get_product_sp3(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        file_name = f"WUM0MGXRAP_${year}${doy}0000_01D_05M_ORB.SP3.gz"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)
    @classmethod
    def get_product_clk(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        file_name = f"WUM0MGXRAP_${year}${doy}0000_01D_30S_CLK.CLK.gz"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_product_erp(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        file_name = f"WUM0MGXRAP_${year}${doy}0000_01D_01D_ERP.ERP.gz"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)

    @classmethod
    def get_product_orbit(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        file_name = f"WUM0MGXRAP_${year}${doy}0000_01D_30S_ATT.OBX.gz"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)
    
    @classmethod
    def get_product_bias(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        file_name = f"WUM0MGXRAP_${year}${doy}0000_01D_01D_OSB.BIA.gz"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        return RemoteResource(ftpserver=cls.ftpserver, directory=directory, file=file_name)
    
class Potsdam:
    ftpserver = "ftp://isdcftp.gfz-potsdam.de"
    daily_products_dir = "gnss/products/final"
