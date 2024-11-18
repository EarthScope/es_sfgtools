from pydantic import BaseModel
from dataclasses import dataclass
import datetime
from typing import Literal,Tuple,List,Optional
from enum import Enum
import re

GNSS_START_TIME = datetime.datetime(1980, 1, 6, tzinfo=datetime.timezone.utc)  # GNSS start time


def _parse_date(date: datetime.date | datetime.datetime) -> Tuple[str, str]:
    if isinstance(date, datetime.datetime):
        date = date.date()
    year = str(date.year)
    doy = date.timetuple().tm_yday
    if doy < 10:
        doy = f"00{doy}"
    elif doy < 100:
        doy = f"0{doy}"
    doy = str(doy)
    return year, doy


def _date_to_gps_week(date: datetime.date | datetime.datetime) -> int:
    # get the number of weeks since the start of the GPS epoch

    if isinstance(date, datetime.datetime):
        date = date.date()
    time_since_epoch = date - GNSS_START_TIME.date()
    gps_week = time_since_epoch.days // 7
    return gps_week


class RemoteQuery:
    def __init__(self, pattern: re.Pattern, sort_order: List[str] = []):
        self.pattern = pattern
        self.sort_order = sort_order

    @classmethod
    def sp3(cls, date: datetime.date):
        year, doy = _parse_date(date)
        pattern = re.compile(rf"\d{year}\d{doy}.*SP3")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def obx(cls, date: datetime.date):
        year, doy = _parse_date(date)
        pattern = re.compile(rf"\d{year}\d{doy}.*OBX")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def clk(cls, date: datetime.date):
        year, doy = _parse_date(date)
        pattern = re.compile(rf"\d{year}\d{doy}.*CLK")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def sum(cls, date: datetime.date):
        year, doy = _parse_date(date)
        pattern = re.compile(rf"\d{year}\d{doy}.*SUM")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def bias(cls, date: datetime.date):
        year, doy = _parse_date(date)
        pattern = re.compile(rf"\d{year}\d{doy}.*BIA")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def erp(cls, date: datetime.date):
        year, doy = _parse_date(date)
        pattern = re.compile(rf"\d{year}\d{doy}.*ERP")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def rnx3(cls, date: datetime.date):
        year, doy = _parse_date(date)
        pattern = re.compile(rf"BRDC.*\d{year}\d{doy}.*rnx")
        return cls(pattern)

    @classmethod
    def rnx2(cls, date: datetime.date, constellation: Literal["gps", "glonass"]):
        year, doy = _parse_date(date)
        constellation_tag = {
            "gps": "n",
            "glonass": "g",
        }
        const_tag = constellation_tag[constellation]
        pattern = re.compile(f"brdc{doy}0.{year[2:]}{const_tag}.gz")
        return cls(pattern)

@dataclass
class RemoteResource:
    ftpserver:str
    directory:str
    query:RemoteQuery
    file_name:Optional[str] = None

    def __str__(self):
        return str({"ftpserver":self.ftpserver,"directory":self.directory,"file":self.file_name})


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
        remote_query = RemoteQuery.rnx2(date,constellation)
        remote_resource = RemoteResource(ftpserver=cls.ftpserver,directory=cls.daily_gps_dir,query=remote_query)
        return remote_resource
    
    @classmethod
    def get_rinex_3_nav(cls,date:datetime) -> RemoteResource:
        remote_query = RemoteQuery.rnx3(date)
        remote_resource = RemoteResource(ftpserver=cls.ftpserver,directory=cls.daily_gps_dir,query=remote_query)
        return remote_resource

    @classmethod
    def get_product_sp3(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/orbit"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.sp3(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)


    @classmethod
    def get_product_obx(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/orbit"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.obx(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)
                

    @classmethod
    def get_product_clk(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/clock"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.clk(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)

    @classmethod
    def get_product_sum(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/clock"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.sum(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)


    @classmethod
    def get_product_bias(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/bias"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.bias(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)

    @classmethod
    def get_product_erp(cls,date:datetime.date)->RemoteResource:
        year,doy = _parse_date(date)
        dir_extension = f"{year}/orbit"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.erp(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)

# class CDDIS:
#     ftpserver = "ftp://cddis.gsfc.nasa.gov"
#     daily_gps_dir = "gnss/data/daily"

#     constellation_tag = {
#         "gps": "n",
#         "glonass": "g",
#     }

#     @classmethod
#     def get_rinex_2_nav(
#         cls, date: datetime.date, constellation: Literal["gps", "glonass"] = "gps"
#     ) -> RemoteResource:
#         assert (
#             constellation in cls.constellation_tag.keys()
#         ), f"Constellation {constellation} not recognized"
#         const_tag = cls.constellation_tag[constellation]
#         year, doy = _parse_date(date)
#         dir_extension = f"{year}/{doy}/{year[2:]}{const_tag}"
#         file_name = f"brdc{doy}0.{year[2:]}{const_tag}.gz"
#         directory = "/".join([cls.daily_gps_dir, dir_extension])
#         return RemoteResource(
#             ftpserver=cls.ftpserver, directory=directory, file_name=file_name
#         )

#     @classmethod
#     def get_rinex_3_nav(cls, date: datetime) -> RemoteResource:
#         year, doy = _parse_date(date)
#         dir_extension = f"{year}/{doy}/{year[2:]}p"
#         file_name = f"BRDC00IGS_R_{year}{doy}0000_01D_MN.rnx.gz"
#         directory = "/".join([cls.daily_gps_dir, dir_extension])
#         return RemoteResource(
#             ftpserver=cls.ftpserver, directory=directory, file_name=file_name
#         )

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
        remote_query = RemoteQuery.rnx2(date, constellation)
        remote_resource = RemoteResource(
            ftpserver=cls.ftpserver, directory=cls.daily_gps_dir, query=remote_query
        )
        return remote_resource

    @classmethod
    def get_rinex_3_nav(cls, date: datetime) -> RemoteResource:
        remote_query = RemoteQuery.rnx3(date)
        remote_resource = RemoteResource(
            ftpserver=cls.ftpserver, directory=cls.daily_gps_dir, query=remote_query
        )
        return remote_resource


class CLSIGS:
    ftpserver = "ftp://igs.ign.fr"
    daily_gps_dir = "pub/igs/data"
    daily_products_dir = "pub/igs/products/mgex"

    @classmethod
    def get_rinex_2_nav(
        cls, date: datetime.date, constellation: Literal["gps", "glonass"] = "gps"
    ) -> RemoteResource:
        assert (
            constellation in cls.constellation_tag.keys()
        ), f"Constellation {constellation} not recognized"
        remote_query = RemoteQuery.rnx2(date, constellation)
        remote_resource = RemoteResource(
            ftpserver=cls.ftpserver, directory=cls.daily_gps_dir, query=remote_query
        )
        return remote_resource

    @classmethod
    def get_rinex_3_nav(cls, date: datetime) -> RemoteResource:
        remote_query = RemoteQuery.rnx3(date)
        remote_resource = RemoteResource(
            ftpserver=cls.ftpserver, directory=cls.daily_gps_dir, query=remote_query
        )
        return remote_resource

    @classmethod
    def get_product_sp3(cls,date:datetime.date)->RemoteResource:
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.sp3(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)


    @classmethod
    def get_product_clk(cls,date:datetime.date)->RemoteResource:
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.clk(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)

    @classmethod
    def get_product_erp(cls,date:datetime.date)->RemoteResource:
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.erp(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)
        

    @classmethod
    def get_product_orbit(cls,date:datetime.date)->RemoteResource:
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.obx(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)

    @classmethod
    def get_product_bias(cls,date:datetime.date)->RemoteResource:
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.bias(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,query=remote_query)

class Potsdam:
    ftpserver = "ftp://isdcftp.gfz-potsdam.de"
    daily_products_dir = "gnss/products/final"
