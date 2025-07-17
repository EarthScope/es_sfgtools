from pydantic import BaseModel
from dataclasses import dataclass
import datetime
from typing import Literal,Tuple,List,Optional
from enum import Enum
import re

GNSS_START_TIME = datetime.datetime(1980, 1, 6, tzinfo=datetime.timezone.utc)  # GNSS start time

from ..logging import PRIDELogger as logger


def _parse_date(date: datetime.date | datetime.datetime) -> Tuple[str, str]:
    """
    Parse a date or datetime object and return the year and day of year (DOY) as strings.
    Args:
        date (datetime.date | datetime.datetime): The date or datetime object to parse.
    Returns:
        Tuple[str, str]: A tuple containing the year and the day of year (DOY) as strings.
    """

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
    """
    Convert a given date to the corresponding GPS week number.

    The GPS week number is calculated as the number of weeks since the start of the GPS epoch (January 6, 1980).

    Args:
        date (datetime.date | datetime.datetime): The date to be converted. Can be either a datetime.date or datetime.datetime object.

    Returns:
        int: The GPS week number corresponding to the given date.
    """
    # get the number of weeks since the start of the GPS epoch

    if isinstance(date, datetime.datetime):
        date = date.date()
    time_since_epoch = date - GNSS_START_TIME.date()
    gps_week = time_since_epoch.days // 7
    return gps_week


class RemoteQuery:
    """
    A class used to create and manage remote query regex patterns for GNSS resources.

    Attributes:
        pattern : re.Pattern
            The compiled regular expression pattern used for searching.

        sort_order : List[str]
            The order in which search results should be sorted, based on the first match in this list.
            This order is based on the product type, including "Final" (FIN), "Rapid" (RAP), and "Real time Streaming" (RTS).

    Methods:
        sp3(date: datetime.date) -> RemoteQuery
            Creates a RemoteQuery instance for SP3 files based on the given date.
        obx(date: datetime.date) -> RemoteQuery
            Creates a RemoteQuery instance for OBX files based on the given date.
        clk(date: datetime.date) -> RemoteQuery
            Creates a RemoteQuery instance for CLK files based on the given date.
        sum(date: datetime.date) -> RemoteQuery
            Creates a RemoteQuery instance for SUM files based on the given date.
        bias(date: datetime.date) -> RemoteQuery
            Creates a RemoteQuery instance for BIA files based on the given date.
        erp(date: datetime.date) -> RemoteQuery
            Creates a RemoteQuery instance for ERP files based on the given date.
        rnx3(date: datetime.date) -> RemoteQuery
            Creates a RemoteQuery instance for RNX3 files based on the given date.
        rnx2(date: datetime.date, constellation: Literal["gps", "glonass"]) -> RemoteQuery
            Creates a RemoteQuery instance for RNX2 files based on the given date and constellation.
    """

    def __init__(self, pattern: re.Pattern, sort_order: List[str] = []):
        self.pattern = pattern
        self.sort_order = sort_order

    @classmethod
    def sp3(cls, date: datetime.date) -> "RemoteQuery":
        """
        Creates a RemoteQuery object to search for SP3 files based on the given date.
        Args:
            date (datetime.date): The date for which to search SP3 files.
        Returns:
            RemoteQuery: An instance of RemoteQuery configured with the search pattern
                         and search order for SP3 files.
        """

        year, doy = _parse_date(date)
        search_pattern = rf".*{year}{doy}.*SP3.*"
        pattern = re.compile(search_pattern)
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def obx(cls, date: datetime.date) -> "RemoteQuery":
        """
        Create a RemoteQuery object to search for OBX files based on the given date.
        Args:
            date (datetime.date): The date for which to search OBX files.
        Returns:
            RemoteQuery: An instance of RemoteQuery initialized with a pattern to match OBX files
                         and a predefined search order.
        """

        year, doy = _parse_date(date)
        pattern = re.compile(rf".*{year}{doy}.*OBX.*")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def clk(cls, date: datetime.date) -> "RemoteQuery":
        """
        Create a RemoteQuery object to search for GNSS clock files for a given date.
        Args:
            date (datetime.date): The date for which to search GNSS clock files.
        Returns:
            RemoteQuery: An instance of RemoteQuery initialized with the search pattern and order.
        """

        year, doy = _parse_date(date)
        pattern = re.compile(rf".*{year}{doy}.*CLK.*")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def sum(cls, date: datetime.date) -> "RemoteQuery":
        """
        Create a RemoteQuery instance to find GNSS sum files for a given date.
        Args:
            date (datetime.date): The date for which to find the GNSS sum files.
        Returns:
            RemoteQuery: An instance of RemoteQuery initialized with a pattern to match
                         GNSS sum files for the specified date and a search order.
        """

        year, doy = _parse_date(date)
        pattern = re.compile(rf".*{year}{doy}.*SUM.*")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def bias(cls, date: datetime.date) -> "RemoteQuery":
        """
        Creates a RemoteQuery object to search for GNSS bias files based on the given date.
        Args:
            date (datetime.date): The date for which to search for GNSS bias files.
        Returns:
            RemoteQuery: An object configured to search for GNSS bias files matching the specified date.
        """

        year, doy = _parse_date(date)
        pattern = re.compile(rf".*{year}{doy}.*BIA.*")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def erp(cls, date: datetime.date) -> "RemoteQuery":
        """
        Creates a RemoteQuery object to search for Earth Rotation Parameters (ERP) files 
        based on the given date.
        Args:
            date (datetime.date): The date for which to search ERP files.
        Returns:
            RemoteQuery: An instance of RemoteQuery initialized with the search pattern 
            and order for ERP files.
        """

        year, doy = _parse_date(date)
        pattern = re.compile(rf".*{year}{doy}.*ERP.*")
        search_order = ["FIN", "RAP", "RTS"]
        return cls(pattern, search_order)

    @classmethod
    def rnx3(cls, date: datetime.date) -> "RemoteQuery":
        """
        Generate a RemoteQuery object for RINEX 3 files based on the given date.
        Args:
            date (datetime.date): The date for which to generate the query.
        Returns:
            RemoteQuery: An instance of RemoteQuery with a pattern matching RINEX 3 files for the specified date.
        """

        year, doy = _parse_date(date)
        pattern = re.compile(rf"BRDC.*{year}{doy}.*rnx.*")
        return cls(pattern)

    @classmethod
    def rnx2(cls, date: datetime.date, constellation: Literal["gps", "glonass"]) -> "RemoteQuery":
        """
        Generate a RemoteQuery object for RINEX files based on the given date and GNSS constellation.
        Args:
            date (datetime.date): The date for which to generate the query.
            constellation (Literal["gps", "glonass"]): The GNSS constellation to query. 
                Must be either "gps" or "glonass".
        Returns:
            RemoteQuery: An object representing the query for the specified RINEX files.
        Raises:
            KeyError: If the provided constellation is not "gps" or "glonass".
        """

        year, doy = _parse_date(date)
        constellation_tag = {
            "gps": "n",
            "glonass": "g",
        }
        const_tag = constellation_tag[constellation]
        pattern = re.compile(rf"brdc{doy}0.{year[2:]}{const_tag}.gz")
        return cls(pattern)

@dataclass
class RemoteResource:
    """
    A class to represent a remote resource accessed via FTP.

    Attributes:
        ftpserver (str):
            The FTP server address.
        directory (str):
            The directory on the FTP server.
        remote_query (RemoteQuery):
            The query object to interact with the remote resource.
        file_name (Optional[str]):
            The name of the file on the FTP server (default is None).

    Methods:
        __str__():
            Returns a string representation of the remote resource.
    """

    ftpserver:str
    directory:str
    remote_query:RemoteQuery
    file_name:Optional[str] = None

    def __str__(self):
        return str({"ftpserver":self.ftpserver,"directory":self.directory,"file":self.file_name})


class WuhanIGS:
    '''
    The `WuhanIGS` class provides methods to retrieve various GNSS (Global Navigation Satellite System) remote resources from the Wuhan IGS (International GNSS Service) FTP server. The class includes methods to get RINEX 2 and 3 navigation files, as well as various GNSS products such as SP3, OBX, clock, sum, bias, and ERP (Earth Rotation Parameters).

    Attributes:
        ftpserver (str): The FTP server URL for Wuhan IGS.
        daily_gps_dir (str): The directory path for daily GPS data.
        daily_product_dir (str): The directory path for daily GNSS products.
        constellation_tag (dict): A dictionary mapping satellite constellations to their respective tags.

    Methods:
        get_rinex_2_nav(date: datetime.date, constellation: Literal["gps", "glonass"] = "gps") -> RemoteResource:

        get_rinex_3_nav(date: datetime) -> RemoteResource:
            Retrieve the RINEX 3 navigation file remote resource for a given date.

        get_product_sp3(date: datetime.date) -> RemoteResource:
            Retrieve the SP3 GNSS product remote resource for a given date.

        get_product_obx(date: datetime.date) -> RemoteResource:
            Retrieve the OBX GNSS product remote resource for a given date.

        get_product_clk(date: datetime.date) -> RemoteResource:
            Retrieve the clock GNSS product remote resource for a given date.

        get_product_sum(date: datetime.date) -> RemoteResource:
            Retrieve the sum GNSS product remote resource for a given date.

        get_product_bias(date: datetime.date) -> RemoteResource:

        get_product_erp(date: datetime.date) -> RemoteResource:
            Retrieve the Earth Rotation Parameters (ERP) GNSS product remote resource for a given date.

    '''
    ftpserver = "ftp://igs.gnsswhu.cn"
    daily_gps_dir = "pub/gps/data/daily"
    daily_product_dir = "pub/whu/phasebias"

    constellation_tag = {
        "gps":'n',
        "glonass":'g',
    }

    @classmethod
    def get_rinex_2_nav(cls,date:datetime.date, constellation:Literal["gps","glonass"]="gps")->RemoteResource:
        """
        Retrieve RINEX 2 navigation file remote resource for a given date and constellation.
        Args:
            date (datetime.date): The date for which to retrieve the RINEX 2 navigation file.
            constellation (Literal["gps", "glonass"], optional): The satellite constellation to use. 
                Defaults to "gps".
        Returns:
            RemoteResource: An object representing the remote resource for the RINEX 2 navigation file.
        Raises:
            AssertionError: If the provided constellation is not recognized.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for RINEX 2 navigation file for {date} and constellation {constellation}")
        assert constellation in cls.constellation_tag.keys(),f"Constellation {constellation} not recognized"
        year,doy = _parse_date(date)
        dir_extension = f"{year}/{doy}/{year[2:]}p"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        remote_query = RemoteQuery.rnx2(date,constellation)
        remote_resource = RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)
        return remote_resource

    @classmethod
    def get_rinex_3_nav(cls,date:datetime) -> RemoteResource:
        """
        Generates the RINEX 3 navigation file remote resource for a given date.
        Args:
            date (datetime): The date for which to retrieve the RINEX 3 navigation file.
        Returns:
            RemoteResource: An object representing the remote resource for the RINEX 3 navigation file.
        """
        logger.logdebug(f"{__file__}: Generating the RemoteResource for RINEX 3 navigation file for {date}")
        remote_query = RemoteQuery.rnx3(date)
        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}/{year[2:]}p"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        remote_resource = RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)
        return remote_resource

    @classmethod
    def get_product_sp3(cls,date:datetime.date)->RemoteResource:
        """
        Generates the SP3 GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the SP3 product.
        Returns:
            RemoteResource: An object representing the remote resource for the SP3 product.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for SP3 GNSS product for {date}")
        year,doy = _parse_date(date)
        dir_extension = f"{year}/orbit"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.sp3(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_obx(cls,date:datetime.date)->RemoteResource:
        """
        Generates the OBX GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the OBX product.
        Returns:
            RemoteResource: An object representing the remote resource for the OBX product.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for OBX GNSS product for {date}")
        year,doy = _parse_date(date)
        dir_extension = f"{year}/orbit"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.obx(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_clk(cls,date:datetime.date)->RemoteResource:
        """
        Generates the clock GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the clock product.
        Returns:
            RemoteResource: An object representing the remote resource for the clock product.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for clock GNSS product for {date}")
        year,doy = _parse_date(date)
        dir_extension = f"{year}/clock"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.clk(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_sum(cls,date:datetime.date)->RemoteResource:
        """
        Generates the sum GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the GNSS product sum.
        Returns:
            RemoteResource: An object representing the remote resource containing the GNSS product sum.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for sum GNSS product for {date}")
        year,doy = _parse_date(date)
        dir_extension = f"{year}/clock"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.sum(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_bias(cls,date:datetime.date)->RemoteResource:
        """
        Retrieve the bias GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the product bias.
        Returns:
            RemoteResource: An object representing the remote resource containing the product bias.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for bias GNSS product for {date}")
        year,doy = _parse_date(date)
        dir_extension = f"{year}/bias"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.bias(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_erp(cls,date:datetime.date)->RemoteResource:
        """
        Generates the Earth Rotation Parameters (ERP) GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the ERP product.
        Returns:
            RemoteResource: An object representing the remote resource containing the ERP product.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for ERP GNSS product for {date}")
        year,doy = _parse_date(date)
        dir_extension = f"{year}/orbit"
        directory = "/".join([cls.daily_product_dir, dir_extension])
        remote_query = RemoteQuery.erp(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

class GSSC:
    """
    A class to interact with the GNSS Service Center (GSSC) FTP server for retrieving RINEX navigation files.

    Attributes:
        ftpserver (str): The FTP server URL for GSSC.
        daily_gps_dir (str): The directory path for daily GPS data on the FTP server.
        constellation_tag (dict): A dictionary mapping constellation names to their respective tags.

    Methods:
        get_rinex_2_nav(date: datetime.date, constellation: Literal["gps", "glonass"] = "gps") -> RemoteResource:
            Generates a RINEX 2 navigation file remote resource for the specified date and constellation.
        get_rinex_3_nav(date: datetime) -> RemoteResource:
            Generates a RINEX 3 navigation file remote resource for the specified date.
    """

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
        '''
        Generates a RINEX 2 navigation file remote resource for a given date and constellation.
        Args:
            date (datetime.date): The date for which to retrieve the RINEX 2 navigation file.
            constellation (Literal["gps", "glonass"], optional): The satellite constellation to use. 
                Defaults to "gps".
        Returns:
            RemoteResource: An object representing the remote resource for the RINEX 2 navigation file.
        Raises:
            AssertionError: If the provided constellation is not recognized.
        '''

        logger.logdebug(f"{__file__}: Generating the RemoteResource for RINEX 2 navigation file for {date} and constellation {constellation}")
        assert (
            constellation in cls.constellation_tag.keys()
        ), f"Constellation {constellation} not recognized"

        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        remote_query = RemoteQuery.rnx2(date, constellation)
        remote_resource = RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, remote_query=remote_query
        )
        return remote_resource

    @classmethod
    def get_rinex_3_nav(cls, date: datetime) -> RemoteResource:
        """
        Generates a RINEX 3 navigation file remote resource for a given date.
        Args:
            date (datetime): The date for which to retrieve the RINEX 3 navigation file.
        Returns:
            RemoteResource: An object representing the remote resource for the RINEX 3 navigation file.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for RINEX 3 navigation file for {date}")
        remote_query = RemoteQuery.rnx3(date)
        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        remote_resource = RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, remote_query=remote_query
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

        """
        Generates a RINEX 2 navigation file remote resource for a given date and constellation.
        Args:
            date (datetime.date): The date for which to retrieve the RINEX 2 navigation file.
            constellation (Literal["gps", "glonass"], optional): The satellite constellation to use.
                Defaults to "gps".
        Returns:
            RemoteResource: An object representing the remote resource for the RINEX 2 navigation file.
        Raises:
            AssertionError: If the provided constellation is not recognized.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for RINEX 2 navigation file for {date} and constellation {constellation}")
        assert (
            constellation in cls.constellation_tag.keys()
        ), f"Constellation {constellation} not recognized"

        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        remote_query = RemoteQuery.rnx2(date, constellation)
        remote_resource = RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, remote_query=remote_query
        )
        return remote_resource

    @classmethod
    def get_rinex_3_nav(cls, date: datetime) -> RemoteResource:
        """
        Generates a RINEX 3 navigation file remote resource for a given date.
        Args:
            date (datetime): The date for which to retrieve the RINEX 3 navigation file.
        Returns:
            RemoteResource: An object representing the remote resource for the RINEX 3 navigation file.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for RINEX 3 navigation file for {date}")
        remote_query = RemoteQuery.rnx3(date)
        year, doy = _parse_date(date)
        dir_extension = f"{year}/{doy}"
        directory = "/".join([cls.daily_gps_dir, dir_extension])
        remote_resource = RemoteResource(
            ftpserver=cls.ftpserver, directory=directory, remote_query=remote_query
        )
        return remote_resource

    @classmethod
    def get_product_sp3(cls,date:datetime.date)->RemoteResource:
        """
        Generates a SP3 GNSS product remote resource for a given date.

        Args:
            date (datetime.date): The date for which to retrieve the SP3 product.

        Returns:
            RemoteResource: An object representing the remote resource for the SP3 product.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for SP3 GNSS product for {date}")
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.sp3(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_clk(cls,date:datetime.date)->RemoteResource:
        """
        Generates a clock GNSS product remote resource for a given date.

        Args:
            date (datetime.date): The date for which to retrieve the clock product.
        Returns:
            RemoteResource: An object representing the remote resource for the clock product.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for clock GNSS product for {date}")
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.clk(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_erp(cls,date:datetime.date)->RemoteResource:
        """
        Generates a Earth Rotation Parameters (ERP) GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the ERP product.
        Returns:
            RemoteResource: An object representing the remote resource for the ERP product.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for ERP GNSS product for {date}")
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.erp(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_obx(cls,date:datetime.date)->RemoteResource:
        """
        Generates a OBX GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the OBX product.
        Returns:
            RemoteResource: An object representing the remote resource for the OBX product.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for OBX GNSS product for {date}")
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.obx(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

    @classmethod
    def get_product_bias(cls,date:datetime.date)->RemoteResource:
        """
        Generates a bias GNSS product remote resource for a given date.
        Args:
            date (datetime.date): The date for which to retrieve the product bias.
        Returns:
            RemoteResource: An object representing the remote resource for the product bias.
        """

        logger.logdebug(f"{__file__}: Generating the RemoteResource for bias GNSS product for {date}")
        gps_week = _date_to_gps_week(date)
        dir_extension = f"{gps_week}"
        directory = "/".join([cls.daily_products_dir, dir_extension])
        remote_query = RemoteQuery.bias(date)
        return RemoteResource(ftpserver=cls.ftpserver,directory=directory,remote_query=remote_query)

class Potsdam:
    ftpserver = "ftp://isdcftp.gfz-potsdam.de"
    daily_products_dir = "gnss/products/final"
