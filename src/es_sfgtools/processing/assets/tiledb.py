import tiledb
import numpy as np
from pathlib import Path
import datetime
import pandas as pd
from pandera import check_types
from pandera.typing import DataFrame
from functools import wraps
from typing import Optional,Dict,Literal
import matplotlib.pyplot as plt

from .observables import AcousticDataFrame,GNSSDataFrame,PositionDataFrame,ShotDataFrame

from es_sfgtools.utils.loggers import ProcessLogger as logger

filters = tiledb.FilterList([tiledb.ZstdFilter(7)])
TimeDomain = tiledb.Dim(name="time", dtype="datetime64[ms]")
TransponderDomain = tiledb.Dim(name="transponderID",dtype="ascii")
attribute_dict: Dict[str,tiledb.Attr] = {
    "east": tiledb.Attr(name="east", dtype=np.float32),
    "north": tiledb.Attr(name="north", dtype=np.float32),
    "up": tiledb.Attr(name="up", dtype=np.float32),
    "east_std": tiledb.Attr(name="east_std", dtype=np.float32,nullable=True),
    "north_std": tiledb.Attr(name="north_std", dtype=np.float32,nullable=True),
    "up_std": tiledb.Attr(name="up_std", dtype=np.float32,nullable=True),
    "latitude": tiledb.Attr(name="latitude", dtype=np.float32),
    "longitude": tiledb.Attr(name="longitude", dtype=np.float32),
    "height": tiledb.Attr(name="height", dtype=np.float32),
    "pingTime":tiledb.Attr(name="pingTime", dtype=np.float64),
    "returnTime":tiledb.Attr(name="returnTime",dtype=np.float64),
    "tt":tiledb.Attr(name="tt",dtype=np.float32),
    "dbv":tiledb.Attr(name="dbv",dtype=np.uint8),
    "xc":tiledb.Attr(name="xc",dtype=np.uint8),
    "snr":tiledb.Attr(name="snr",dtype=np.float32),
    "tat":tiledb.Attr(name="tat",dtype=np.float32),
}

GNSSAttributes = [
    attribute_dict["latitude"],
    attribute_dict["longitude"],
    attribute_dict["height"],
    attribute_dict["east"],
    attribute_dict["north"],
    attribute_dict["up"],
    # attribute_dict["east_std"],
    # attribute_dict["north_std"],
    # attribute_dict["up_std"],
    tiledb.Attr(name="number_of_satellites", dtype="uint8"),
    tiledb.Attr(name="pdop", dtype=np.float32),
]
GNSSArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain),
    attrs=GNSSAttributes,
    cell_order='col-major', 
    tile_order='row-major',
    allows_duplicates=False,
    coords_filters=filters,
)

PositionAttributes = [
    tiledb.Attr(name="head", dtype=np.float32),
    tiledb.Attr(name="pitch", dtype=np.float32),
    tiledb.Attr(name="roll", dtype=np.float32),
    attribute_dict["east"],
    attribute_dict["north"],
    attribute_dict["up"],
    attribute_dict["east_std"],
    attribute_dict["north_std"],
    attribute_dict["up_std"],

]
PositionArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain),
    attrs=PositionAttributes,
    cell_order='col-major', 
    tile_order='row-major',
    allows_duplicates=False,
    coords_filters=filters,
)

ShotDataAttributes = [
    tiledb.Attr(name="head0", dtype=np.float32),
    tiledb.Attr(name="pitch0", dtype=np.float32),
    tiledb.Attr(name="roll0", dtype=np.float32),
    tiledb.Attr(name="head1", dtype=np.float32),
    tiledb.Attr(name="pitch1", dtype=np.float32),
    tiledb.Attr(name="roll1", dtype=np.float32),
    tiledb.Attr(name="east0", dtype=np.float32),
    tiledb.Attr(name="north0", dtype=np.float32),
    tiledb.Attr(name="up0", dtype=np.float32),
    tiledb.Attr(name="east1", dtype=np.float32),
    tiledb.Attr(name="north1", dtype=np.float32),
    tiledb.Attr(name="up1", dtype=np.float32),
    attribute_dict["east_std"],
    attribute_dict["north_std"],
    attribute_dict["up_std"],
    attribute_dict["pingTime"],
    attribute_dict["returnTime"],
    attribute_dict["tt"],
    attribute_dict["dbv"],
    attribute_dict["xc"],
    attribute_dict["snr"],
    attribute_dict["tat"]
]

ShotDataArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(
        tiledb.Dim(name="triggerTime", dtype="datetime64[ms]"), TransponderDomain
    ),
    attrs=ShotDataAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

AcousticDataAttributes = [
    attribute_dict["pingTime"],
    attribute_dict["returnTime"],
    attribute_dict["tt"],
    attribute_dict["dbv"],
    attribute_dict["xc"],
    attribute_dict["snr"],
    attribute_dict["tat"],
]

AcousticArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain,TransponderDomain),
    attrs=AcousticDataAttributes,
    cell_order='col-major', 
    tile_order='row-major',
    allows_duplicates=False,
    coords_filters=filters,
)

# Create a configuration object
config = tiledb.Config()

# Set configuration parameters
config["vfs.s3.region"] = "us-east-2"

ctx = tiledb.Ctx(config=config)

filters1 = tiledb.FilterList([tiledb.ZstdFilter(level=7)])
filters2 = tiledb.FilterList([tiledb.ByteShuffleFilter(), tiledb.ZstdFilter(level=7)])
filters3 = tiledb.FilterList(
    [tiledb.BitWidthReductionFilter(), tiledb.ZstdFilter(level=7)]
)
filters4 = tiledb.FilterList(
    [
        tiledb.FloatScaleFilter(0.0001, offset=None, bytewidth=8),
        tiledb.ZstdFilter(level=7),
    ]
)
filters5 = tiledb.FilterList(
    [
        tiledb.FloatScaleFilter(0.001, offset=None, bytewidth=4),
        tiledb.ZstdFilter(level=7),
    ]
)

roll_periods = {"1D": 43200000}
for roll_period, tile_value in roll_periods.items():

    # Dimensions
    # time - dimension w/ millisecond precision and 12 hour tiles
    # sys - system (e.g. 0:GPS, 1:GLONASS, 2:SBAS, ...)
    # sat - satellite PRN or slot number
    # obs - GNSS observation type (e.g. 1C, 2X, 5Q, ...) mapped to integer
    d0 = tiledb.Dim(
        name="time",
        domain=(315964800000, 4102444800000),
        tile=tile_value,
        dtype=np.int64,
        filters=filters1,
    )
    d1 = tiledb.Dim(
        name="sys", domain=(0, 254), tile=1, dtype=np.uint8, filters=filters1
    )
    d2 = tiledb.Dim(
        name="sat", domain=(0, 254), tile=1, dtype=np.uint8, filters=filters1
    )
    d3 = tiledb.Dim(
        name="obs", domain=(0, 65534), tile=1, dtype=np.uint16, filters=filters1
    )

    # Attributes
    # range - pseudorange measurement in meters
    # phase - phase measurement in cycles
    # doppler - doppler measurement in Hz
    # snr - signal to noise in dB-Hz
    # slip - slip counter or locktime
    # flags - bit 0: LLI; bit 1: half-cycle; bit 2: BOC-tracking
    # fcn - GLONASS Frequency Channel Number (FCN); reserved for other systems
    a0 = tiledb.Attr(name="range", dtype=np.float64, filters=filters4)
    a1 = tiledb.Attr(name="phase", dtype=np.float64, filters=filters4)
    a2 = tiledb.Attr(name="doppler", dtype=np.float64, filters=filters4)
    a3 = tiledb.Attr(name="snr", dtype=np.float32, filters=filters5)
    a4 = tiledb.Attr(name="slip", dtype=np.uint16, filters=filters3)
    a5 = tiledb.Attr(name="flags", dtype=np.uint16, filters=filters3)
    a6 = tiledb.Attr(name="fcn", dtype=np.int8, filters=filters1)

    dom = tiledb.Domain(d0, d1, d2, d3)

    offsets_filters = tiledb.FilterList(
        [
            tiledb.PositiveDeltaFilter(),
            tiledb.BitWidthReductionFilter(),
            tiledb.ZstdFilter(level=7),
        ]
    )

    GNSSObsSchema = tiledb.ArraySchema(
        domain=dom,
        sparse=True,
        attrs=[a0, a1, a2, a3, a4, a5, a6],
        cell_order="row-major",
        tile_order="row-major",
        capacity=500000,
        offsets_filters=offsets_filters,
        ctx=ctx,
    )


class TBDArray:
    dataframe_schema = None
    array_schema = None 
    def __init__(self,uri:Path|str):
        if isinstance(uri,str):
            uri = Path(uri)
        self.uri = uri
        if not uri.exists():
            tiledb.Array.create(str(uri),self.array_schema)

    def write_df(self,df:pd.DataFrame,validate:bool=True):
        """
        Write a dataframe to the array
        
        Args:
            df (pd.DataFrame): The dataframe to write
            validate (bool, optional): Whether to validate the dataframe. Defaults to True.
        """
        logger.logdebug(f"Writing dataframe to {self.uri}")
        if validate:
            df_val = self.dataframe_schema.validate(df,lazy=True)
        tiledb.from_pandas(str(self.uri),df_val,mode='append')

    def read_df(self,start:datetime.datetime,end:datetime.datetime=None,validate:bool=True,**kwargs)->pd.DataFrame:
        """
        Read a dataframe from the array between the start and end dates
        
        Args:
            start (datetime.datetime): The start date
            end (datetime.datetime, optional): The end date. Defaults to None.
            validate (bool, optional): Whether to validate the dataframe. Defaults to True.

        Returns:
            pd.DataFrame: dataframe
        """
        logger.logdebug(f"Reading dataframe from {self.uri}")
        # TODO slice array by start and end and return the dataframe
        if end is None:
            end = start + datetime.timedelta(days=1)
        else:
            end = end + datetime.timedelta(days=1)
        with tiledb.open(str(self.uri), mode="r") as array:
            try:
                df = array.df[slice(np.datetime64(start), np.datetime64(end))]
            except IndexError as e:
                print(e)
                return None
        if validate:
            df = self.dataframe_schema.validate(df,lazy=True)
        return df

    def get_unique_dates(self,field:str)->np.ndarray:
        with tiledb.open(str(self.uri), mode="r") as array:
            values = array[:][field]
            try:
                values = values.astype("datetime64[D]")
                return np.unique(values)
            except Exception as e:
                print(e)
                return None

    def consolidate(self):
        ctx = tiledb.Ctx()
        config = tiledb.Config()
        config["sm.consolidation.steps"] = 3
        uri = tiledb.consolidate(uri=str(self.uri),ctx=ctx,config=config)
        print(f"Consolidate to {uri}")
        tiledb.vacuum(str(self.uri))

    def view(self, network: str = "", station: str = ""):
        dates = self.get_unique_dates()
        if dates.shape[0] == 0:
            raise ValueError("No dates found in the array")
        # Plot the values, with a marker seperating the dates
        fig, ax = plt.subplots()
        date_tick_map = {i: date for i, date in enumerate(dates)}

        for i, date in enumerate(dates):
            ax.axvline(x=i + 1, color="black", linestyle="-")

        ax.xaxis.set_ticks(
            [i + 1 for i in date_tick_map.keys()],
            [str(date) for date in date_tick_map.values()],
        )
        ax.yaxis.set_ticks([])
        ax.set_xlabel("Date")
        ax.set_ylabel("Found Data")
        fig.suptitle(f"Found {self.name} Dates For {network} {station}")
        plt.show()


class TDBAcousticArray(TBDArray):
    dataframe_schema = AcousticDataFrame
    array_schema = AcousticArraySchema
    def __init__(self,uri:Path|str):
        super().__init__(uri)
    def get_unique_dates(self,field="triggerTime")->np.ndarray:
        return super().get_unique_dates(field)

    def write_df(self, df: pd.DataFrame):
        df = self.dataframe_schema.validate(df, lazy=True)
        tiledb.from_pandas(str(self.uri), df, mode="append")

    def read_df(self, start: datetime, end: datetime = None, **kwargs) -> pd.DataFrame:
        if end is None:
            end = start
        with tiledb.open(str(self.uri), mode="r") as array:
            df = array.df[slice(np.datetime64(start), np.datetime64(end)), :]
        df = self.dataframe_schema.validate(df, lazy=True)
        return df


class TDBGNSSArray(TBDArray):
    dataframe_schema = GNSSDataFrame
    array_schema = GNSSArraySchema
    name = "GNSS Data"
    def __init__(self,uri:Path|str):
        super().__init__(uri)

    def get_unique_dates(self,field="time")->np.ndarray:
        return super().get_unique_dates(field)


class TDBPositionArray(TBDArray):
    dataframe_schema = PositionDataFrame
    array_schema = PositionArraySchema
    def __init__(self,uri:Path|str):
        super().__init__(uri)
    def get_unique_dates(self,field="time")->np.ndarray:
        return super().get_unique_dates(field)

class TDBShotDataArray(TBDArray):
    dataframe_schema = ShotDataFrame
    array_schema = ShotDataArraySchema
    name = "Shot Data"
    def __init__(self,uri:Path|str):
        super().__init__(uri)

    def get_unique_dates(self,field="triggerTime")->np.ndarray:
        return super().get_unique_dates(field)

    def read_df(self, start: datetime, end: datetime = None, **kwargs) -> pd.DataFrame:
        """ 
        Read a dataframe from the array between the start and end dates 
        
        Args:
            start (datetime.datetime): The start date
            end (datetime.datetime, optional): The end date. Defaults to None.
        
        Returns:
            pd.DataFrame: dataframe
        """ 

        logger.logdebug(f"Reading dataframe from {self.uri} for {start} to {end}")
        # TODO slice array by start and end and return the dataframe
        if end is None:
            end = start + datetime.timedelta(days=1)
        with tiledb.open(str(self.uri), mode="r") as array:
            try:
                df = array.df[slice(np.datetime64(start), np.datetime64(end)),:]
            except IndexError as e:
                print(e)
                return None
        # self.dataframe_schema.validate(df, lazy=True)
        return df

    def write_df(self, df: pd.DataFrame, validate: bool = True):
        """ 
        Write a dataframe to the array

        Args:
            df (pd.DataFrame): The dataframe to write
            validate (bool, optional): Whether to validate the dataframe. Defaults to True.
        """
        logger.logdebug(f"Writing dataframe to {self.uri}")
        if validate:
            df_val = self.dataframe_schema.validate(df, lazy=True)
        else:
            df_val = df
        df_val.triggerTime = df_val.triggerTime.astype("datetime64[ns]")
        tiledb.from_pandas(str(self.uri), df_val, mode="append")

class TDBGNSSObsArray(TBDArray):
    array_schema = GNSSObsSchema
    def __init__(self,uri:Path|str):
        super().__init__(uri)

    def get_unique_dates(self, field: str="time") -> np.ndarray:
        with tiledb.open(str(self.uri), mode="r") as array:
            values = array[:][field]
            try:
                values = values.astype("datetime64[ms]")
                return np.unique(values)
            except Exception as e:
                print(e)
                return None
