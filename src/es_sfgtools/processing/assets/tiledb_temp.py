import tiledb
import numpy as np
from pathlib import Path
from datetime import datetime
import pandas as pd
from pandera import check_types
from pandera.typing import DataFrame
from functools import wraps
from typing import Optional,Dict,Literal

from .observables import AcousticDataFrame,GNSSDataFrame,PositionDataFrame,ShotDataFrame

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
    "pingTime":tiledb.Attr(name="pingTime", dtype=np.float32),
    "returnTime":tiledb.Attr(name="returnTime",dtype=np.float32),
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


class TBDArray:
    dataframe_schema = None
    array_schema = None 
    def __init__(self,uri:Path|str):
        if isinstance(uri,str):
            uri = Path(uri)
        self.uri = uri
        if not uri.exists():
            tiledb.Array.create(str(uri),self.array_schema)
    
    def write_df(self,df:pd.DataFrame):
        df = self.dataframe_schema.validate(df,lazy=True)
        tiledb.from_pandas(str(self.uri),df,mode='append')

    def read_df(self,start:datetime,end:datetime=None,**kwargs)->pd.DataFrame:

        # TODO slice array by start and end and return the dataframe
        if end is None:
            end = start
        with tiledb.open(str(self.uri), mode="r") as array:
            try:
                df = array.df[slice(np.datetime64(start), np.datetime64(end)), :]
            except IndexError as e:
                print(e)
                return None
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
    def __init__(self,uri:Path|str):
        super().__init__(uri)

    def get_unique_dates(self,field="triggerTime")->np.ndarray:
        return super().get_unique_dates(field)

