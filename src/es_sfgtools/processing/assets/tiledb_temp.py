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

filters = tiledb.FilterList([tiledb.ZstdFilter(5)])
TimeDomain = tiledb.Dim(name="time", dtype="datetime64[ms]")
attribute_dict: Dict[str,tiledb.Attr] = {
    "east": tiledb.Attr(name="east", dtype=np.float32),
    "north": tiledb.Attr(name="north", dtype=np.float32),
    "up": tiledb.Attr(name="up", dtype=np.float32),
    "east_std": tiledb.Attr(name="east_std", dtype=np.float32,nullable=True),
    "north_std": tiledb.Attr(name="north_std", dtype=np.float32,nullable=True),
    "up_std": tiledb.Attr(name="up_std", dtype=np.float32,nullable=True),
    "latitude": tiledb.Attr(name="latitude", dtype=np.float32),
    "longitude": tiledb.Attr(name="longitude", dtype=np.float32),
    "height": tiledb.Attr(name="height", dtype=np.float32)
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
]

ShotDataArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain),
    attrs=ShotDataAttributes,
    cell_order='col-major', 
    tile_order='row-major',
    allows_duplicates=False,
    coords_filters=filters,
)

accousticTimeDim = tiledb.Dim(name="triggerTime", dtype="datetime64[ms]")
acousticIDDim = tiledb.Dim(name="transponderID",dtype="ascii")

AcousticDataAttributes = [
    
    tiledb.Attr(name="pingTime",dtype=np.float32),
    tiledb.Attr(name="returnTime",dtype=np.float32),
    tiledb.Attr(name="tt",dtype=np.float32),
    tiledb.Attr(name="dbv",dtype=np.uint8),
    tiledb.Attr(name="xc",dtype=np.uint8),
    tiledb.Attr(name="snr",dtype=np.float32),
    tiledb.Attr(name="tat",dtype=np.float32),
]
AcousticArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(accousticTimeDim,acousticIDDim),
    attrs=AcousticDataAttributes,
    cell_order='col-major', 
    tile_order='row-major',
    allows_duplicates=True,
    coords_filters=filters,
)


class TBDArray:
    dataframe_schema = None
    def __init__(self,uri:Path|str):
        if isinstance(uri,str):
            uri = Path(uri)
        self.uri = uri
        if not uri.exists():
            tiledb.Array.create(str(uri),AcousticArraySchema)
    
    def write_df(self,df:pd.DataFrame):
        df = self.dataframe_schema.validate(df,lazy=True)
        tiledb.from_pandas(str(self.uri),df,mode='append')

    def read_df(self,start:datetime,end:datetime,**kwargs)->pd.DataFrame:
        with tiledb.open(str(self.uri), mode="r") as array:
            df = array.df[slice(np.datetime64(start), np.datetime64(end)), :]
        df = self.dataframe_schema.validate(df,lazy=True)
        return df
    

class TDBAcousticArray(TBDArray):
    dataframe_schema = AcousticDataFrame
    def __init__(self,uri:Path|str):
        super().__init__(uri)

    
class TDBGNSSArray(TBDArray):
    dataframe_schema = GNSSDataFrame
    def __init__(self,uri:Path|str):
        super().__init__(uri)


class TDBPositionArray(TBDArray):
    dataframe_schema = PositionDataFrame
    def __init__(self,uri:Path|str):
        super().__init__(uri)

class TDBShotDataArray(TBDArray):
    dataframe_schema = ShotDataFrame
    def __init__(self,uri:Path|str):
        super().__init__(uri)