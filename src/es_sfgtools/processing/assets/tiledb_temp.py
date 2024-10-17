import tiledb
from pathlib import Path
import pandas as pd
from typing import Optional,Dict

filters = tiledb.FilterList([tiledb.ZstdFilter(5)])
TimeDomain = tiledb.Dim(name="time", domain=(0,), tile=100, dtype="datetime64['ms']")
attribute_dict: Dict[str,tiledb.Attr] = {
    "east": tiledb.Attr(name="east", dtype="float64"),
    "north": tiledb.Attr(name="north", dtype="float64"),
    "up": tiledb.Attr(name="up", dtype="float64"),
    "east_std": tiledb.Attr(name="east_std", dtype="float64",nullable=True),
    "north_std": tiledb.Attr(name="north_std", dtype="float64",nullable=True),
    "up_std": tiledb.Attr(name="up_std", dtype="float64",nullable=True),
    "latitude": tiledb.Attr(name="latitude", dtype="float64"),
    "longitude": tiledb.Attr(name="longitude", dtype="float64"),
    "height": tiledb.Attr(name="height", dtype="float64")
}

GNSSAttributes = [
    attribute_dict["latitude"],
    attribute_dict["longitude"],
    attribute_dict["height"],
    attribute_dict["east"],
    attribute_dict["north"],
    attribute_dict["up"],
    attribute_dict["east_std"],
    attribute_dict["north_std"],
    attribute_dict["up_std"],
    tiledb.Attr(name="number_of_satellites", dtype="uint8"),
    tiledb.Attr(name="pdop", dtype="float64"),
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
    tiledb.Attr(name="head", dtype="float64"),
    tiledb.Attr(name="pitch", dtype="float64"),
    tiledb.Attr(name="roll", dtype="float64"),
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
    tiledb.Attr(name="head0", dtype="float64"),
    tiledb.Attr(name="pitch0", dtype="float64"),
    tiledb.Attr(name="roll0", dtype="float64"),
    tiledb.Attr(name="head1", dtype="float64"),
    tiledb.Attr(name="pitch1", dtype="float64"),
    tiledb.Attr(name="roll1", dtype="float64"),
    tiledb.Attr(name="east0", dtype="float64"),
    tiledb.Attr(name="north0", dtype="float64"),
    tiledb.Attr(name="up0", dtype="float64"),
    tiledb.Attr(name="east1", dtype="float64"),
    tiledb.Attr(name="north1", dtype="float64"),
    tiledb.Attr(name="up1", dtype="float64"),
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

accousticTimeDim = tiledb.Dim(name="triggerTime", domain=(0,), tile=100, dtype="datetime64[ms]")
acousticIDDim = tiledb.Dim(name="transponderID", domain=(0,), tile=100, dtype="U")

AcousticDataAttributes = [
    tiledb.Attr(name="transponderID",dtype="U"),
    tiledb.Attr(name="pingTime",dtype="float64"),
    tiledb.Attr(name="returnTime",dtype="float64"),
    tiledb.Attr(name="tt",dtype="float64"),
    tiledb.Attr(name="dbv",dtype="np.uint8"),
    tiledb.Attr(name="xc",dtype="np.uint8"),
    tiledb.Attr(name="snr",dtype="float64"),
    tiledb.Attr(name="tat",dtype="float64"),
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