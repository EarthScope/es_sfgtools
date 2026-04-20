"""
TileDB array schema definitions for seafloor geodesy data types.

Defines schemas for kinematic position, IMU position, acoustic,
shot data, and GNSS observation arrays.
"""

import os

import numpy as np
import tiledb

filters = tiledb.FilterList([tiledb.ZstdFilter(7)])
TimeDomain = tiledb.Dim(name="time", dtype="datetime64[ms]")
TransponderDomain = tiledb.Dim(name="transponderID", dtype="ascii")
attribute_dict: dict[str, tiledb.Attr] = {
    "east": tiledb.Attr(name="east", dtype=np.float64),
    "north": tiledb.Attr(name="north", dtype=np.float64),
    "up": tiledb.Attr(name="up", dtype=np.float64),
    "east_std0": tiledb.Attr(name="east_std0", dtype=np.float64, nullable=True),
    "north_std0": tiledb.Attr(name="north_std0", dtype=np.float64, nullable=True),
    "up_std0": tiledb.Attr(name="up_std0", dtype=np.float64, nullable=True),
    "east_std1": tiledb.Attr(name="east_std1", dtype=np.float64, nullable=True),
    "north_std1": tiledb.Attr(name="north_std1", dtype=np.float64, nullable=True),
    "up_std1": tiledb.Attr(name="up_std1", dtype=np.float64, nullable=True),
    "latitude": tiledb.Attr(name="latitude", dtype=np.float64),
    "longitude": tiledb.Attr(name="longitude", dtype=np.float64),
    "height": tiledb.Attr(name="height", dtype=np.float64),
    "latitude_std": tiledb.Attr(name="latitude_std", dtype=np.float64, nullable=True),
    "longitude_std": tiledb.Attr(name="longitude_std", dtype=np.float64, nullable=True),
    "height_std": tiledb.Attr(name="height_std", dtype=np.float64, nullable=True),
    "returnTime": tiledb.Attr(name="returnTime", dtype="datetime64[ns]"),
    "tt": tiledb.Attr(name="tt", dtype=np.float64),
    "dbv": tiledb.Attr(name="dbv", dtype=np.float32),
    "xc": tiledb.Attr(name="xc", dtype=np.uint8),
    "snr": tiledb.Attr(name="snr", dtype=np.float64),
    "tat": tiledb.Attr(name="tat", dtype=np.float64),
    "isUpdated": tiledb.Attr(name="isUpdated", dtype=np.bool_),
    "northVelocity": tiledb.Attr(name="northVelocity", dtype=np.float64),
    "eastVelocity": tiledb.Attr(name="eastVelocity", dtype=np.float64),
    "upVelocity": tiledb.Attr(name="upVelocity", dtype=np.float64),
    "northVelocity_std": tiledb.Attr(name="northVelocity_std", dtype=np.float64, nullable=True),
    "eastVelocity_std": tiledb.Attr(name="eastVelocity_std", dtype=np.float64, nullable=True),
    "upVelocity_std": tiledb.Attr(name="upVelocity_std", dtype=np.float64, nullable=True),
    "roll": tiledb.Attr(name="roll", dtype=np.float64),
    "pitch": tiledb.Attr(name="pitch", dtype=np.float64),
    "azimuth": tiledb.Attr(name="azimuth", dtype=np.float64),
    "roll_std": tiledb.Attr(name="roll_std", dtype=np.float64, nullable=True),
    "pitch_std": tiledb.Attr(name="pitch_std", dtype=np.float64, nullable=True),
    "azimuth_std": tiledb.Attr(name="azimuth_std", dtype=np.float64, nullable=True),
}

KinPositionAttributes = [
    attribute_dict["latitude"],
    attribute_dict["longitude"],
    attribute_dict["height"],
    attribute_dict["east"],
    attribute_dict["north"],
    attribute_dict["up"],
    tiledb.Attr(name="number_of_satellites", dtype="uint8"),
    tiledb.Attr(name="pdop", dtype=np.float64),
    tiledb.Attr(name="wrms", dtype=np.float64),
]
KinPositionArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain),
    attrs=KinPositionAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

IMUPositionAttributes = [
    attribute_dict["azimuth"],
    attribute_dict["pitch"],
    attribute_dict["roll"],
    attribute_dict["latitude"],
    attribute_dict["longitude"],
    attribute_dict["height"],
    attribute_dict["latitude_std"],
    attribute_dict["longitude_std"],
    attribute_dict["height_std"],
    attribute_dict["northVelocity"],
    attribute_dict["eastVelocity"],
    attribute_dict["upVelocity"],
    attribute_dict["northVelocity_std"],
    attribute_dict["eastVelocity_std"],
    attribute_dict["upVelocity_std"],
    attribute_dict["roll_std"],
    attribute_dict["pitch_std"],
    attribute_dict["azimuth_std"],
]
IMUPositionArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain),
    attrs=IMUPositionAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

ShotDataAttributes = [
    tiledb.Attr(name="head0", dtype=np.float64),
    tiledb.Attr(name="pitch0", dtype=np.float64),
    tiledb.Attr(name="roll0", dtype=np.float64),
    tiledb.Attr(name="head1", dtype=np.float64),
    tiledb.Attr(name="pitch1", dtype=np.float64),
    tiledb.Attr(name="roll1", dtype=np.float64),
    tiledb.Attr(name="east0", dtype=np.float64),
    tiledb.Attr(name="north0", dtype=np.float64),
    tiledb.Attr(name="up0", dtype=np.float64),
    tiledb.Attr(name="east1", dtype=np.float64),
    tiledb.Attr(name="north1", dtype=np.float64),
    tiledb.Attr(name="up1", dtype=np.float64),
    attribute_dict["east_std0"],
    attribute_dict["north_std0"],
    attribute_dict["up_std0"],
    attribute_dict["east_std1"],
    attribute_dict["north_std1"],
    attribute_dict["up_std1"],
    attribute_dict["returnTime"],
    attribute_dict["tt"],
    attribute_dict["dbv"],
    attribute_dict["xc"],
    attribute_dict["snr"],
    attribute_dict["tat"],
    attribute_dict["isUpdated"],
]

ShotDataArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(tiledb.Dim(name="pingTime", dtype="datetime64[ns]"), TransponderDomain),
    attrs=ShotDataAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

AcousticDataAttributes = [
    attribute_dict["returnTime"],
    attribute_dict["tt"],
    attribute_dict["dbv"],
    attribute_dict["xc"],
    attribute_dict["snr"],
    attribute_dict["tat"],
]

AcousticArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(TimeDomain, TransponderDomain),
    attrs=AcousticDataAttributes,
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)

# S3 / TileDB context configuration
config = tiledb.Config()
config["vfs.s3.region"] = "us-east-2"
config["vfs.s3.scheme"] = "https"
config["vfs.s3.endpoint_override"] = ""
config["vfs.s3.use_virtual_addressing"] = "true"

aws_profile = os.environ.get("AWS_PROFILE", "")
if aws_profile:
    config["vfs.s3.aws_profile"] = aws_profile
else:
    config["vfs.s3.aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID", "")
    config["vfs.s3.aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    config["vfs.s3.session_token"] = os.environ.get("AWS_SESSION_TOKEN", "")

ctx = tiledb.Ctx(config=config)

filters1 = tiledb.FilterList([tiledb.ZstdFilter(level=7)])
filters2 = tiledb.FilterList([tiledb.ByteShuffleFilter(), tiledb.ZstdFilter(level=7)])
filters3 = tiledb.FilterList([tiledb.BitWidthReductionFilter(), tiledb.ZstdFilter(level=7)])
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
for _, tile_value in roll_periods.items():
    d0 = tiledb.Dim(
        name="time",
        domain=(315964800000, 4102444800000),
        tile=tile_value,
        dtype=np.int64,
        filters=filters1,
    )
    d1 = tiledb.Dim(name="sys", domain=(0, 254), tile=1, dtype=np.uint8, filters=filters1)
    d2 = tiledb.Dim(name="sat", domain=(0, 254), tile=1, dtype=np.uint8, filters=filters1)
    d3 = tiledb.Dim(name="obs", domain=(0, 65534), tile=1, dtype=np.uint16, filters=filters1)

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
