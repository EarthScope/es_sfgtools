"""
This module defines the TileDB array schemas and provides a set of classes
for interacting with those arrays.

The schemas are defined for various types of seafloor geodesy data, including
kinematic position data, IMU data, acoustic data, and raw shot data.

The TBDArray class and its subclasses provide a high-level interface for
creating, writing to, and reading from these TileDB arrays, handling both
local and S3 storage.
"""
import datetime
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tiledb
from cloudpathlib import S3Path

from ..data_models.community_standards import (
    SFGDSTFSeafloorAcousticData,
)
from ..logging import ProcessLogger as logger
from .tiledb_schemas import TBDArray

filters = tiledb.FilterList([tiledb.ZstdFilter(7)])
TimeDomain = tiledb.Dim(name="time", dtype="datetime64[ms]")
TransponderDomain = tiledb.Dim(name="transponderID", dtype="ascii")
attribute_dict: Dict[str, tiledb.Attr] = {
    "TravelTime": tiledb.Attr(name="TravelTime", dtype=np.float64),
    "X_transmit": tiledb.Attr(name="X_transmit", dtype=np.float64),
    "Y_transmit": tiledb.Attr(name="Y_transmit", dtype=np.float64),
    "Z_transmit": tiledb.Attr(name="Z_transmit", dtype=np.float64),
    "T_receive": tiledb.Attr(name="T_receive", dtype=np.float64),
    "X_receive": tiledb.Attr(name="X_receive", dtype=np.float64),
    "Y_receive": tiledb.Attr(name="Y_receive", dtype=np.float64),
    "Z_receive": tiledb.Attr(name="Z_receive", dtype=np.float64),
    "TDC_ID": tiledb.Attr(name="TDC_ID", dtype="ascii", nullable=True),
    "aSNR": tiledb.Attr(name="aSNR", dtype=np.float64, nullable=True),
    "acc": tiledb.Attr(name="acc", dtype=np.int64, nullable=True),
    "dbV": tiledb.Attr(name="dbV", dtype=np.float64, nullable=True),
    "quality_flag": tiledb.Attr(name="quality_flag", dtype="ascii", nullable=True),
    "trans_sigX0": tiledb.Attr(name="trans_sigX0", dtype=np.float64, nullable=True),
    "trans_sigY0": tiledb.Attr(name="trans_sigY0", dtype=np.float64, nullable=True),
    "trans_sigZ0": tiledb.Attr(name="trans_sigZ0", dtype=np.float64, nullable=True),
    "trans_sigX1": tiledb.Attr(name="trans_sigX1", dtype=np.float64, nullable=True),
    "trans_sigY1": tiledb.Attr(name="trans_sigY1", dtype=np.float64, nullable=True),
    "trans_sigZ1": tiledb.Attr(name="trans_sigZ1", dtype=np.float64, nullable=True),
    "ant_X0": tiledb.Attr(name="ant_X0", dtype=np.float64, nullable=True),
    "ant_Y0": tiledb.Attr(name="ant_Y0", dtype=np.float64, nullable=True),
    "ant_Z0": tiledb.Attr(name="ant_Z0", dtype=np.float64, nullable=True),
    "ant_sigX0": tiledb.Attr(name="ant_sigX0", dtype=np.float64, nullable=True),
    "ant_sigY0": tiledb.Attr(name="ant_sigY0", dtype=np.float64, nullable=True),
    "ant_sigZ0": tiledb.Attr(name="ant_sigZ0", dtype=np.float64, nullable=True),
    "ant_cov_XY0": tiledb.Attr(name="ant_cov_XY0", dtype=np.float64, nullable=True),
    "ant_X1": tiledb.Attr(name="ant_X1", dtype=np.float64, nullable=True),
    "ant_Y1": tiledb.Attr(name="ant_Y1", dtype=np.float64, nullable=True),
    "ant_Z1": tiledb.Attr(name="ant_Z1", dtype=np.float64, nullable=True),
    "ant_sigX1": tiledb.Attr(name="ant_sigX1", dtype=np.float64, nullable=True),
    "ant_sigY1": tiledb.Attr(name="ant_sigY1", dtype=np.float64, nullable=True),
    "ant_sigZ1": tiledb.Attr(name="ant_sigZ1", dtype=np.float64, nullable=True),
    "heading0": tiledb.Attr(name="heading0", dtype=np.float64, nullable=True),
    "pitch0": tiledb.Attr(name="pitch0", dtype=np.float64, nullable=True),
    "roll0": tiledb.Attr(name="roll0", dtype=np.float64, nullable=True),
    "roll1": tiledb.Attr(name="roll1", dtype=np.float64, nullable=True),
    "doa_R": tiledb.Attr(name="doa_R", dtype=np.float64, nullable=True),
    "doa_P": tiledb.Attr(name="doa_P", dtype=np.float64, nullable=True),
    "doa_H": tiledb.Attr(name="doa_H", dtype=np.float64, nullable=True),
}


SFGDSTFSeafloorAcousticDataArraySchema = tiledb.ArraySchema(
    sparse=True,
    domain=tiledb.Domain(
        tiledb.Dim(name="T_transmit", dtype="datetime64[ns]"),
        tiledb.Dim(name="MT_ID", dtype="ascii"),
    ),
    attrs=list(attribute_dict.values()),
    cell_order="col-major",
    tile_order="row-major",
    allows_duplicates=False,
    coords_filters=filters,
)


class TDBSFGDSTFSeafloorAcousticDataArray(TBDArray):
    """Handles TileDB storage for SFGDSTF seafloor acoustic data."""

    dataframe_schema = SFGDSTFSeafloorAcousticData
    array_schema = SFGDSTFSeafloorAcousticDataArraySchema
    name = "SFGDSTF Seafloor Acoustic Data"

    def __init__(self, uri: Path | S3Path | str):
        super().__init__(uri)

    def get_unique_dates(self, field="T_transmit") -> np.ndarray:
        """Gets unique dates from the 'T_transmit' field."""
        return super().get_unique_dates(field)
