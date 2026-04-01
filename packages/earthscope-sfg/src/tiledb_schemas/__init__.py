"""
TileDB array schemas and base classes for seafloor geodesy data.

Requires the ``tiledb`` optional extra::

    pip install earthscope-sfg[tiledb]
"""

try:
    import tiledb  # noqa: F401
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "The tiledb_schemas module requires TileDB. "
        "Install it with: pip install earthscope-sfg[tiledb]"
    ) from exc

from es_sfgtools.tiledb_schemas.schemas import (
    AcousticArraySchema,
    GNSSObsSchema,
    IMUPositionArraySchema,
    KinPositionArraySchema,
    ShotDataArraySchema,
    config,
    ctx,
    filters,
)
from es_sfgtools.tiledb_schemas.arrays import (
    TBDArray,
    TDBAcousticArray,
    TDBGNSSObsArray,
    TDBIMUPositionArray,
    TDBKinPositionArray,
    TDBShotDataArray,
)

__all__ = [
    "AcousticArraySchema",
    "GNSSObsSchema",
    "IMUPositionArraySchema",
    "KinPositionArraySchema",
    "ShotDataArraySchema",
    "TBDArray",
    "TDBAcousticArray",
    "TDBGNSSObsArray",
    "TDBIMUPositionArray",
    "TDBKinPositionArray",
    "TDBShotDataArray",
    "config",
    "ctx",
    "filters",
]
