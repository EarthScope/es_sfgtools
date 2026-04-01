"""
Re-export TileDB schemas and array classes from the earthscope-sfg package.

All schema definitions, array wrapper classes, and helpers are maintained in
``es_sfgtools.tiledb_schemas`` (the earthscope-sfg package).  This module
re-exports them so that existing ``es_sfgtools.tiledb_tools.tiledb_schemas``
imports within the workflows package continue to work unchanged.
"""

from es_sfgtools.tiledb_schemas import TILEDB_AVAILABLE  # noqa: F401 -- re-export

if TILEDB_AVAILABLE:
    from es_sfgtools.tiledb_schemas import (  # noqa: F401 -- re-exports
        AcousticArraySchema,
        GNSSObsSchema,
        IMUPositionArraySchema,
        KinPositionArraySchema,
        ShotDataArraySchema,
        TBDArray,
        TDBAcousticArray,
        TDBGNSSObsArray,
        TDBIMUPositionArray,
        TDBKinPositionArray,
        TDBShotDataArray,
        config,
        ctx,
        filters,
    )

# Some callers also import DataFrame schemas from this module; re-export them
# so the import path stays compatible.
from es_sfgtools.data_models.observables import (  # noqa: F401 -- re-exports
    AcousticDataFrame,
    IMUPositionDataFrame,
    KinPositionDataFrame,
    ShotDataFrame,
)

_TILEDB_NAMES = [
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

__all__ = [
    "TILEDB_AVAILABLE",
    "AcousticDataFrame",
    "IMUPositionDataFrame",
    "KinPositionDataFrame",
    "ShotDataFrame",
] + _TILEDB_NAMES
