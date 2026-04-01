"""
TileDB array schemas and base classes for seafloor geodesy data.

Requires the ``tiledb`` optional extra::

    pip install earthscope-sfg[tiledb]

Use :data:`TILEDB_AVAILABLE` to check at runtime without triggering an
import error::

    from es_sfgtools.tiledb_schemas import TILEDB_AVAILABLE
    if TILEDB_AVAILABLE:
        from es_sfgtools.tiledb_schemas import TDBShotDataArray
"""

try:
    import tiledb  # noqa: F401

    TILEDB_AVAILABLE = True
except ModuleNotFoundError:
    TILEDB_AVAILABLE = False

if TILEDB_AVAILABLE:
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
        "TILEDB_AVAILABLE",
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
else:
    __all__ = ["TILEDB_AVAILABLE"]

    def __getattr__(name: str):
        """Raise a helpful error when accessing TileDB symbols without the extra."""
        raise ModuleNotFoundError(
            f"Cannot access '{name}' because TileDB is not installed. "
            "Install it with: pip install earthscope-sfg[tiledb]"
        )
