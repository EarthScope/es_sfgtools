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
    # On macOS, explicitly pre-load the conda/pixi-managed libtiledb.dylib before
    # importing the tiledb extension module.  Without this, macOS dyld may resolve
    # the @rpath/libtiledb.dylib reference to a stale system-installed copy (e.g.
    # /usr/local/lib/libtiledb.dylib) that is missing symbols required by the
    # version of tiledb-py installed in this environment.
    #
    # Derive the env root from sys.executable (immune to CONDA_PREFIX being
    # overridden by conda init in .zshrc when pixi shell spawns an interactive
    # shell):  .../envs/default/bin/python → .../envs/default
    import ctypes
    import os
    import sys

    if sys.platform == "darwin":
        _env_root = os.path.dirname(os.path.dirname(os.path.realpath(sys.executable)))
        _pixi_tiledb_lib = os.path.join(_env_root, "lib", "libtiledb.dylib")
        if os.path.exists(_pixi_tiledb_lib):
            try:
                ctypes.CDLL(_pixi_tiledb_lib, ctypes.RTLD_GLOBAL)
            except OSError:
                pass

    import tiledb  # noqa: F401

    TILEDB_AVAILABLE = True
except (ModuleNotFoundError, ImportError, OSError):
    TILEDB_AVAILABLE = False

if TILEDB_AVAILABLE:
    from .arrays import (
        TBDArray,
        TDBAcousticArray,
        TDBGNSSObsArray,
        TDBIMUPositionArray,
        TDBKinPositionArray,
        TDBShotDataArray,
    )
    from .schemas import (
        AcousticArraySchema,
        GNSSObsSchema,
        IMUPositionArraySchema,
        KinPositionArraySchema,
        ShotDataArraySchema,
        config,
        ctx,
        filters,
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
