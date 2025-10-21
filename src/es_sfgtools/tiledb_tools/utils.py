# External imports
from pathlib import Path
import tiledb
from cloudpathlib import S3Path

# Local imports
from ..utils.command_line_utils import (
    GOLANG_BINARY_BUILD_DIR,
    get_system_architecture,
)

TILE2RINEX_BIN_PATH = {
    "darwin_amd64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_darwin_amd64",
    "darwin_arm64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_darwin_arm64",
    "linux_amd64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_linux_amd64",
    "linux_arm64": GOLANG_BINARY_BUILD_DIR / "tdb2rnx_linux_arm64",
}


def get_tile2rinex_binary_path() -> Path:
    """Get the path to the tile2rinex golang binary based on the current platform."""
    system, arch = get_system_architecture()
    binary_path = TILE2RINEX_BIN_PATH.get(f"{system}_{arch}")
    if not binary_path:
        raise FileNotFoundError(f"TILE2RINEX binary not found for {system} {arch}")

    return binary_path


def sync_local_to_s3(local_uri: Path, s3_uri: S3Path):
    """
    Sync a local TileDB array to an S3 bucket destination.

    Parameters
    ----------
    local_uri : Path
        Path to the local TileDB array (e.g., '/data/my_array').
    s3_uri : S3Path
        Destination URI in S3 (e.g., 's3://my-bucket/arrays/my_array').


    Notes
    -----
    - Uses TileDB's built-in object storage consolidation to efficiently copy new fragments.
    - Requires AWS credentials (via environment, profile, or IAM role).
    """
    # Set AWS credentials if profile is provided

    # Validate local path
    if not local_uri.exists():
        raise FileNotFoundError(f"Local path {local_uri} does not exist.")
    
    if not s3_uri.exists():
        s3_uri.mkdir(parents=True, exist_ok=True)
        
    # Initialize TileDB config
    cfg = tiledb.Config()
    ctx = tiledb.Ctx(cfg)

    # Ensure local array is valid
    if not tiledb.object_type(str(local_uri)) == "array":
        raise ValueError(f"Local URI {local_uri} is not a TileDB array.")


    print(f"Uploading TileDB array from {local_uri} → {s3_uri} ...")

    # Use TileDB’s built-in copy to S3
    tiledb.VFS().copy_dir(local_uri, str(s3_uri))
