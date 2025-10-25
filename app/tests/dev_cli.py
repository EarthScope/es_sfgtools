"""
This module is for development and testing of the CLI.
"""
import os
import sys
from pathlib import Path

sys.path.append((test:=str(Path(__file__).parent.parent)))
os.environ["DYLD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib"
# add PRIDE to path
os.environ["PATH"] += os.pathsep + str(Path.home() / ".PRIDE_PPPAR_BIN")
from pathlib import Path

sys.path.append("/Users/franklyndunbar/Project/SeaFloorGeodesy/gnatss/src")

if __name__ == "__main__":
    from src import PipelineManifest, run_manifest
    pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
    os.environ["PATH"] += os.pathsep + str(pride_path)

    manifest_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/NCC1-preproc-manifest.json"
    )

    run_manifest(
        manifest_object=PipelineManifest.from_json(manifest_path)
    )
