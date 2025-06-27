import sys
from pathlib import Path
import os
sys.path.append((test:=str(Path(__file__).parent.parent)))

from pathlib import Path
os.environ["GARPOS_PATH"] = str(Path("/Users/franklyndunbar/Project/garpos").resolve())


if __name__ == "__main__":
    from src import run_manifest, PipelineManifest
    pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
    os.environ["PATH"] += os.pathsep + str(pride_path)

    manifest_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/app/tests/pre-proc-manifest.json"
    )
   
    run_manifest(
        manifest_object=PipelineManifest.from_json(manifest_path)
    )
