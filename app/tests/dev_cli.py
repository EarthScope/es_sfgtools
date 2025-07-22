import sys
from pathlib import Path
import os
sys.path.append((test:=str(Path(__file__).parent.parent)))

from pathlib import Path


if __name__ == "__main__":
    from src import run_manifest, PipelineManifest
    pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
    os.environ["PATH"] += os.pathsep + str(pride_path)

    manifest_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/SeaTrial_20250616_10Hz_PRIDE/NGH1-processing.json"
    )

    run_manifest(
        manifest_object=PipelineManifest.from_json(manifest_path)
    )
