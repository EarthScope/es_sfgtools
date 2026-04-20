"""
This module is for development and testing of the CLI.
"""

import os
from pathlib import Path

if __name__ == "__main__":
    from es_sfgtools.cli.commands import run_manifest
    from es_sfgtools.cli.manifest import PipelineManifest
    from es_sfgtools.config.env_config import Environment
    from es_sfgtools.logging import ProcessLogger

    Environment.load_working_environment()
    ProcessLogger.route_to_console()

    pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
    os.environ["PATH"] += os.pathsep + str(pride_path)

    manifest_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/NCC1-preproc-manifest.json"
    )

    run_manifest(manifest_object=PipelineManifest.load(manifest_path))
