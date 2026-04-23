"""
This module is for development and testing of the CLI.
"""

import os
from pathlib import Path

if __name__ == "__main__":
    from earthscope_sfg.logging import ProcessLogger
    from earthscope_sfg_cli.commands import run_manifest
    from earthscope_sfg_cli.manifest import PipelineManifest
    from earthscope_sfg_workflows.config.env_config import Environment

    Environment.load_working_environment()
    ProcessLogger.route_to_console()

    pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
    os.environ["PATH"] += os.pathsep + str(pride_path)

    manifest_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/NCC1-preproc-manifest.json"
    )

    run_manifest(manifest_object=PipelineManifest.load(manifest_path))
