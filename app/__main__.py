"""
This module is the entry point for the application.
"""
import os
import sys
from pathlib import Path

import typer

sys.path.append(str(Path(__file__).parent))
from src.commands import run_manifest
from src.manifest import PipelineManifest

from es_sfgtools.logging import ProcessLogger

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

ProcessLogger.route_to_console()

app = typer.Typer()


@app.command()
def run(file: Path):
    """
    Runs the pipeline from a manifest file.

    :param file: The path to the manifest file.
    :type file: Path
    :raises ValueError: If the file type is not supported.
    """
    match file.suffix:
        case ".json":
            manifest_object = PipelineManifest.from_json(file)
        case ".yaml" | ".yml":
            manifest_object = PipelineManifest.from_yaml(file)
        case _:
            raise ValueError(f"Unsupported file type: {file.suffix}")
    run_manifest(manifest_object)


if __name__ == "__main__":

    app()
