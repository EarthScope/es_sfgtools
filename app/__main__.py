"""
This module is the entry point for the application.
"""
import os
import sys
from pathlib import Path
from typing import List

import typer

sys.path.append(str(Path(__file__).parent))
from src.commands import run_manifest, run_preprocessing
from src.manifest import PipelineManifest

from es_sfgtools.logging import ProcessLogger

pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

ProcessLogger.route_to_console()

app = typer.Typer()


@app.command()
def run(file: Path):
    """Runs the pipeline from a manifest file.

    Parameters
    ----------
    file : Path
        The path to the manifest file.

    Raises
    ------
    ValueError
        If the file type is not supported.
    """
    match file.suffix:
        case ".json":
            manifest_object = PipelineManifest.from_json(file)
        case ".yaml" | ".yml":
            manifest_object = PipelineManifest.from_yaml(file)
        case _:
            raise ValueError(f"Unsupported file type: {file.suffix}")
    run_manifest(manifest_object)


@app.command()
def preprocess(
    main_dir: Path = typer.Option(..., help="Main directory for the workflow"),
    network: str = typer.Option(..., help="Network ID"),
    campaign: str = typer.Option(..., help="Campaign ID"),
    stations: List[str] = typer.Option(..., help="List of station IDs"),
):
    """Runs the preprocessing pipeline for a given network, campaign, and stations."""
    run_preprocessing(
        network_id=network,
        campaign_id=campaign,
        stations=stations,
        main_dir=str(main_dir),
    )


if __name__ == "__main__":

    app()