"""
This module is the command-line entry point for the application.

It uses Typer to create a CLI for running and preprocessing data pipelines
based on manifest files.
"""

import multiprocessing
from pathlib import Path

import typer

try:
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    # This will fail if the context has already been set, which is fine.
    pass

from earthscope_sfg.logging import ProcessLogger
from earthscope_sfg_cli.commands import run_manifest, run_preprocessing
from earthscope_sfg_cli.manifest import PipelineManifest
# This adds the PRIDE binary path to the system's PATH.
# A better long-term solution is for the user to configure this in their shell.
# pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
# os.environ["PATH"] += os.pathsep + str(pride_path)

ProcessLogger.route_to_console()

app = typer.Typer()


@app.command()
def run(file: Path):
    """
    Runs the entire pipeline from a specified manifest file.

    The file format (JSON or YAML) is determined by the file extension.

    Args:
        file: The path to the manifest file.

    Raises:
        ValueError: If the file extension is not .json, .yaml, or .yml.
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
    main_dir: Path = typer.Option(..., help="Root directory for the workspace"),
    network: str = typer.Option(..., help="Network ID"),
    campaign: str = typer.Option(..., help="Campaign ID"),
    stations: list[str] = typer.Option(..., help="List of station IDs"),
):
    """Run the preprocessing pipeline for a network, campaign, and set of stations."""
    run_preprocessing(
        directory=main_dir,
        network_id=network,
        campaign_id=campaign,
        stations=stations,
    )


if __name__ == "__main__":
    app()
