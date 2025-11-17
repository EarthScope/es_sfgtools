"""
This module is the command-line entry point for the application.

It uses Typer to create a CLI for running and preprocessing data pipelines
based on manifest files.
"""
import os
import sys
from pathlib import Path
from typing import List
import typer
import multiprocessing
try:
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    # This will fail if the context has already been set, which is fine.
    pass

from es_sfgtools.logging import ProcessLogger
from es_sfgtools.config.env_config import Environment
Environment.load_working_environment()


# This is a temporary workaround for the import system.
# A better long-term solution is to install the package in editable mode.
sys.path.append(str(Path(__file__).parent))
from src.commands import run_manifest, run_preprocessing
from src.manifest import PipelineManifest

# This adds the PRIDE binary path to the system's PATH.
# A better long-term solution is for the user to configure this in their shell.
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

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
    Environment.load_aws_credentials()
    run_manifest(manifest_object)


@app.command()
def preprocess(
    main_dir: Path = typer.Option(..., help="Main directory for the workflow"),
    network: str = typer.Option(..., help="Network ID"),
    campaign: str = typer.Option(..., help="Campaign ID"),
    stations: List[str] = typer.Option(..., help="List of station IDs"),
):
    """
    Runs the preprocessing pipeline for a given network, campaign, and stations.

    Args:
        main_dir: The main directory where data and results will be stored.
        network: The identifier for the network.
        campaign: The identifier for the campaign.
        stations: A list of station identifiers to be processed.
    """
    run_preprocessing(
        network_id=network,
        campaign_id=campaign,
        stations=stations,
        main_dir=str(main_dir),
    )


if __name__ == "__main__":
    app()
