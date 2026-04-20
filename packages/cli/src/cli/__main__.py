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

from es_sfgtools.cli.commands import run_manifest, run_preprocessing
from es_sfgtools.cli.manifest import PipelineManifest
from es_sfgtools.config.workspace import Workspace, WorkspaceType
from es_sfgtools.logging import ProcessLogger

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
    workspace_type: str = typer.Option(
        "local", help="Workspace type: local, geolab, ecs"
    ),
    s3_bucket: str | None = typer.Option(
        None, help="S3 sync bucket URI (required for geolab/ecs)"
    ),
    aws_profile: str | None = typer.Option(None, help="AWS profile name"),
    aws_access_key_id: str | None = typer.Option(None, envvar="AWS_ACCESS_KEY_ID"),
    aws_secret_access_key: str | None = typer.Option(
        None, envvar="AWS_SECRET_ACCESS_KEY"
    ),
    aws_session_token: str | None = typer.Option(
        None, envvar="AWS_SESSION_TOKEN"
    ),
    pride_dir: Path | None = typer.Option(
        None, help="Path to PRIDE-PPPAR binary directory"
    ),
):
    """Run the preprocessing pipeline for a network, campaign, and set of stations."""
    wtype = WorkspaceType(workspace_type.lower())
    match wtype:
        case WorkspaceType.LOCAL:
            workspace = Workspace.local(
                main_dir,
                pride_binary_dir=pride_dir,
                aws_profile=aws_profile,
                s3_sync_bucket=s3_bucket,
            )
        case WorkspaceType.GEOLAB:
            if not s3_bucket:
                raise typer.BadParameter(
                    "--s3-bucket is required for geolab workspaces"
                )
            workspace = Workspace.geolab(
                main_dir,
                s3_bucket,
                aws_profile=aws_profile,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                pride_binary_dir=pride_dir,
            )
        case WorkspaceType.ECS:
            if not s3_bucket:
                raise typer.BadParameter(
                    "--s3-bucket is required for ecs workspaces"
                )
            workspace = Workspace.ecs(
                main_dir,
                s3_bucket,
                aws_profile=aws_profile,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                pride_binary_dir=pride_dir,
            )

    run_preprocessing(
        workspace=workspace,
        network_id=network,
        campaign_id=campaign,
        stations=stations,
    )


if __name__ == "__main__":
    app()
