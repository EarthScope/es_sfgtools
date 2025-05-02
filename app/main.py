import os
from pathlib import Path
import typer
from .src.manifest import PipelineManifest
from es_sfgtools.processing.pipeline.data_handler import DataHandler
from es_sfgtools.utils.archive_pull import list_campaign_files
from es_sfgtools.utils.loggers import ProcessLogger
from .src.commands import run_manifest
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

ProcessLogger.route_to_console()

app = typer.Typer()


@app.command()
def run(file: Path):
    manifest_object = PipelineManifest.from_json(file)
    run_manifest(manifest_object)
   


if __name__ == "__main__":

    app()
