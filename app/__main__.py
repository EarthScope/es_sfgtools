import os
from pathlib import Path
import typer
import sys
sys.path.append(str(Path(__file__).parent))
from src.manifest import PipelineManifest
from es_sfgtools.data_mgmt.data_handler import DataHandler
from es_sfgtools.utils.archive_pull import list_campaign_files
from es_sfgtools.logging import ProcessLogger
from src.commands import run_manifest
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

ProcessLogger.route_to_console()

app = typer.Typer()


@app.command()
def run(file: Path):
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
