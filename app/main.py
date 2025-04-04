import os
from pathlib import Path
import typer 
from es_sfgtools.processing.pipeline.pipelines import (
    PipelineManifest,
)
from es_sfgtools.processing.pipeline.data_handler import DataHandler
from es_sfgtools.utils.archive_pull import list_survey_files
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

app = typer.Typer()

@app.command()
def run(file:Path):
    manifest_object = PipelineManifest.from_yaml(file)

    dh = DataHandler(manifest_object.main_dir)
    for ingest_job in manifest_object.ingestion_jobs:
        dh.change_working_station(
            network=ingest_job.network,
            station=ingest_job.station,
            campaign=ingest_job.campaign,
        )
        assert ingest_job.directory.exists(), "Directory listed does not exist"
        dh.discover_data_and_add_files(ingest_job.directory)

    for job in manifest_object.download_jobs:
        urls = list_survey_files(**job.model_dump())
        if not urls:
            print(f"No Remote Assets Found For {job.model_dump()}")
        dh.change_working_station(**job.model_dump())
        dh.add_data_remote(remote_filepaths=urls)
        dh.download_data()

    for job in manifest_object.process_jobs:
        dh.change_working_station(
            network=job.network, station=job.station, campaign=job.campaign
        )
        pipeline, config = dh.get_pipeline_sv3()
        job.config.rinex_config.settings_path = dh.rinex_metav2
        pipeline.config = job.config
        pipeline.run_pipeline()

if __name__ == "__main__":

    app()
