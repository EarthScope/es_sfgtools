from pathlib import Path
from typing import List, Optional
import os
from es_sfgtools.processing.pipeline.data_handler import DataHandler
from es_sfgtools.utils.archive_pull import list_campaign_files
from .manifest import PipelineManifest


def run_manifest(manifest_object: PipelineManifest):
  
    dh = DataHandler(manifest_object.main_dir)
    if not manifest_object.ingestion_jobs:
        print("No Manifest Jobs Found")
    for ingest_job in manifest_object.ingestion_jobs:
        dh.change_working_station(
            network=ingest_job.network,
            station=ingest_job.station,
            campaign=ingest_job.campaign,
        )
        assert ingest_job.directory.exists(), "Directory listed does not exist"
        dh.discover_data_and_add_files(ingest_job.directory)
    if not manifest_object.download_jobs:
        print("No Download Jobs Found")
    for job in manifest_object.download_jobs:
        urls = list_campaign_files(**job.model_dump())
        if not urls:
            print(f"No Remote Assets Found For {job.model_dump()}")
        dh.change_working_station(**job.model_dump())
        dh.add_data_remote(remote_filepaths=urls)
        dh.download_data()
    if not manifest_object.process_jobs:
        print("No Process Jobs Found")
    for job in manifest_object.process_jobs:
        dh.change_working_station(
            network=job.network, station=job.station, campaign=job.campaign
        )
        pipeline, config = dh.get_pipeline_sv3()
        job.config.rinex_config.settings_path = dh.rinex_metav2
        pipeline.config = job.config
        pipeline.run_pipeline()
