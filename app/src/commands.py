from pathlib import Path
from typing import List, Optional
import os
from es_sfgtools.processing.pipeline.data_handler import DataHandler
from es_sfgtools.utils.archive_pull import list_campaign_files
from .manifest import PipelineManifest
from .utils import display_pipelinemanifest

def run_manifest(manifest_object: PipelineManifest):
    """
    Executes a series of data ingestion, download, and processing jobs based on the 
    provided PipelineManifest object.

    Args:
        manifest_object (PipelineManifest): An object containing details about 
            ingestion jobs, download jobs, and process jobs, as well as the main 
            directory for data handling.

    Workflow:
        1. Displays the details of the provided PipelineManifest object.
        2. Handles ingestion jobs:
            - Changes the working station based on the job's network, station, 
              and campaign.
            - Verifies the existence of the specified directory.
            - Discovers and adds files from the directory.
        3. Handles download jobs:
            - Retrieves remote file URLs for the specified campaign.
            - Changes the working station and adds remote file paths.
            - Downloads the data from the remote sources.
        4. Handles process jobs:
            - Changes the working station based on the job's network, station, 
              and campaign.
            - Retrieves the pipeline and configuration for processing.
            - Updates the pipeline configuration and runs the processing pipeline.

    Raises:
        AssertionError: If a directory listed in an ingestion job does not exist.
    """

    display_pipelinemanifest(manifest_object)
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
        urls = list_campaign_files(**job.model_dump())
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
