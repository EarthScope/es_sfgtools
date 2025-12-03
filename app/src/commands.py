"""
This module contains the core logic for executing pipeline commands.

It orchestrates the data handling and processing workflows based on the
parsed manifest file.
"""
from es_sfgtools.data_mgmt.ingestion.archive_pull import list_campaign_files
from es_sfgtools.modeling.garpos_tools.load_utils import load_lib
from es_sfgtools.utils.model_update import validate_and_merge_config
from es_sfgtools.workflows.workflow_handler import WorkflowHandler

from .manifest import GARPOSConfig, PipelineManifest
from .utils import display_pipelinemanifest


def run_manifest(manifest_object: PipelineManifest):
    """
    Executes a series of data ingestion, download, and processing jobs
    based on the provided PipelineManifest object.

    Args:
        manifest_object: An object containing details about all jobs and
            the main directory for data handling.

    Raises:
        AssertionError: If a directory listed in an ingestion job does not exist.
    """
    display_pipelinemanifest(manifest_object)
    load_lib()
    wfh = WorkflowHandler(manifest_object.main_directory)

    for ingest_job in manifest_object.ingestion_jobs:
        wfh.set_network_station_campaign(
            network_id=ingest_job.network,
            station_id=ingest_job.station,
            campaign_id=ingest_job.campaign,
        )
        assert ingest_job.directory.exists(), "Directory listed does not exist"
        wfh.ingest_add_local_data(ingest_job.directory)

    for job in manifest_object.download_jobs:
        urls = list_campaign_files(**job.model_dump())
        if not urls:
            print(f"No Remote Assets Found For {job.model_dump()}")
        wfh.set_network_station_campaign(
            network_id=job.network,
            station_id=job.station,
            campaign_id=job.campaign,
        )
        wfh.ingest_catalog_archive_data(remote_filepaths=urls)
        wfh.ingest_download_archive_data()

    for job in manifest_object.process_jobs:
        wfh.set_network_station_campaign(
            network_id=job.network, station_id=job.station, campaign_id=job.campaign
        )
        wfh.preprocess_run_pipeline_sv3(
            job=job.job_type,
            primary_config=job.global_config,
            secondary_config=job.secondary_config,
        )

    for job in manifest_object.garpos_jobs:
        config:GARPOSConfig = validate_and_merge_config(
            base_class=job.global_config,
            override_config=job.secondary_config,
        )
        wfh.set_network_station_campaign(
            network_id=job.network, station_id=job.station, campaign_id=job.campaign
        )
        wfh.midprocess_prep_garpos(
            custom_filters=(
                config.filter_config.model_dump() if config.filter_config else None
            ),
            override=config.override,
            override_survey_parsing=False,
            survey_id=None,
            write_intermediate=False,
        )

        surveys = (
            job.surveys
            if job.surveys
            else [None]
        )

        for survey_id in surveys:
            wfh.modeling_run_garpos(
                iterations=config.iterations,
                run_id=config.run_id,
                override=config.override,
                survey_id=survey_id,
                custom_settings=config.inversion_params,
            )


def run_preprocessing(
    network_id: str, campaign_id: str, stations: list, main_dir: str
):
    """
    Initializes and runs the preprocessing workflow for a set of stations.

    Args:
        network_id: The network identifier.
        campaign_id: The campaign identifier.
        stations: A list of station identifiers.
        main_dir: The main project directory.
    """
    wfh = WorkflowHandler(main_dir)
    for station_id in stations:
        wfh.set_network_station_campaign(
            network_id=network_id,
            station_id=station_id,
            campaign_id=campaign_id,
        )
        wfh.preprocess_run_pipeline_sv3(job="all")
