from es_sfgtools.data_mgmt.data_handler import DataHandler
from es_sfgtools.modeling.garpos_tools.load_utils import load_lib
from es_sfgtools.data_mgmt.ingestion.archive_pull import list_campaign_files, load_site_metadata

from .manifest import PipelineManifest
from .utils import display_pipelinemanifest


def run_manifest(manifest_object: PipelineManifest):
    """
    Executes a series of data ingestion, download, and processing jobs based on the 
    provided PipelineManifest object.

    :param manifest_object: An object containing details about 
            ingestion jobs, download jobs, and process jobs, as well as the main 
            directory for data handling.
    :type manifest_object: PipelineManifest
    :raises AssertionError: If a directory listed in an ingestion job does not exist.
    """

    display_pipelinemanifest(manifest_object)
    load_lib()
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
        job.config.rinex_config.settings_path = pipeline.config.rinex_config.settings_path
        pipeline.config = job.config
        pipeline.run_pipeline()

    for job in manifest_object.garpos_jobs:
        dh.change_working_station(
            network=job.network, station=job.station, campaign=job.campaign
        )
        site = load_site_metadata(network=job.network, station=job.station)
        garpos_handler = dh.get_garpos_handler(site_data=site)
        garpos_handler.set_campaign(job.campaign)
        garpos_handler.prep_shotdata(job.config.override)
        garpos_handler.load_sound_speed_data()
        garpos_handler.set_inversion_params(job.config.inversion_params)
        surveys = job.surveys if job.surveys else [x.id for x in garpos_handler.current_campaign.surveys]
        for survey_id in surveys:
            garpos_handler.run_garpos(
                run_id=job.config.run_id,
                override=job.config.override,
                campaign_id=job.campaign,
                survey_id=survey_id
            )
