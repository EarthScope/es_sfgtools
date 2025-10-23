from es_sfgtools.data_mgmt.data_handler import DataHandler
from es_sfgtools.modeling.garpos_tools.load_utils import load_lib
from es_sfgtools.data_mgmt.ingestion.archive_pull import list_campaign_files, load_site_metadata
from es_sfgtools.workflows.workflow_handler import WorkflowHandler


from .manifest import PipelineManifest
from .utils import display_pipelinemanifest

def run_manifest(manifest_object: PipelineManifest):
    """Executes a series of data ingestion, download, and processing jobs.

    This is based on the provided PipelineManifest object.

    Parameters
    ----------
    manifest_object : PipelineManifest
        An object containing details about ingestion jobs, download jobs, and
        process jobs, as well as the main directory for data handling.

    Raises
    ------
    AssertionError
        If a directory listed in an ingestion job does not exist.
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
            job='all',
            primary_config=job.config,
        )
    

    for job in manifest_object.garpos_jobs:
        wfh.set_network_station_campaign(
            network_id=job.network, station_id=job.station, campaign_id=job.campaign
        )
        wfh.midprocess_parse_surveys(override=False)
        garpos_handler = wfh.modeling_get_garpos_handler()
        
        surveys = job.surveys if job.surveys else [x.id for x in garpos_handler.current_campaign.surveys]
        for survey_id in surveys:
            garpos_handler.run_garpos(
                run_id=job.config.run_id,
                override=job.config.override,
                campaign_id=job.campaign,
                survey_id=survey_id,
                custom_settings=job.config,
            )