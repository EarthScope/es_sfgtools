from es_sfgtools.processing.pipeline.pipelines import SV3PipelineConfig,PipelineManifest,SV3Pipeline
from es_sfgtools.utils.archive_pull import list_survey_files
from pathlib import Path
from es_sfgtools.processing.pipeline.data_handler import DataHandler
import os
pride_path = Path.home() / ".PRIDE_PPPAR_BIN"
os.environ["PATH"] += os.pathsep + str(pride_path)

if __name__ == "__main__":

    # main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")

    # dh = DataHandler(main_dir)

    # network = "cascadia-gorda"
    # station = "NCC1"
    # campaign = "2024_A_1126"

    # dh.change_working_station(network=network, station=station, campaign=campaign)

    # pipeline, config = dh.get_pipeline_sv3()
    # # Example usage of the SV3PipelineConfig

    # yaml_path = Path(__file__).parent / "sv3_pipeline_config.yaml"
    # config.to_yaml(yaml_path)

    manifest_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/app/pre-proc-manifest.yaml"
    )

    manifest_object = PipelineManifest.from_yaml(manifest_path)

    dh = DataHandler(manifest_object.main_dir)
    for ingest_job in manifest_object.ingestion_jobs:
        dh.change_working_station(network=ingest_job.network,station=ingest_job.station,campaign=ingest_job.campaign)
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
        dh.change_working_station(network=job.network,station=job.station,campaign=job.campaign)
        pipeline,config = dh.get_pipeline_sv3()
        job.config.rinex_config.settings_path = dh.rinex_metav2
        pipeline.config = job.config
        pipeline.run_pipeline()
