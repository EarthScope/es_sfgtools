from es_sfgtools.processing.pipeline.pipelines import SV3PipelineConfig,PipelineManifest,SV3Pipeline
from pathlib import Path
from es_sfgtools.processing.pipeline.data_handler import DataHandler

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
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/pre-proc-manifest.yaml"
    )

    manifest_object = PipelineManifest.from_yaml(manifest_path)

    dh = DataHandler(manifest_object.main_dir)
    for job in manifest_object.jobs:
        dh.change_working_station(network=job.network,station=job.station,campaign=job.campaign)
        pipeline,config = dh.get_pipeline_sv3()
        job.config.rinex_config.settings_path = dh.rinex_metav2
        pipeline.config = job.config
        pipeline.run_pipeline()


