from es_sfgtools.processing.pipeline.pipelines import SV3PipelineConfig
from pathlib import Path
from es_sfgtools.processing.pipeline.data_handler import DataHandler

if __name__ == "__main__":

    main_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain")

    dh = DataHandler(main_dir)

    network = "cascadia-gorda"
    station = "NCC1"
    campaign = "2024_A_1126"

    dh.change_working_station(network=network, station=station, campaign=campaign)

    pipeline, config = dh.get_pipeline_sv3()
    # Example usage of the SV3PipelineConfig

    yaml_path = Path(__file__).parent / "sv3_pipeline_config.yaml"
    config.to_yaml(yaml_path)
