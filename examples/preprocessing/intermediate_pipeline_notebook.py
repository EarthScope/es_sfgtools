import argparse
import json
import os
from pathlib import Path

from earthscope_sfg_workflows.workflows.workflow_handler import WorkflowHandler


def build_global_config(
    dfop00_override=True,
    novatel_n_processes=5,
    novatel_override=False,
    position_update_override=True,
    position_update_lengthscale=0.1,
    position_update_plot=False,
    pride_cutoff_elevation=7,
    pride_frequency=None,
    pride_system="GREC23J",
    pride_tides="SOP",
    pride_override_products=False,
    pride_override=True,
    rinex_n_processes=5,
    rinex_time_interval=24,
    rinex_override=False,
):
    """Build the primary configuration dictionary for the pipeline."""
    if pride_frequency is None:
        pride_frequency = ["G12", "R12", "E15", "C26", "J12"]

    return {
        "dfop00_config": {"override": dfop00_override},
        "novatel_config": {"n_processes": novatel_n_processes, "override": novatel_override},
        "position_update_config": {
            "override": position_update_override,
            "lengthscale": position_update_lengthscale,
            "plot": position_update_plot,
        },
        "pride_config": {
            "cutoff_elevation": pride_cutoff_elevation,
            "end": None,
            "frequency": pride_frequency,
            "high_ion": None,
            "interval": None,
            "local_pdp3_path": None,
            "loose_edit": True,
            "sample_frequency": 1,
            "start": None,
            "system": pride_system,
            "tides": pride_tides,
            "override_products_download": pride_override_products,
            "override": pride_override,
        },
        "rinex_config": {
            "n_processes": rinex_n_processes,
            "time_interval": rinex_time_interval,
            "override": rinex_override,
        },
    }


def run_intermediate_pipeline(
    data_dir,
    network_id,
    station_id,
    campaign_id,
    job="intermediate",
    primary_config_dict=None,
):
    """
    Run the intermediate processing pipeline.

    Parameters
    ----------
    data_dir : str or Path
        Root data directory for the workflow
    network_id : str
        Network identifier (e.g., "cascadia-gorda")
    station_id : str
        Station identifier (e.g., "NCC1")
    campaign_id : str
        Campaign identifier (e.g., "2025_A_1126")
    job : str, optional
        Job type: "intermediate", "all", etc. Default: "intermediate"
    primary_config_dict : dict, optional
        Primary configuration dictionary. If None, uses defaults from build_global_config()
    """
    data_dir = Path(data_dir)

    # Use default configs if not provided
    if primary_config_dict is None:
        primary_config_dict = build_global_config()

    # Create directories
    os.makedirs(data_dir, exist_ok=True)

    # Initialize workflow
    workflow = WorkflowHandler(directory=data_dir)
    workflow.set_network_station_campaign(
        network_id=network_id,
        station_id=station_id,
        campaign_id=campaign_id,
    )

    # Run the pipeline
    workflow.preprocess_run_pipeline_sv3(
        job=job,
        primary_config=primary_config_dict,
    )


def main():
    """Command-line interface for running the intermediate processing pipeline."""

    parser = argparse.ArgumentParser(
        description="Run intermediate processing pipeline for SV3 seafloor geodesy data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""

    Examples:
    # Run with defaults
    python intermediate_processing.py --data-dir ~/data/sfg_march13 \\
        --network cascadia-gorda --station NCC1 --campaign 2025_A_1126

    # Run with custom config file
    python intermediate_processing.py --data-dir ~/data/sfg_march13 \\
        --network cascadia-gorda --station NCC1 --campaign 2025_A_1126 \\
        --primary-config my_config.json

    # Run with data ingestion
    python intermediate_processing.py --data-dir ~/data/sfg_march13 \\
        --network cascadia-gorda --station NCC1 --campaign 2025_A_1126 \\
        --ingest --verbose
        """,
    )

    # Required arguments
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Root data directory for the workflow",
    )
    parser.add_argument(
        "--network",
        required=True,
        help="Network identifier (e.g., 'cascadia-gorda')",
    )
    parser.add_argument(
        "--station",
        required=True,
        help="Station identifier (e.g., 'NCC1')",
    )
    parser.add_argument(
        "--campaign",
        required=True,
        help="Campaign identifier (e.g., '2025_A_1126')",
    )

    # Optional arguments - Job type
    parser.add_argument(
        "--job",
        default="intermediate",
        choices=[
            "intermediate",
            "all",
            "process_novatel",
            "build_rinex",
            "run_pride",
            "process_kinematic",
            "process_dfop00",
            "refine_shotdata",
            "process_svp",
        ],
        help="Job type to run (default: intermediate)",
    )

    # Optional arguments - Config files
    parser.add_argument(
        "--primary-config",
        type=str,
        default=None,
        help="Path to JSON file with primary configuration. If not provided, uses defaults.",
    )

    # Optional arguments - General
    parser.add_argument(
        "--rinex-1hz", action="store_true", help="Download 1Hz RINEX data (if ingesting)"
    )

    args = parser.parse_args()

    # Load config files or use defaults
    if args.primary_config:
        with open(args.primary_config, "r") as f:
            primary_config = json.load(f)
    else:
        primary_config = build_global_config()

    # Run the pipeline
    run_intermediate_pipeline(
        data_dir=args.data_dir,
        network_id=args.network,
        station_id=args.station,
        campaign_id=args.campaign,
        job=args.job,
        primary_config_dict=primary_config,
    )


if __name__ == "__main__":
    main()
