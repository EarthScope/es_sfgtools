"""
Example: Running the QC Processing Pipeline

This example demonstrates how to use the WorkflowHandler to run
the Quality Control (QC) processing pipeline and GARPOS modeling.

The QC pipeline processes .pin (QC JSON) files containing acoustic
interrogation and range data, converts them to ShotDataFrames,
and runs GARPOS inversion to compute seafloor positions.

Requirements:
- QC .pin files must be ingested into the campaign directory first
- Site metadata should be available for the station
- GARPOS must be installed and configured

Steps:
1. Initialize the WorkflowHandler with a working directory
2. Set the network, station, and campaign context
3. Ingest QC data files from a local directory
4. Run the QC processing and modeling pipeline
"""

from pathlib import Path
from es_sfgtools.workflows.workflow_handler import WorkflowHandler


def main():
    # =========================================================================
    # Configuration
    # =========================================================================
    
    # Main working directory for all data operations
    main_dir = Path("/path/to/your/data/directory")
    
    # Network, station, and campaign identifiers
    NETWORK = "cascadia-gorda"
    STATION = "NTH1"
    CAMPAIGN = "2025_A_1126"
    
    # Directory containing raw QC .pin files to ingest
    raw_qc_data_dir = Path("/path/to/raw/qc/data")
    
    # =========================================================================
    # Initialize Workflow
    # =========================================================================
    
    # Create the workflow handler
    workflow = WorkflowHandler(directory=main_dir)
    
    # Set the processing context (network/station/campaign)
    workflow.set_network_station_campaign(
        network_id=NETWORK,
        station_id=STATION,
        campaign_id=CAMPAIGN,
    )
    
    # =========================================================================
    # Ingest QC Data (if not already done)
    # =========================================================================
    
    # Ingest raw QC files from local directory
    # This step adds .pin files to the asset catalog
    workflow.ingest_add_local_data(directory_path=raw_qc_data_dir)
    
    # =========================================================================
    # Run QC Processing and GARPOS Modeling
    # =========================================================================
    
    # Basic usage - process QC files and run GARPOS with default settings
    workflow.qc_process_and_model(
        run_id="qc_run_001",  # Identifier for this GARPOS run
        iterations=1,         # Number of GARPOS iterations
    )
    
    print("QC processing complete!")


def main_with_custom_settings():
    """Example with custom GARPOS settings and configuration."""
    
    main_dir = Path("/path/to/your/data/directory")
    
    NETWORK = "cascadia-gorda"
    STATION = "NTH1"
    CAMPAIGN = "2025_A_1126"
    
    workflow = WorkflowHandler(directory=main_dir)
    workflow.set_network_station_campaign(
        network_id=NETWORK,
        station_id=STATION,
        campaign_id=CAMPAIGN,
    )
    
    # Custom GARPOS inversion parameters
    garpos_settings = {
        "log_lambda": [-2.0, -1.0],    # Hyperparameter search range
        "mu_t": [0.5, 1.0],            # Temporal correlation
        "mu_mt": 0.5,                  # Gradient correlation
    }
    
    # Pre-processing configuration options
    pre_process_config = {
        "override": False,  # Set True to reprocess already-processed files
    }
    
    # Run with custom settings
    workflow.qc_process_and_model(
        run_id="qc_custom_run",
        iterations=2,
        garpos_settings=garpos_settings,
        garpos_override=False,          # Set True to re-run existing results
        pre_process_config=pre_process_config,
    )
    
    # Plot the results
    workflow.modeling_plot_garpos_results(
        run_id="qc_custom_run",
        residuals_filter=10,  # Filter outliers > 10m
        save_fig=True,
        show_fig=False,
    )
    
    print("QC processing with custom settings complete!")



if __name__ == "__main__":
    # Run the basic example
    main()
    
    # Or run with custom settings
    # main_with_custom_settings()
    
    # Or run batch processing
    # main_batch_stations()
