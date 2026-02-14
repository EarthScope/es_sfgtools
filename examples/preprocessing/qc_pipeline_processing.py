"""Example script for processing QC PIN data from Sonardyne equipment.

This demonstrates how to use the WorkflowHandler to run the QC processing 
pipeline on QC PIN files, which contain embedded GNSS observations (RANGEA)
and acoustic ranging data.

The QC pipeline processes data through these stages:
1. Extract shotdata and GNSS epochs from QC PIN files
2. Generate RINEX files from GNSS observations
3. Run PRIDE-PPP for precise positioning
4. Convert kinematic files to dataframes
5. Refine shotdata with high-precision positions
"""

from pathlib import Path
from es_sfgtools.workflows import WorkflowHandler

if __name__ == "__main__":
    # Step 1: Define project parameters
    NETWORK = "cascadia-gorda"
    STATION = "NCC1"
    CAMPAIGN = "2025_A_1234"
    PROJECT_DIRECTORY = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/TestQC")

    # Step 2: Initialize the WorkflowHandler
    workflow = WorkflowHandler(directory=PROJECT_DIRECTORY)

    # Step 3: Set the network, station, and campaign context
    workflow.set_network_station_campaign(
        network_id=NETWORK,
        station_id=STATION,
        campaign_id=CAMPAIGN,
    )

    # Step 4 (Optional): Ingest QC PIN files if not already in the catalog
    qcpin_directory = "/Volumes/DunbarSSD/Project/SeafloorGeodesy/Misc/20250812"
    # Uncomment the following line to ingest local QC PIN files:
    workflow.ingest_add_local_data(directory_path=qcpin_directory)

    # Step 5: Run the QC processing pipeline (Option A: full pipeline)
    workflow.preprocess_run_pipeline_qc(job="all")

    # Step 6 (Optional): Custom configuration (example shown in comments)

    # Step 7 (Optional): Get pipeline instance for advanced usage (example shown in comments)

    print(f"{'='*60}")
    print(f"QC Pipeline processing complete for {NETWORK}/{STATION}/{CAMPAIGN}")
    print(f"{'='*60}")
