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


'''
Step 1: Define project parameters.

Set the network, station, and campaign identifiers for your data.
The PROJECT_DIRECTORY should point to your main data directory.
'''
NETWORK = "cascadia-gorda"
STATION = "NCC1"
CAMPAIGN = "2025_A_1234"
PROJECT_DIRECTORY = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/TestQC")


'''
Step 2: Initialize the WorkflowHandler.

The WorkflowHandler manages the directory structure, asset catalog,
and provides access to all processing pipelines.
'''
workflow = WorkflowHandler(directory=PROJECT_DIRECTORY)


'''
Step 3: Set the network, station, and campaign context.

This configures the workflow to operate on the specified data context
and initializes the necessary directory structures.
'''
workflow.set_network_station_campaign(
    network_id=NETWORK,
    station_id=STATION,
    campaign_id=CAMPAIGN,
)


'''
Step 4 (Optional): Ingest QC PIN files if not already in the catalog.

If your QC PIN files are in a local directory and haven't been added
to the asset catalog yet, use ingest_add_local_data to scan and add them.
'''
qcpin_directory = "/Volumes/DunbarSSD/Project/SeafloorGeodesy/Misc/20250812"
# Uncomment the following line to ingest local QC PIN files:
#workflow.ingest_add_local_data(directory_path=qcpin_directory)


'''
Step 5: Run the QC processing pipeline.

Option A: Run the full pipeline (all steps)
'''
workflow.preprocess_run_pipeline_qc(job="all")


'''
Option B: Run individual pipeline steps

You can run specific steps of the pipeline individually:

# Process QC PIN files to extract shotdata and GNSS epochs
workflow.preprocess_run_pipeline_qc(job="process_qcpin")

# Generate RINEX files from QC GNSS observations
workflow.preprocess_run_pipeline_qc(job="build_rinex")

# Run PRIDE-PPP on RINEX files
workflow.preprocess_run_pipeline_qc(job="run_pride")

# Process kinematic files
workflow.preprocess_run_pipeline_qc(job="process_kinematic")

# Refine shotdata with high-precision positions
workflow.preprocess_run_pipeline_qc(job="refine_shotdata")
'''


'''
Step 6 (Optional): Custom configuration.

You can override default pipeline settings using configuration dictionaries
or config objects.
'''
# Example: Run with custom configuration
# from es_sfgtools.workflows.pipelines.config import QCPipelineConfig, QCPinConfig

# custom_config = {
#     "qcpin_config": {"n_processes": 8, "override": True},
#     "rinex_config": {"time_interval": 86400},
#     "pride_config": {"override": False},
# }
# workflow.preprocess_run_pipeline_qc(job="all", primary_config=custom_config)


'''
Step 7 (Optional): Get pipeline instance for advanced usage.

For more control, you can get the pipeline instance directly and
call methods individually.
'''
# pipeline = workflow.preprocess_get_pipeline_qc()
# pipeline.process_qcpin()
# pipeline.get_rinex_files()
# pipeline.process_rinex()
# pipeline.process_kin()
# pipeline.update_shotdata()


print(f"{'='*60}")
print(f"QC Pipeline processing complete for {NETWORK}/{STATION}/{CAMPAIGN}")
print(f"{'='*60}")
