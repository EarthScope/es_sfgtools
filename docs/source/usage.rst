Usage Guide
===========

This guide provides comprehensive instructions for installing and using es_sfgtools for seafloor geodesy data processing. The package supports complete workflows from raw data ingestion through acoustic positioning modeling.

.. _installation:

Installation
------------

The es_sfgtools package provides comprehensive tools for seafloor geodesy data processing, including GNSS-Acoustic positioning workflows, data management, and acoustic modeling.

**Future PyPI Installation (Recommended)**

The package will be published on PyPI and installable as follows:

.. code-block:: bash

   pip install es_sfgtools

**Development Installation (Current)**

For now, clone the `repository <https://github.com/EarthScope/es_sfgtools>`_ and install from source:

.. code-block:: bash

   git clone https://github.com/EarthScope/es_sfgtools.git
   cd es_sfgtools
   pip install .

For development work, use editable mode:

.. code-block:: bash

   pip install -e .

**Dependencies**

The package requires several external tools for complete functionality:

- **GARPOS**: Acoustic positioning solver (set ``GARPOS_PATH`` environment variable)
- **PRIDE-PPPAR**: Precise GNSS processing (set ``PATH`` to include binaries)
- **AWS CLI**: For cloud data access (configure credentials)
- **TileDB**: Array storage (installed automatically with package)

.. _:

Environment Setup
-----------------

es_sfgtools uses conda environments for dependency management. Two main environments are supported,
Linux and macOS.

Build the linux environment:

.. code-block:: bash
  
   conda env create -f linux_environment.yml
   conda activate es_sfgtools

Build the macOS environment:

.. code-block:: bash

   conda env create -f macos_environment.yml
   conda activate es_sfgtools



Examples and Workflows
---------------------

The package includes comprehensive examples demonstrating different aspects of seafloor geodesy processing:

Data Organization
~~~~~~~~~~~~~~~~

es_sfgtools follows a hierarchical data organization to manage data from multiple seafloor networks, stations, and campaigns. The workflow module assumes a directory structure as follows:

.. code-block:: text

   Main Directory/
   ├── network_name/           # e.g., "cascadia-gorda"
   │   ├── station_name/       # e.g., "NCC1", "NBR1"
   │   │   ├── campaign_name/  # e.g., "2025_A_1126"
   │   │   │   ├── raw/        # Raw data files
   │   │   │   ├── processed/  # Processed products
   │   │   │   ├── logs/       # Processing logs
   │   │   │   └── results/    # Analysis results


GeoLab Data Processing
~~~~~~~~~~~~~~~~~~~~~

The GeoLab example (``examples/geolab/get_data.py``) demonstrates mid-process data preparation for acoustic modeling:

.. code-block:: python

   #!/usr/bin/env python3
   """
   Seafloor Geodesy Data Processing Demo - GeoLab Environment
   
   Demonstrates mid-process workflow for preparing data for GARPOS modeling in GEOLAB.
   """
   
   import os
   from es_sfgtools.workflows.workflow_handler import WorkflowHandler
   
   # Configure GeoLab environment
   DEFAULT_CONFIG = {
       "WORKING_ENVIRONMENT": "GEOLAB",
       "MAIN_DIRECTORY_GEOLAB": "/path/to/geolab/data",
       "S3_SYNC_BUCKET": "your-s3-bucket"
   }
   
   for key, value in DEFAULT_CONFIG.items():
       os.environ[key] = value
   
   # Initialize workflow handler
   workflow = WorkflowHandler()
   
   # Configure GARPOS data filters
   FILTER_CONFIG = {
       "acoustic_filters": {
           "enabled": True,
           "level": "OK",
           "description": "Apply standard acoustic data quality filters"
       }
   }
   
   # Process multiple stations
   NETWORK = "cascadia-gorda"
   CAMPAIGN = "2025_A_1126"
   STATIONS = ["NTH1", "NCC1", "NBR1", "GCC1"]
   
   for station in STATIONS:
       # Set processing context
       workflow.set_network_station_campaign(
           network_id=NETWORK,
           station_id=station,
           campaign_id=CAMPAIGN,
       )
       
       # Parse survey data
       workflow.midprocess_parse_surveys()
       
       # Prepare GARPOS data with quality filters
       workflow.midprocess_prep_garpos(custom_filters=FILTER_CONFIG)
       workflow.modeling_run_garpos()

Complete Preprocessing Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The preprocessing example (``examples/preprocessing/preprocessing.py``) shows the full SV3 data processing pipeline:

.. code-block:: python

   #!/usr/bin/env python3
   """
   Complete SV3 Preprocessing Pipeline Example
   
   Demonstrates the full preprocessing workflow from raw data to analysis-ready products.
   """
   
   import os
   from pathlib import Path
   from es_sfgtools.workflows.workflow_handler import WorkflowHandler
   
   # Configure external tool paths
   os.environ["GARPOS_PATH"] = str(Path.home() / "garpos")
   os.environ["PATH"] += os.pathsep + str(Path.home() / ".PRIDE_PPPAR_BIN")
   
   def main():
       # Initialize workflow with data directory
       main_dir = Path("/path/to/seafloor/data")
       workflow = WorkflowHandler(main_dir)
       
       # Configure processing parameters
       global_config = {
           "novatel_config": {
               "n_processes": 14,
               "override": False
           },
           "pride_config": {
               "cutoff_elevation": 7,
               "frequency": ["G12", "R12", "E15", "C26", "J12"],
               "system": "GREC23J"
           },
           "rinex_config": {
               "time_interval": 24,
               "override": False
           }
       }
       
       # Set processing context
       workflow.set_network_station_campaign(
           network_id="cascadia-gorda",
           station_id="NCC1", 
           campaign_id="2025_A_1126"
       )
       
       # Add local raw data to catalog
       raw_data_dir = main_dir / "cascadia-gorda" / "NCC1" / "2025_A_1126" / "raw"
       workflow.ingest_add_local_data(directory_path=raw_data_dir)
       
       # Run complete SV3 preprocessing pipeline
       workflow.preprocess_run_pipeline_sv3(
           job="all",
           primary_config=global_config
       )
   
   if __name__ == "__main__":
       main()

**Pipeline Components Demonstrated:**
- **Raw Data Ingestion**: Scanning and cataloging local data files
- **NOVATEL Processing**: Converting binary GNSS observations
- **RINEX Generation**: Creating standardized GNSS observation files  
- **PRIDE-PPPAR Processing**: Precise GNSS positioning solutions
- **Kinematic Processing**: High-rate position and velocity solutions
- **Data Quality Control**: Automated filtering and validation



Basic Workflow Pattern
~~~~~~~~~~~~~~~~~~~~~

Most processing follows this general pattern:

.. code-block:: python

   from es_sfgtools.workflows.workflow_handler import WorkflowHandler
   
   # 1. Initialize workflow
   workflow = WorkflowHandler(data_directory)
   
   # 2. Set processing context
   workflow.set_network_station_campaign(network, station, campaign)
   
   # 3. Add data to the catalog for preprocessing

   # 3.1 Ingest raw data (optional)
   workflow.ingest_add_local_data(raw_data_path)
   
   # 3.2 Download cloud data (optional)
   workflow.ingest_catalog_archive_data()
   workflow.ingest_download_catalog_data()

   # 4. Run preprocessing
   workflow.preprocess_run_pipeline_sv3()
   
   # 5. Parse and prepare data
   workflow.midprocess_parse_surveys()
   workflow.midprocess_prep_garpos()
   
   # 6. Run acoustic modeling
   workflow.modeling_run_garpos()
   
   # 7. Generate results and plots
   workflow.modeling_plot_garpos_results()

Configuration Management
~~~~~~~~~~~~~~~~~~~~~~~

Customize processing with configuration dictionaries:

.. code-block:: python

   # GNSS processing configuration
   gnss_config = {
       "pride_config": {
           "cutoff_elevation": 7,
           "frequency": ["G12", "R12", "E15"],
           "system": "GREC23J"
       },
       "novatel_config": {
           "n_processes": 8,
           "override": False
       }
   }
   
   # Data quality filters
   filter_config = {
       "acoustic_filters": {"enabled": True, "level": "OK"},
       "pride_residuals": {"enabled": True, "max_residual_mm": 10}
   }

Getting Help
-----------

**Documentation**
- :doc:`api` - Complete API reference
- `GitHub Repository <https://github.com/EarthScope/es_sfgtools>`_ - Source code and issues

**Community Support**
- GitHub Issues for bug reports and feature requests
- EarthScope forums for scientific discussions
- Tutorial workshops and webinars

**Contributing**
Contributions are welcome! See the repository for development guidelines and coding standards. 