API Reference
=============

This section provides comprehensive documentation for all modules, classes, and functions 
in the `es_sfgtools` package. The package is organized into several main categories that 
support the complete seafloor geodesy data processing workflow.

.. toctree::
   :maxdepth: 1
   :caption: Complete API Reference

   api/modules

Core Package
------------

.. toctree::
   :maxdepth: 2

   api/es_sfgtools

Configuration Management
------------------------
Configuration modules for environment setup, file paths, and processing parameters.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.config

Data Management
---------------
Modules for data organization, asset catalogs, and directory structure management.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.data_mgmt
   api/es_sfgtools.data_mgmt.assetcatalog
   api/es_sfgtools.data_mgmt.directorymgmt

Data Models
-----------
Pydantic models for metadata, observables, and data validation.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.data_models
   api/es_sfgtools.data_models.metadata
   api/es_sfgtools.data_models.observables
   api/es_sfgtools.data_models.sv3_models

Logging System
--------------
Logging utilities and structured logging for workflow tracking.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.logging

Modeling Tools
--------------
GARPOS modeling tools and acoustic positioning analysis.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.modeling
   api/es_sfgtools.modeling.garpos_tools

Instrument-Specific Tools
------------------------
Tools for processing data from specific GNSS and acoustic instruments.

NOVATEL GNSS Tools
~~~~~~~~~~~~~~~~~~
Processing tools for NOVATEL GNSS receivers.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.novatel_tools

PRIDE-PPPAR Tools
~~~~~~~~~~~~~~~~~
Tools for precise GNSS processing using PRIDE-PPPAR.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.pride_tools

Sonardyne Acoustic Tools
~~~~~~~~~~~~~~~~~~~~~~~~
Processing tools for Sonardyne acoustic positioning systems.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.sonardyne_tools

Seafloor Site Tools
~~~~~~~~~~~~~~~~~~~
Tools for seafloor site metadata and sound velocity processing.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.seafloor_site_tools

Data Processing Components
--------------------------

Pre-filtering
~~~~~~~~~~~~~
Data quality control and filtering utilities.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.prefiltering

TileDB Integration
~~~~~~~~~~~~~~~~~~
Tools for TileDB array storage and management.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.tiledb_tools

Workflow Management
-------------------
High-level workflow orchestration and processing pipelines.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.workflows
   api/es_sfgtools.workflows.preprocess_ingest
   api/es_sfgtools.workflows.midprocess
   api/es_sfgtools.workflows.modeling
   api/es_sfgtools.workflows.pipelines

Utilities
---------
General utility functions and helper modules.

.. toctree::
   :maxdepth: 2

   api/es_sfgtools.utils

Module Index
------------
Alphabetical listing of all modules:

.. toctree::
   :maxdepth: 1

   api/es_sfgtools.config.env_config
   api/es_sfgtools.config.file_config
   api/es_sfgtools.config.garpos_config
   api/es_sfgtools.config.loadconfigs
   api/es_sfgtools.config.shotdata_filters
   api/es_sfgtools.config.tiledb_s3_config
   api/es_sfgtools.data_mgmt.assetcatalog.handler
   api/es_sfgtools.data_mgmt.assetcatalog.schemas
   api/es_sfgtools.data_mgmt.assetcatalog.tables
   api/es_sfgtools.data_mgmt.directorymgmt.config
   api/es_sfgtools.data_mgmt.directorymgmt.handler
   api/es_sfgtools.data_mgmt.directorymgmt.schemas
   api/es_sfgtools.data_mgmt.utils
   api/es_sfgtools.data_models.community_standards
   api/es_sfgtools.data_models.constants
   api/es_sfgtools.data_models.log_models
   api/es_sfgtools.data_models.metadata.benchmark
   api/es_sfgtools.data_models.metadata.campaign
   api/es_sfgtools.data_models.metadata.catalogs
   api/es_sfgtools.data_models.metadata.site
   api/es_sfgtools.data_models.metadata.utils
   api/es_sfgtools.data_models.metadata.vessel
   api/es_sfgtools.data_models.observables
   api/es_sfgtools.data_models.sv3_models
   api/es_sfgtools.logging.loggers
   api/es_sfgtools.modeling.garpos_tools.data_prep
   api/es_sfgtools.modeling.garpos_tools.functions
   api/es_sfgtools.modeling.garpos_tools.load_utils
   api/es_sfgtools.modeling.garpos_tools.plotting
   api/es_sfgtools.modeling.garpos_tools.schemas
   api/es_sfgtools.novatel_tools.novatel_ascii_operations
   api/es_sfgtools.novatel_tools.novatel_binary_operations
   api/es_sfgtools.novatel_tools.utils
   api/es_sfgtools.prefiltering.schemas
   api/es_sfgtools.prefiltering.utils
   api/es_sfgtools.pride_tools.gnss_product_operations
   api/es_sfgtools.pride_tools.gnss_product_schemas
   api/es_sfgtools.pride_tools.kin_file_operations
   api/es_sfgtools.pride_tools.pride_cli_config
   api/es_sfgtools.pride_tools.pride_file_config
   api/es_sfgtools.pride_tools.pride_operations
   api/es_sfgtools.pride_tools.rinex_utils
   api/es_sfgtools.seafloor_site_tools.soundspeed_operations
   api/es_sfgtools.sonardyne_tools.sv2_operations
   api/es_sfgtools.sonardyne_tools.sv3_operations
   api/es_sfgtools.sonardyne_tools.utils
   api/es_sfgtools.tiledb_tools.tiledb_operations
   api/es_sfgtools.tiledb_tools.tiledb_schemas
   api/es_sfgtools.tiledb_tools.utils
   api/es_sfgtools.utils.command_line_utils
   api/es_sfgtools.utils.custom_warnings_exceptions
   api/es_sfgtools.utils.model_update
   api/es_sfgtools.utils.rinex_histogram
   api/es_sfgtools.workflows.midprocess.mid_processing
   api/es_sfgtools.workflows.midprocess.utils
   api/es_sfgtools.workflows.modeling.garpos_handler
   api/es_sfgtools.workflows.pipelines.config
   api/es_sfgtools.workflows.pipelines.exceptions
   api/es_sfgtools.workflows.pipelines.plotting
   api/es_sfgtools.workflows.pipelines.shotdata_gnss_refinement
   api/es_sfgtools.workflows.pipelines.sv2_ops
   api/es_sfgtools.workflows.pipelines.sv2_pipeline
   api/es_sfgtools.workflows.pipelines.sv3_pipeline
   api/es_sfgtools.workflows.preprocess_ingest.data_handler
   api/es_sfgtools.workflows.workflow_handler

