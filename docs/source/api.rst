Code documentation
=============================================

API reference for the three packages in this monorepo:

- **earthscope_sfg** — data models, parsing tools, TileDB schemas, utilities
- **earthscope_sfg_workflows** — workflows, pipelines, data management, modeling, configuration
- **earthscope_sfg_cli** — command-line interface

----

earthscope_sfg
==============

Data models, hardware parsers, TileDB schemas, and shared utilities.

Data Models
~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg.data_models.*

NovAtel Tools
~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg.novatel_tools.*

Sonardyne Tools
~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg.sonardyne_tools.*

TileDB Schemas
~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg.tiledb_schemas.*

Seafloor Site Tools
~~~~~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg.seafloor_site_tools.*

Utilities
~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg.utils.*

Logging
~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg.logging.*

----

earthscope_sfg_workflows
========================

Workflow orchestration, data management, modeling, and configuration.

Workflows & Pipelines
~~~~~~~~~~~~~~~~~~~~~

Workflow Handler

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.workflows.workflow_handler

Pre-Processing

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.workflows.preprocess_ingest.*

Intermediate Processing

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.workflows.midprocess.*

Modeling (Workflows)

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.workflows.modeling.*

Pipelines

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.workflows.pipelines.*

Workflow Utils

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.workflows.utils.*

Modeling
~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.modeling.garpos_tools.*

Data Management
~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.data_mgmt.assetcatalog.*
   api/earthscope_sfg_workflows.data_mgmt.directorymgmt.*
   api/earthscope_sfg_workflows.data_mgmt.ingestion.*
   api/earthscope_sfg_workflows.data_mgmt.utils

Metadata Models
~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.data_models.metadata.*

Configuration
~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.config.*

Prefiltering
~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_workflows.prefiltering.*

----

earthscope_sfg_cli
==================

Command-line interface for manifest-driven pipeline execution.

.. toctree::
   :glob:
   :maxdepth: 1

   api/earthscope_sfg_cli.*
