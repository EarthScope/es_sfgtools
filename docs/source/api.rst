Code documentation
=============================================

This page is the top-level API index. Modules are organized by functional
area and correspond to the monorepo package layout:

- **earthscope-sfg** — data models, parsing tools, TileDB schemas, utilities
- **earthscope-sfg-workflows** — workflows, pipelines, data management, modeling, configuration
- **earthscope-sfg-cli** — command-line interface

----

Workflows & Pipelines
---------------------

High-level workflow and pipeline implementations.

Workflow Handler
~~~~~~~~~~~~~~~~
.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.workflows.workflow_handler

Pre-Processing
~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.workflows.preprocess_ingest.*

Intermediate Processing
~~~~~~~~~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.workflows.midprocess.*

Modeling
~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.workflows.modeling.*

Pipelines
~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.workflows.pipelines.*

Utils
~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.workflows.utils.*

----

Modeling
--------

Numerical modeling and GARPOS-specific utilities.

Garpos Tools
~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.modeling.garpos_tools.*

----

Data management
---------------

Catalogs, ingestion, and directory management utilities.

Asset Catalog
~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.data_mgmt.assetcatalog.*

Directory Management
~~~~~~~~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.data_mgmt.directorymgmt.*

Ingestion
~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.data_mgmt.ingestion.*

Shared Utilities
~~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.data_mgmt.utils

----

Data models
-----------

Pydantic models and schemas used across the project.

Metadata Models
~~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.data_models.metadata.*

Other Data Models
~~~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.data_models.community_standards
   api/es_sfgtools.data_models.constants
   api/es_sfgtools.data_models.log_models
   api/es_sfgtools.data_models.observables
   api/es_sfgtools.data_models.sv3_models

----

NovAtel & Sonardyne
--------------------

Driver and processing helpers for NovAtel and Sonardyne devices.

NovAtel Tools
~~~~~~~~~~~~~~
.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.novatel_tools.*

Sonardyne Tools
~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.sonardyne_tools.*

----

TileDB Schemas
--------------

TileDB array schemas and array helpers.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.tiledb_schemas.*

----

Seafloor Site Tools & Prefiltering
----------------------------------

Seafloor Site Tools
~~~~~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.seafloor_site_tools.*

Prefiltering
~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.prefiltering.*

----

Utilities, config, logging
--------------------------

Utilities
~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.utils.*

Configuration Management
~~~~~~~~~~~~~~~~~~~~~~~~

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.config.*

Logging
~~~~~~~
.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.logging.*
