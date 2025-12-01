Code documentation
=============================================

This page is the top-level API index. It groups generated API pages by functional
areas and package layout. The sections below match the current package
structure under `src/es_sfgtools`.

----

Workflows & Pipelines
---------------------

High-level workflow and pipeline implementations (SV2/SV3 and related pipeline
helpers).

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
Includes modules used for numerical modeling and GARPOS-specific utilities.

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

PRIDE & GNSS tooling
---------------------

Utilities and wrappers for PRIDE processing and GNSS product handling.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.pride_tools.*

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

TileDB Tools
--------------

TileDB schema and operations helpers.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.tiledb_tools.*

----

Seafloor Site Tools & Prefiltering
----------------------------------

Seafloor Site Tools
~~~~~~~~~~~~~~~~~~~
Seafloor site and sound-speed related utilities.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.seafloor_site_tools.*

Prefiltering
~~~~~~~~~~~~

Prefiltering utilities and schemas.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.prefiltering.*

----

Utilities, config, logging
--------------------------

General utilities, configuration loaders, and logging helpers.

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
