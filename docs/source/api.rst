Code documentation
=============================================

This page is the top-level API index. It groups generated API pages by functional
areas and package layout. The sections below match the current package
structure under `src/es_sfgtools`.

Workflows & Pipelines
---------------------

High-level workflow and pipeline implementations (SV2/SV3 and related pipeline
helpers).

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.workflows.*

Modeling
--------

Includes modules used for numerical modeling and GARPOS-specific utilities.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.modeling.*

Data management
---------------

Catalogs, ingestion, and directory management utilities.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.data_mgmt.*
   api/es_sfgtools.data_mgmt.assetcatalog.*

Data models
-----------

Pydantic models and schemas used across the project.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.data_models.*

PRIDE & GNSS tooling
---------------------

Utilities and wrappers for PRIDE processing and GNSS product handling.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.pride_tools.*

NovAtel & Sonardyne
--------------------

Driver and processing helpers for NovAtel and Sonardyne devices.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.novatel_tools.*
   api/es_sfgtools.sonardyne_tools.*

TileDB Tools
--------------

TileDB schema and operations helpers.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.tiledb_tools.*

Seafloor Site Tools
---------------------

Seafloor site and sound-speed related utilities.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.seafloor_site_tools.*

Prefiltering
------------

Small prefiltering utilities and schemas.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.prefiltering.*

Utilities, config, logging
--------------------------

General utilities, configuration loaders, and logging helpers.

.. toctree::
   :glob:
   :maxdepth: 1

   api/es_sfgtools.utils.*
   api/es_sfgtools.config.*
   api/es_sfgtools.logging.*
