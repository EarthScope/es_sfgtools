# EarthScope Seafloor Geodesy Tools
[![Read the Docs](https://readthedocs.org/projects/es-sfgtools/badge/?version=latest)](https://es-sfgtools.readthedocs.io/en/latest/)

`es_sfgtools` is a Python library designed to support preprocessing and GNSS-A processing workflows for Seafloor Geodesy using data from Liquid Robotics SV2/SV3 Wave Gliders.  

The toolkit also integrates with the [**GARPOS**](https://github.com/s-watanabe-jhod/garpos) GNSS-A processing.

Due to a dependency of GARPOS, the library currently is only installable via conda.  Also GARPOS installation requires gfortran, which (if you dont already have it) can be installed on a mac with the command
> brew install gfortran

## Installation  

1. Clone the repository

    ```bash
    git clone https://github.com/EarthScope/es_sfgtools.git
    cd es_sfgtools
    ```

2. Create and activate a Conda environment

    Choose the environment file appropriate for your operating system.

    **macOS**

    ```bash
    conda env create -f mac_environment.yml
    conda activate seafloor_geodesy_mac
    ```

    **linux**

    ```bash
    conda env create -f linux_environment.yml
    conda activate seafloor_geodesy_mac
    ```

    These environment files provide all required scientific and compiler dependencies.

    **macOS TileDB Note (DYLD path)**

    In order to run parts of the library dependent on TileDB, you may also need to set the following environmental variable (use the correct path to your conda environment lib folder)

    `export DYLD_LIBRARY_PATH="/path/to/conda/env/lib"`

    For example:
    `export DYLD_LIBRARY_PATH="$HOME/miniconda3/envs/seafloor_geodesy_mac/lib"`

## Documentation

Documentation (in development) is available on ReadTheDocs:

[ReadTheDocs](https://es-sfgtools.readthedocs.io/en/latest/)

## Repository Files & Dependency Notes

* `linux_environment.yml`
  * Conda environment specification for Linux.

* `mac_environment.yml`
  * Conda environment specification for macOS.
  * Includes macOS compiler toolchain (clang, gfortran).

* `pyproject.toml`
  * Defines Python package metadata for PyPI distribution.
  * Conda environment installs from this.

* `requirements-dev.txt`
  * dev requirements pointed to by pyproj.toml

* `docs/requirements.txt`
  * Documentation build dependencies (Sphinx, RTD theme, myst-parser).

## Versioning

This project uses setuptools_scm for automatic versioning from git tags. Version information is generated at build time.

---

**Maintainers**: Mike Gottlieb, Franklyn Dunbar, Rachel Akie
**Organization**: [EarthScope](https://www.earthscope.org/)