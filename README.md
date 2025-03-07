# EarthScope Seafloor Geodesy Tools

This repo contains a python library `es_sfgtools` designed to enable users to preprocess raw GNSS-A data from Liquid Robotics SV2 and SV3 wavegliders, as well as run GNSS-A processing using [GARPOS](https://github.com/s-watanabe-jhod/garpos)

Due to a dependency of GARPOS, the library currently is only installable via conda.  Also GARPOS installation requires gfortran, which (if you dont already have it) can be installed on a mac with the command
> brew install gfortran

### Conda Install Instructions

clone the library and enter the repo
> git clone https://github.com/EarthScope/es_sfgtools.git

> cd es_sfgtools

create and activate conda environment (use the correct environment file for your OS)
i.e. on mac:
> conda env create -f environment_mac.yml

> conda activate seafloor_geodesy_mac

In order to run parts of the library dependent on TileDB, you may also need to set the following environmental variable (use the correct path to your conda environment lib folder)

> export DYLD_LIBRARY_PATH=/Users/gottlieb/miniconda3/envs/seafloor_geodesy/lib


## In development... [ReadTheDocs](https://es-sfgtools.readthedocs.io/en/latest/)

### notes on dependency files


environment.yml
- conda dependencies file for linux

environment_mac.yml
- conda dependencies file for mac

pyproject.toml
- used to pip install this library into the conda environment. 

requirements.txt
- not used currently

requirements-dev.txt
- dev requirements pointed to by pyproj.toml

docs/requirements.txt
- requirements currently pointed to by pyproj.toml.  currently contains sphynx/RTD dependencies only, untested if that works.

