# EarthScope Seafloor Geodesy Tools

This repo contains a python library `es_sfgtools` designed to enable users to preprocess raw GNSS-A data from SV2 and SV3 wavegliders, as well as run GNSS-A processing using [GARPOS](https://github.com/s-watanabe-jhod/garpos)


Due to a dependency of GARPOS, the library currently is only installable via conda.  Also GARPOS installation requires gfortran, which (if you dont already have it) can be installed on a mac with the command
> brew install gfortran

### Conda Install Instructions

clone the library and enter the repo
> git clone https://github.com/EarthScope/es_sfgtools.git

> cd es_sfgtools

create and activate conda environment
> conda env create -f environment.yml

> conda activate seafloor_geodesy

In order to run parts of the library dependent on TileDB, you will also need to set the following environmental variable (use the correct path to your conda environment lib folder)

> export DYLD_LIBRARY_PATH=/Users/gottlieb/miniconda3/envs/seafloor_geodesy/lib

## In development... [ReadTheDocs](https://es-sfgtools.readthedocs.io/en/latest/)


# Deprecated install, doesn't work due to scikit-sparse dependency

The plan is to make this library will be published on PyPi, and installable as follows

> pip install es_sfgtools

For now, you need to clone the repo and install in your python environment from the repo base directory using

> pip install .

or in editable mode for development work using

> pip install -e .

