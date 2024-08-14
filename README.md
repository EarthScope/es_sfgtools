# EarthScope Seafloor Geodesy Tools

This repo contains a python library `es_sfgtools` designed to enable users to preprocess raw GNSS-A data from SV2 and SV3 wavegliders, as well as run GNSS-A processing using [GARPOS](https://github.com/s-watanabe-jhod/garpos)

The plan is to make this library will be published on PyPi, and installable as follows

> pip install es_sfgtools

For now, you need to clone the repo and install in your python environment from the repo base directory using

> pip install .

or in editable mode for development work using

> pip install -e .