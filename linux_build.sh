#!/bin/bash
apt-get update
apt-get install -y build-essential software-properties-common libsuitesparse-dev gfortran

# set environment variables
export SUITESPARSE_INCLUDE_DIR=/usr/include
export SUITESPARSE_LIBRARY_DIR=/usr/lib/x86_64-linux-gnu

if ! command -v conda &> /dev/null; then
    echo "conda could not be found"
    exit 1
fi
conda env create -f linux_enviroment.yml


# build golang binaries
cd src/golangtools

# change shell to conda environment and run makefile
CONDA_ENV_NAME="seafloor_geodesy_linux"

conda activate $CONDA_ENV_NAME
make -B 

cd ../../