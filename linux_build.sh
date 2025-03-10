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

# Stage 2: build golang binaries
#SHELL ["conda", "run", "-n", "seafloor_geodesy_linux", "/bin/bash", "-c"]

# build golang binaries
cd src/golangtools

# change shell to conda environment and run makefile
CONDA_ENV_NAME="seafloor_geodesy_linux"
# CONDA_ENV_PATH=$(conda env list | awk -v env_name="$CONDA_ENV_NAME" '$1 == env_name {print $NF}')

# # Check if the environment path was found
# if [ -z "$CONDA_ENV_PATH" ]; then
#     echo "Conda environment '$CONDA_ENV_NAME' not found."
#     exit 1
# else
#     echo "Conda environment '$CONDA_ENV_NAME' path: $CONDA_ENV_PATH"
# fi

# # run makefile
# $CONDA_ENV_PATH/bin/bash run -n
conda activate $CONDA_ENV_NAME
make -B 

cd ../../