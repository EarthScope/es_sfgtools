FROM continuumio/miniconda3:latest

# Enable universe repository and install dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get update && \
    apt-get install -y libsuitesparse-dev gfortran

# Set environment variables for SuiteSparse locations
ENV SUITESPARSE_INCLUDE_DIR=/usr/local/include
ENV SUITESPARSE_LIBRARY_DIR=/usr/lib/aarch64-linux-gnu

# Copy project files into the container
COPY . /home/es_sfgtools

# Install dependencies using conda
RUN cd /home/es_sfgtools && conda env create -f environment.yml
