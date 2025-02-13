FROM continuumio/miniconda3:latest

# Set environment variables for SuiteSparse locations
ENV SUITESPARSE_INCLUDE_DIR=/usr/local/include
ENV SUITESPARSE_LIBRARY_DIR=/usr/lib/aarch64-linux-gnu

# Copy project files into the container
COPY . /home/es_sfgtools
# Stage 1: build env
WORKDIR /home/es_sfgtools

# Enable universe repository and install dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y libsuitesparse-dev gfortran && \
    apt-get install -y build-essential &&\
    conda env create -f environment.yml &&\
    conda clean --all -y




# Stage 2: build golang binaries
SHELL ["conda", "run", "-n", "seafloor_geodesy", "/bin/bash", "-c"]
WORKDIR /home/es_sfgtools/src/golangtools/
RUN make  

# Stage 3: install pride
WORKDIR /home
RUN git clone --depth 1 https://github.com/PrideLab/PRIDE-PPPAR.git

ENV WORK_DIR /home/PRIDE-PPPAR
# Install the software
RUN cd $WORK_DIR &&\
    chmod +x install.sh &&\
    echo "Y" | HOME=/home ./install.sh 

WORKDIR /home/es_sfgtools
SHELL ["conda", "run", "-n", "seafloor_geodesy", "/bin/bash", "-c"]
# Set up environment
RUN echo "conda activate seafloor_geodesy" >> ~/.bashrc &&\
    echo "export PATH=/home/.PRIDE_PPPAR_BIN:$PATH" >> ~/.bashrc

# Set entrypoint to bash
ENTRYPOINT ["bash", "-l"]