# Configuration File Overview

This document provides an overview of the key fields in the configuration file used for the command-line application. The configuration file defines the main working directory and specifies jobs for data ingestion, downloading, and processing.

## Fields and Their Usage

### `main_dir`
- **Description**: This is the primary directory where all processing, ingestion, and downloaded data are stored.
- **Usage**: The application reads and writes data from/to this directory during execution.
- **Example**:
  ```yaml
  main_dir: /Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain
  ```

### `ingestion`
- **Description**: Specifies the data ingestion jobs, defining where raw data files are located before being processed.
- **Usage**: The application processes the data from the specified directory and adds it to the appropriate project location.
- **Structure**:
  - `network`: Defines the network associated with the station.
  - `station`: Identifies the specific station in the network.
  - `campaign`: Names the campaign related to the data collection.
  - `directory`: Specifies the location of the data files to be ingested.
- **Example**:
  ```yaml
  ingestion:
    jobs:
      - network: cascadia-gorda
        station: NDP1
        campaign: 2023_from_john
        directory: /Users/franklyndunbar/Project/SeaFloorGeodesy/Data/Cascadia2023/NDP1/HR
  ```

### `download`
- **Description**: Defines jobs for downloading remote data assets.
- **Usage**: The application checks for available remote assets and downloads them to the corresponding directory.
- **Structure**:
  - `network`: Defines the network associated with the station.
  - `station`: Identifies the specific station in the network.
  - `campaign`: Names the campaign related to the data collection.
- **Example**:
  ```yaml
  download:
    jobs:
      - network: cascadia-gorda
        station: GCC1
        campaign: 2023_A_1063
  ```

### `processing`
- **Description**: Defines processing jobs that run on the ingested or downloaded data.
- **Usage**: The application applies specific processing pipelines based on the configuration settings for each station and campaign.
- **Structure**:
  - `network`: Defines the network associated with the station.
  - `station`: Identifies the specific station in the network.
  - `campaign`: Names the campaign related to the data collection.
  - `config`: Contains processing parameters such as `rinex_config`.
    - `rinex_config`: Holds settings related to RINEX data processing, including time interval and override options.
- **Example**:
  ```yaml
  processing:
    jobs:
      - network: cascadia-gorda
        station: NDP1
        campaign: 2023_from_john
        config:
          rinex_config:
            time_interval: 24
            override: false
  ```


## Configuration
See the [Configuration Settings](INDEX.md) for details on fine-tuning each step