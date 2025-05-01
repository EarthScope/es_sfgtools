# Configuration File Overview

This document provides an overview of the key fields in the configuration file used for the command-line application. The configuration file defines the main working directory and specifies jobs for data ingestion, downloading, and processing.

---

## Fields and Their Usage

### `projectDir`
- **Description**: This is the primary directory where all processing, ingestion, and downloaded data are stored.
- **Usage**: The application reads and writes data from/to this directory during execution.

- **Examples**:

  **JSON**:
  ```json
  {
    "projectDir": "/Users/user/Project/SeaFloorGeodesy/Data/SFGMain",
    "operations": [],
    "globalConfig": {}
  }
  ```

  **YAML**:
  ```yaml
  projectDir: "/Users/user/Project/SeaFloorGeodesy/Data/SFGMain"
  operations: []
  globalConfig: {}
  ```

---

### `operations`
- **Description**: This is where the rubber meets the road. In this list of dictionaries, we specify respective operations. An operation is a collection of "jobs" that pertain to a specific network, station, and campaign.

- **Structure**:
  - `network`: Defines the network associated with the station.
  - `station`: Identifies the specific station in the network.
  - `campaign`: Names the campaign related to the data collection.
  - `jobs`: Collection of jobs for this structure.

- **Examples**:

  **JSON**:
  ```json
  {
    "projectDir": "/Users/user/Project/SeaFloorGeodesy/Data/SFGMain",
    "operations": [
      {
        "network": "cascadia-gorda",
        "station": "NDP1",
        "campaign": "2023_from_john",
        "jobs": []
      }
    ],
    "globalConfig": {}
  }
  ```

  **YAML**:
  ```yaml
  projectDir: "/Users/user/Project/SeaFloorGeodesy/Data/SFGMain"
  operations:
    - network: "cascadia-gorda"
      station: "NDP1"
      campaign: "2023_from_john"
      jobs: []
  globalConfig: {}
  ```

---

#### `jobs`
There are types of jobs that the pipeline can handle. They are:
  - preprocessing
  - ingestion
  - download

---

##### `ingestion`
- **Description**: Specifies the data ingestion jobs, defining where raw data files are located before being processed.
- **Usage**: The application processes the data from the specified directory and adds it to the appropriate project location.

- **Examples**:

  **JSON**:
  ```json
  {
    "type": "ingestion",
    "directory": "/Users/user/Project/SeaFloorGeodesy/Data/Cascadia2023/NDP1/HR"
  }
  ```

  **YAML**:
  ```yaml
  type: "ingestion"
  directory: "/Users/user/Project/SeaFloorGeodesy/Data/Cascadia2023/NDP1/HR"
  ```

---

##### `download`
- **Description**: Defines jobs for downloading remote data assets.
- **Usage**: The application checks for available remote assets and downloads them to the corresponding directory.

- **Examples**:

  **JSON**:
  ```json
  {
    "type": "download"
  }
  ```

  **YAML**:
  ```yaml
  type: "download"
  ```

---

##### `preprocessing`
- **Description**: Defines processing jobs that run on the ingested or downloaded data.
- **Usage**: The application applies specific processing pipelines based on the configuration settings for each station and campaign.

- **Examples**:

  **JSON**:
  ```json
  {
    "type": "processing",
    "config": {
      "rinex_config": { "override": true, "time_interval": 24 },
      "pride_config": { "sample_frequency": 1 }
    }
  }
  ```

  **YAML**:
  ```yaml
  type: "processing"
  config:
    rinex_config:
      override: true
      time_interval: 24
    pride_config:
      sample_frequency: 1
  ```

---

## Putting it All Together

- **Examples**:

  **JSON**:
  ```json
  {
    "projectDir": "/Users/user/Project/SeaFloorGeodesy/Data/SFGMain",
    "operations": [
      {
        "network": "cascadia-gorda",
        "station": "NDP1",
        "campaign": "2023_from_john",
        "jobs": [
          {
            "type": "ingestion",
            "directory": "/Users/user/Project/SeaFloorGeodesy/Data/Cascadia2023/NDP1/HR"
          },
          {
            "type": "processing",
            "config": {
              "rinex_config": { "override": true, "time_interval": 24 },
              "pride_config": { "sample_frequency": 1 }
            }
          }
        ]
      }
    ],
    "globalConfig": {}
  }
  ```

  **YAML**:
  ```yaml
  projectDir: "/Users/user/Project/SeaFloorGeodesy/Data/SFGMain"
  operations:
    - network: "cascadia-gorda"
      station: "NDP1"
      campaign: "2023_from_john"
      jobs:
        - type: "ingestion"
          directory: "/Users/user/Project/SeaFloorGeodesy/Data/Cascadia2023/NDP1/HR"
        - type: "processing"
          config:
            rinex_config:
              override: true
              time_interval: 24
            pride_config:
              sample_frequency: 1
  globalConfig: {}
  ```

---

## Configuration
See the [Configuration Settings](INDEX.md) for details on fine-tuning each step.