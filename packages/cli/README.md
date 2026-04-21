# This tutorial outlines the steps needed to build a configuration file and run it to perform pre-processing and modeling on seafloor gnss acoustic data.


# 1. Configuration File 

The configuration file allows users to schedule jobs specific to a given network,station, and campaign. This section gives an overview of the key fields in the configuration file used for the command-line application.

Note: empty fields will be described later.

## Fields and Their Usage

### `projectDir`
Note: this is not required when working in GeoLab.

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
  - garpos

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
    "type": "preprocessing",
    "config": {
      "rinex_config": { "override": true, "time_interval": 24, "modulo_millis": 1000 },
      "pride_config": { "sample_frequency": 1 }
    }
  }
  ```

  **YAML**:
  ```yaml
  type: "preprocessing"
  config:
    rinex_config:
      override: true
      time_interval: 24
      modulo_millis: 1000  # decimate to 1 Hz (keep epochs at 1-second intervals)
    pride_config:
      sample_frequency: 1
  ```

---

##### `garpos`
Note: see [GARPOS config](INDEX.md#garpos-config) for config details
- **Description**: Defines a GARPOS modeling job

- **Examples**:

  **JSON**:
  ```json
    {
      "network": "cascadia-gorda",
      "station": "NCC1",
      "campaign": "2024_A_1126",
      "jobs": [
         {
          "type": "garpos",
          "config": {
            "run_id": 1,
            "override": true,
            "inversion_params": { "rejectcriteria": 2.5, "log_lambda": [0] }
          }
        }
      ]
    }
  ```

  **YAML**:
  ```yaml
  - network: "cascadia-gorda"
    station: "NCC1"
    campaign: "2024_A_1126"
    jobs:
      - type: "garpos"
        config:
          run_id: 1
          override: true
          inversion_params:
            rejectcriteria: 2.5
            log_lambda:
              - 0
  ```


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
            "type": "preprocessing",
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
        - type: "preprocessing"
          config:
            rinex_config:
              override: true
              time_interval: 24
            pride_config:
              sample_frequency: 1
  globalConfig: {}
  ```


# Useage

**CLI**
```bash

$ python3 path/to/es_sfgtools/app run your_config.json
```

**Python Scripting**
```python

from es_sfgtools.app.src import run_manifest,PipelineManifest

configPath = '/path/to/your_config.json'

pipelineManifest = PipelineManifest.from_json(configPath)

run_manifest(piplineManifest)

```

---

## Configuration
See the [Configuration Settings](INDEX.md) for details on fine-tuning each step.