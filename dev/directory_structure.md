# GARPOS Results Folder Structure Guide

## Overview
This archive contains processed seafloor geodesy data for the **cascadia-gorda** network, **2025_A_1126** campaign across three stations: **NBR1**, **NCC1**, and **NTH1**.

## Folder Structure

```
cascadia-gorda/
├── NBR1/
│   └── 2025_A_1126/
│       └── GARPOS/
├── NCC1/
│   └── 2025_A_1126/
│       └── GARPOS/
└── NTH1/
    └── 2025_A_1126/
        └── GARPOS/
```

## Where to Find GARPOS Results

### Primary Results Location
For each station, navigate to:
```
<station>/2025_A_1126/<survey_id>/GARPOS/results/<run_id>/
```

Example:
```
NBR1/2025_A_1126/2025_A_1126_NBR1_1/GARPOS/results/Test2/
```

### Garpos Directory contents

#### **1. Shot Data**
- `<survey id>_<survey type>_shotdata_filtered_rectified.csv` - Preprocessed acoustic ranging data used as input
 (ex: `2025_A_1126_1_circledrive_shotdata_filtered_rectified.csv`)

- Filtered according to quality criteria (acoustic diagnostics, PRIDE residuals, etc.)

#### **2. Logs directory**
- logs from the run

#### **3. sound velocity profile**
- svp in .csv format

### Results Directory Contents

Each `results/<run_id>/` folder contains:

#### **1. GARPOS output files**
- `*-res.dat`
- `*-obs.csv`

#### **2. Model Parameters**
- `<run_id>_<iteration>_settings.ini` - GARPOS configuration used for processing
- `<run_id>_<iteration>_observation.ini` - Obs file use for each iteration


#### **4. Plots (if generated)**
- `<run id>_results.png`  Position time series with residuals
- Color-coded by residual magnitude for quality assessment


### Survey Organization

Each station has multiple surveys organized as:
```
GARPOS/
├── 2025_A_1126_<STATION>_1/    # First survey
│   └── results/
│       └── run_Test2/              # Run ID: Test2
├── 2025_A_1126_<STATION>_2/    # Second survey
│   └── results/
│       └── run_Test2/
└── 2025_A_1126_<STATION>_3/    # Third survey
    └── results/
        └── run_Test2/
```

## Processing Configuration

### Run ID: Test2

**GARPOS Settings:**
- **Iterations:** 2
- **Max loops:** 
  - NBR1: 100
  - NCC1: 50
  - NTH1: 50

**Data Filters Applied:**
- Acoustic diagnostics: Enabled (level: "OK")
- PRIDE residuals: Disabled
- Max distance from center: Disabled
- Ping replies: Disabled

