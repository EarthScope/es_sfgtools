# Configuration Documentation

## NovatelConfig
This configuration class handles Novatel-specific settings.

- `override` (bool): If `True`, existing data will be overridden.
- `n_processes` (int): Number of processes to use for processing Novatel data. Defaults to `14`.

## RinexConfig
This configuration class manages settings for RINEX processing.

- `override` (bool): If `True`, existing data will be overridden.
- `n_processes` (int): Number of processes to use for processing RINEX data. Defaults to `14`.
- `time_interval` (Optional[int]): Time interval in seconds for RINEX file pagination. Higher numbers (24 is the max) mean faster processing but higher memory consumption. Defaults to `1s`.

## DFOP00Config
This configuration class handles settings related to DFOP00 processing.

- `override` (bool): If `True`, existing data will be overridden.

## PositionUpdateConfig
Configuration class for position update settings.

- `override` (bool): If `True`, existing data will be overridden.

## PridePdpConfig
Configuration for PRIDE PDP processing.

- `cutoff_elevation` (int): Elevation cutoff angle. Defaults to `7` degrees.
- `end` (Optional[str]): End time for processing.
- `frequency` (List[str]): List of frequency bands to process. Defaults to `[G12, R12, E15, C26, J12]`.
- `high_ion` (Optional[bool]): If `True`, considers high ionospheric activity.
- `interval` (Optional[int]): Processing interval in seconds.
- `local_pdp3_path` (Optional[Path]): Path to local PDP3 data.
- `loose_edit` (bool): If `True`, enables loose editing mode.
- `sample_frequency` (int): Frequency of sampling in Hz. Defaults to `1`.
- `start` (Optional[str]): Start time for processing.
- `system` (str): GNSS system to be used. Defaults to `GREC23J`.
- `tides` (str): Tide model to use. Defaults to `SOP`.
- `override_products_download` (bool): If `True`, existing downloaded products will be overridden.
- `override` (bool): If `True`, existing kin files will be replaced with newley generated ones

## Garpos Config
#TODO
