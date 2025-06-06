# Configuration Documentation

## NovatelConfig
This configuration class handles Novatel-specific settings.

- `override` (bool): If `True`, existing data will be overridden.
- `n_processes` (int): Number of processes to use for processing Novatel data. Defaults to `14`.

## RinexConfig
This configuration class manages settings for RINEX processing.

- `override` (bool): If `True`, existing data will be overridden.
- `n_processes` (int): Number of processes to use for processing RINEX data. Defaults to `14`.
- `time_interval` (Optional[int]): Time interval in hours for RINEX file pagination. Higher numbers (24 is the max) mean faster processing but higher memory consumption. Defaults to `24`.
- `processing_year` (Optional[int]): Sets the specific calendar years to generate RINEX files from the tiledb array. Defaults to `-1` which includes all years.

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
- `override` (bool): If `True`, existing kin files will be replaced with newly generated ones

## Garpos Config
- `override` (bool): If `True`, will override existing garpos runs of the same run_id and prepared shot data

- `run_id` (str | int ): label for the garpos run instance

### Inversion Parameters

- `spline_degree` (int): Degree of the spline used in the inversion. Defaults to `3`.
- `log_lambda` (List[int]): Logarithmic lambda values for inversion. Defaults to `[-2]`.
- `log_gradlambda` (int): Logarithmic gradient lambda value. Defaults to `-1`.
- `mu_t` (List[float]): Temporal regularization parameter. Defaults to `[0.0]`.
- `mu_mt` (List[float]): Spatial regularization parameter. Defaults to `[0.5]`.
- `knotint0` (int): Knot interval for the first dimension. Defaults to `5`.
- `knotint1` (int): Knot interval for the second dimension. Defaults to `0`.
- `knotint2` (int): Knot interval for the third dimension. Defaults to `0`.
- `rejectcriteria` (int): Criteria for rejecting data points. Defaults to `2`.
- `inversiontype` (int): Type of inversion to perform. Defaults to `0`.
- `positionalOffset` (List[float]): Positional offset values in `[east, north, up]`. Defaults to `[0.0, 0.0, 0.0]`.
- `traveltimescale` (float): Scaling factor for travel time. Defaults to `0.0001`.
- `maxloop` (int): Maximum number of iterations for the inversion loop. Defaults to `100`.
- `convcriteria` (float): Convergence criteria for the inversion. Defaults to `0.005`.
- `deltap` (float): Perturbation parameter for inversion. Defaults to `1e-06`.
- `deltab` (float): Perturbation parameter for baseline adjustment. Defaults to `1e-06`.
- `delta_center_position.east` (int): Eastward offset for the center position. Defaults to `0`.
- `delta_center_position.north` (int): Northward offset for the center position. Defaults to `0`.
- `delta_center_position.up` (int): Upward offset for the center position. Defaults to `0`.
- `delta_center_position.east_sigma` (float): Sigma value for eastward offset. Defaults to `1.0`.
- `delta_center_position.north_sigma` (float): Sigma value for northward offset. Defaults to `1.0`.
- `delta_center_position.up_sigma` (float): Sigma value for upward offset. Defaults to `0`.
- `delta_center_position.cov_nu` (int): Covariance between north and up. Defaults to `0`.
- `delta_center_position.cov_ue` (int): Covariance between up and east. Defaults to `0`.
- `delta_center_position.cov_en` (int): Covariance between east and north. Defaults to `0`.
