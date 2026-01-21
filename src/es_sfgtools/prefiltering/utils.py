from datetime import datetime, timezone
from typing import Optional,Union
import numpy as np
import pandas as pd
import pymap3d as pm

from es_sfgtools.data_models.metadata import Site,SurveyType,classify_survey_type
from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.tiledb_tools.tiledb_schemas import TDBKinPositionArray
from es_sfgtools.utils.model_update import validate_and_merge_config

from .schemas import  FilterLevel ,FilterConfig

def filter_shotdata(
    survey_type: Union[str, SurveyType],
    site: Site,
    shot_data: pd.DataFrame,
    kinPostionTDBUri: str,
    start_time: datetime,
    end_time: datetime,
    base_config: Optional[FilterConfig] = None,
    custom_filters: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Filter the shot data based on the specified acoustic level and minimum ping replies.

    Parameters
    ----------
    survey_type : str
        The type of survey.
    site : Site
        The site metadata.
    shot_data : pd.DataFrame
        The shot data to filter.
    kinPostionTDBUri : str
        The URI of the kinematic position TileDB array.
    start_time : datetime
        The start time of the survey.
    end_time : datetime
        The end time of the survey.
    custom_filters : dict, optional
        Custom filters to apply.

    Returns
    -------
    pd.DataFrame
        The filtered shot data.
    """

    if base_config is None:
        filter_config = FilterConfig()

    initial_count = len(shot_data)
    new_shot_data_df = shot_data.copy()

    # Convert start_time and end_time to Unix timestamps in UTC
    start_unix = start_time.replace(tzinfo=timezone.utc).timestamp()
    end_unix = end_time.replace(tzinfo=timezone.utc).timestamp()

    # Filter by time
    new_shot_data_df = new_shot_data_df[
        (new_shot_data_df["pingTime"] >= start_unix)
        & (new_shot_data_df["pingTime"] <= end_unix)
    ]

    if custom_filters:
        filter_config = validate_and_merge_config(
            base_class=filter_config, override_config=custom_filters
        )
        logger.loginfo(f"Using custom filter configuration: {filter_config}")

    """
    Apply acoustic diagnostics filtering. This is based on the SNR, DBV, and XC thresholds.
    """
    acoustic_config = filter_config.acoustic_filters
    if acoustic_config.enabled:
        level = acoustic_config.level
        match level:
            case FilterLevel.GOOD:
                new_shot_data_df = good_acoustic_diagnostics(new_shot_data_df)
            case FilterLevel.OK:
                new_shot_data_df = ok_acoustic_diagnostics(new_shot_data_df)
            case FilterLevel.DIFFICULT:
                new_shot_data_df = difficult_acoustic_diagnostics(new_shot_data_df)
            case _:
                logger.loginfo("No acoustic filtering applied, using original shot data")

    """
    Apply ping replies filtering. This is based on the minimum number of replies.
    """
    ping_replies_config = filter_config.ping_replies
    if ping_replies_config.enabled:
        min_replies = ping_replies_config.min_replies
        new_shot_data_df = filter_ping_replies(
            new_shot_data_df, min_replies=min_replies
        )

    """
    Apply max distance from center filtering. This is typically used for center surveys.
    """
    if survey_type == SurveyType.CENTER:
        max_distance = filter_config.max_distance_from_center
        if max_distance.enabled:
            new_shot_data_df = filter_wg_distance_from_center(
                df=new_shot_data_df,
                array_center_lat=site.arrayCenter.latitude,
                array_center_lon=site.arrayCenter.longitude,
                max_distance_m=max_distance.max_distance_m,
            )
    """
    Apply PRIDE residuals filtering. This removes shots with high PRIDE residuals.
    """
    if filter_config.pride_residuals.enabled:
        new_shot_data_df = filter_pride_residuals(
            df=new_shot_data_df,
            kinPostionTDBUri=kinPostionTDBUri,
            start_time=start_time,
            end_time=end_time,
            max_wrms=filter_config.pride_residuals.max_residual_mm,
        )

    filtered_count = len(new_shot_data_df)
    logger.loginfo(
        f"Filtered {initial_count - filtered_count} records from shot data based on filtering criteria: {filter_config}"
    )
    logger.loginfo(f"Remaining shot data records: {filtered_count}")
    return new_shot_data_df


def filter_wg_distance_from_center(
    df: pd.DataFrame,
    array_center_lat: float,
    array_center_lon: float,
    max_distance_m: float = 150,
) -> pd.DataFrame:
    """
    Remove data where waveglider is > x meters from array center. Typically used for center surveys.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with shotdata.
    array_center_lat : float
        Latitude of the array center.
    array_center_lon : float
        Longitude of the array center.
    max_distance_m : float, optional
        Maximum distance from center in meters, by default 150.
    
    Returns
    -------
    pd.DataFrame
        Filtered DataFrame.
    """
    # Convert array center lat/lon to ECEF coordinates (assuming sea level)
    center_x, center_y, center_z = pm.geodetic2ecef(
        lat=array_center_lat, lon=array_center_lon, alt=0
    )

    def calc_horizontal_distance(row):
        # Calculate horizontal distance only (ignore Z/up component)
        dx = row["east0"] - center_x
        dy = row["north0"] - center_y

        horizontal_distance = np.sqrt(dx**2 + dy**2)
        return horizontal_distance

    # Calculate horizontal distance from center for each row
    df = df.copy()
    df["distance_from_center"] = df.apply(calc_horizontal_distance, axis=1)

    # Filter data
    filtered_df = df[df["distance_from_center"] <= max_distance_m].copy()

    logger.loginfo(
        f"Removed {len(df) - len(filtered_df)} records > {max_distance_m}m horizontal distance from array center"
    )

    # Drop the temporary column if you don't want to keep it
    filtered_df = filtered_df.drop("distance_from_center", axis=1)
    return filtered_df


def filter_SNR(df, snr_min=12):
    """
    Remove data based on SNR threshold.
    GOOD: > 20
    OKAY: 12-20
    DIFFICULT(default): < 12

    :param df: DataFrame with shotdata.
    :type df: pd.DataFrame
    :param snr_min: Minimum SNR threshold. Defaults to 12.
    :type snr_min: int, optional
    :return: Filtered DataFrame.
    :rtype: pd.DataFrame
    """
    if "snr" not in df.columns:
        logger.logerr("SNR column not found, skipping filter")
        return df

    initial_count = len(df)
    # Filter based on SNR theshold greater than or equal to snr_min
    df = df[df["snr"] >= snr_min].copy()

    logger.loginfo(f"Removed {initial_count - len(df)} records with SNR < {snr_min}")
    return df


def filter_dbv(df, dbv_min=-36, dbv_max=-3):
    """
    Remove data based on DBV threshold.
    GOOD: -3 to -26
    OKAY: -26 to -36
    DIFFICULT (default): <-36 or >-3

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with shotdata.
    dbv_min : int, default -36
        Minimum DBV threshold.
    dbv_max : int, default -3
        Maximum DBV threshold.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame.
    """
    if "dbv" not in df.columns:
        logger.logerr("DBV column not found, skipping filter")
        return df

    initial_count = len(df)
    df = df[(df["dbv"] >= dbv_min) & (df["dbv"] <= dbv_max)].copy()

    logger.loginfo(
        f"Removed {initial_count - len(df)} records with DBV < {dbv_min} or > {dbv_max}"
    )
    return df


def filter_xc(df, xc_min=45):
    """
    Remove data based on XC threshold.
    GOOD: > 60
    OKAY: 45-60
    DIFFICULT (Default): < 45

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with shotdata.
    xc_min : int, default 45
        Minimum XC threshold.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame.
    """
    if "xc" not in df.columns:
        logger.logerr("XC column not found, skipping filter")
        return df

    initial_count = len(df)
    df = df[df["xc"] >= xc_min].copy()

    logger.loginfo(f"Removed {initial_count - len(df)} records with XC < {xc_min}")
    return df


def filter_acoustic_diagnostics(df, snr_min=12, dbv_min=-36, dbv_max=-3, xc_min=45):
    """
    Remove data based on acoustic diagnostics (SNR, DBV, XC)

    Quality thresholds:
    - Good: SNR>20, DBV(-3 to -26), XC>60
    - Okay: SNR(12-20), DBV(-26 to -36), XC(45-60)
    - Difficult (default): SNR<12, DBV(<-36 or >-3), XC<45

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with shotdata.
    snr_min : int, optional
        Minimum SNR threshold.
    dbv_min : int, optional
        Minimum DBV threshold.
    dbv_max : int, optional
        Maximum DBV threshold.
    xc_min : int, optional
        Minimum XC threshold.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame.
    """

    initial_count = len(df)
    df = filter_SNR(df=df, snr_min=snr_min)
    df = filter_dbv(df=df, dbv_min=dbv_min, dbv_max=dbv_max)
    df = filter_xc(df=df, xc_min=xc_min)

    logger.loginfo(
        f"Total acoustic diagnostic filtering removed {initial_count - len(df)} records"
    )
    return df


def good_acoustic_diagnostics(df):
    """
    Filter for "good" level acoustic diagnostics.

    :param df: DataFrame with shotdata.
    :type df: pd.DataFrame
    :return: Filtered DataFrame with "good" acoustic diagnostics.
    :rtype: pd.DataFrame
    """
    return filter_acoustic_diagnostics(
        df, snr_min=20, dbv_min=-26, dbv_max=-3, xc_min=60
    )


def ok_acoustic_diagnostics(df):
    """
    Filter for "ok" level acoustic diagnostics.

    :param df: DataFrame with shotdata.
    :type df: pd.DataFrame
    :return: Filtered DataFrame with "ok" level acoustic diagnostics.
    :rtype: pd.DataFrame
    """
    return filter_acoustic_diagnostics(
        df, snr_min=12, dbv_min=-36, dbv_max=-3, xc_min=45
    )


def difficult_acoustic_diagnostics(df):
    """
    Filter for "difficult" level acoustic diagnostics.

    :param df: DataFrame with shotdata.
    :type df: pd.DataFrame
    :return: Filtered DataFrame with "difficult" level acoustic diagnostics.
    :rtype: pd.DataFrame
    """
    return filter_acoustic_diagnostics(df)


def filter_ping_replies(df, min_replies=3):
    """
    Require minimum number of replies for each ping (e.g., 3 replies for the 3 transponders).

    :param df: DataFrame with shotdata.
    :type df: pd.DataFrame
    :param min_replies: Minimum number of replies required. Defaults to 3.
    :type min_replies: int, optional
    :return: Filtered DataFrame.
    :rtype: pd.DataFrame
    """
    if "pingTime" not in df.columns:
        logger.logerr("pingTime column not found, skipping filter")
        return df

    # Count replies per ping time
    ping_counts = df["pingTime"].value_counts()

    # Get ping times that have at least min_replies
    valid_ping_times = ping_counts[ping_counts >= min_replies].index

    # Filter dataframe to only include pings with enough replies
    filtered_df = df[df["pingTime"].isin(valid_ping_times)].copy()

    removed_pings = len(ping_counts) - len(valid_ping_times)
    removed_records = len(df) - len(filtered_df)

    logger.loginfo(
        f"Removed {removed_pings} ping times with < {min_replies} replies ({removed_records} total records)"
    )

    return filtered_df


def filter_pride_residuals(
    df, kinPostionTDBUri: str, start_time: datetime, end_time: datetime, max_wrms=15
):
    """
    Filter Pride PPP data based on wrms residuals in position tileDB array.

    :param df: DataFrame with shotdata.
    :type df: pd.DataFrame
    :param kinPostionTDBUri: URI for the KinPosition tileDB array.
    :type kinPostionTDBUri: str
    :param start_time: Start time for filtering.
    :type start_time: datetime
    :param end_time: End time for filtering.
    :type end_time: datetime
    :param max_wrms: Maximum WRMS threshold in millimeters. Defaults to 15.
    :type max_wrms: int, optional
    :return: Filtered DataFrame.
    :rtype: pd.DataFrame
    """

    # Convert tileDB array to dataframe
    pride_data = TDBKinPositionArray(kinPostionTDBUri)
    ppp_df = pride_data.read_df(start=start_time, end=end_time)
    if ppp_df.empty:
        logger.logerr("No Pride PPP data found, skipping residual filter")
        return df

    # Check if wrms column exists
    if "wrms" not in ppp_df.columns:
        logger.logerr("WRMS column not found in Pride data, skipping residual filter")
        return df

    # Filter Pride data for high WRMS values
    high_wrms_times = ppp_df[ppp_df["wrms"] > max_wrms]["time"].tolist()

    if not high_wrms_times:
        logger.loginfo(f"No Pride PPP data exceeds WRMS threshold of {max_wrms}mm")
        return df

    # Convert Pride PPP datetime to Unix timestamp to match pingTime format
    high_wrms_unix_times = []
    for bad_time in high_wrms_times:
        if pd.isna(bad_time):
            continue
        # Convert datetime to Unix timestamp
        unix_timestamp = bad_time.timestamp()
        high_wrms_unix_times.append(unix_timestamp)

    # Create exclusion time ranges using Unix timestamps
    exclusion_ranges = []
    time_buffer_seconds = 1  # 1 second buffer before/after

    for bad_unix_time in high_wrms_unix_times:
        exclusion_ranges.append(
            {
                "start": bad_unix_time - time_buffer_seconds,
                "end": bad_unix_time + time_buffer_seconds,
            }
        )

    # Filter shot data - remove shots that fall within any exclusion range
    initial_count = len(df)
    mask = pd.Series(True, index=df.index)  # Start with all True

    for time_range in exclusion_ranges:
        # Mark shots within this exclusion range as False (pingTime is Unix timestamp)
        in_range = (df["pingTime"] >= time_range["start"]) & (
            df["pingTime"] <= time_range["end"]
        )
        mask = mask & ~in_range  # Remove shots in this range

    filtered_df = df[mask].copy()

    removed_count = initial_count - len(filtered_df)
    logger.loginfo(
        f"Removed {removed_count} shot records due to high WRMS (>{max_wrms}mm) in Pride PPP data"
    )
    logger.loginfo(
        f"Used {len(exclusion_ranges)} time exclusion ranges with ±1s buffer"
    )

    return filtered_df  # Return filtered_df instead of original df
