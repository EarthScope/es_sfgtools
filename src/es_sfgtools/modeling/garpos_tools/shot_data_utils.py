import pandas as pd
import numpy as np
import pymap3d as pm

from es_sfgtools.logging.loggers import GarposLogger as logger

def load_shot_data(file_path: str) -> pd.DataFrame:
    """
    Load shot data from a CSV file into a pandas DataFrame.
    
    Parameters:
    - file_path: Path to the CSV file containing shot data.
    
    Returns:
    - pd.DataFrame: DataFrame containing the shot data.
    """
    try:
        df = pd.read_csv(file_path)
        logger.loginfo(f"Loaded shot data from {file_path} with {len(df)} records")
        return df
    except Exception as e:
        logger.logerr(f"Error loading shot data from {file_path}: {e}")
        return pd.DataFrame()
    

def write_shot_data(df: pd.DataFrame, file_path: str) -> None:
    """
    Write shot data to a CSV file.
    
    Parameters:
    - df: DataFrame containing the shot data.
    - file_path: Path to save the CSV file.
    
    Returns:
    - None
    """
    try:
        df.to_csv(file_path, index=False)
        logger.loginfo(f"Shot data written to {file_path} with {len(df)} records")
    except Exception as e:
        logger.logerr(f"Error writing shot data to {file_path}: {e}")

def filter_wg_distance_from_center(df: pd.DataFrame, array_center_lat: float, array_center_lon: float, max_distance_m: float = 150) -> pd.DataFrame:
    """
    Remove data where waveglider is > x meters from array center. Typically used for center surveys.
    
    Parameters:
    - df: DataFrame with shotdata
    - array_center_lat: Latitude of array center
    - array_center_lon: Longitude of array center  
    - max_distance_m: Maximum distance in meters (default 150)
    
    Returns:
    - Filtered DataFrame
    """
    # Convert array center lat/lon to ECEF coordinates (assuming sea level)
    center_x, center_y, center_z = pm.geodetic2ecef(lat=array_center_lat, lon=array_center_lon, alt=0)
    
    def calc_horizontal_distance(row):
        # Calculate horizontal distance only (ignore Z/up component)
        dx = row['east0'] - center_x  
        dy = row['north0'] - center_y 
        
        horizontal_distance = np.sqrt(dx**2 + dy**2)
        return horizontal_distance
    
    # Calculate horizontal distance from center for each row
    df = df.copy()
    df['distance_from_center'] = df.apply(calc_horizontal_distance, axis=1)
    
    # Filter data
    filtered_df = df[df['distance_from_center'] <= max_distance_m].copy()
    
    logger.loginfo(f"Removed {len(df) - len(filtered_df)} records > {max_distance_m}m horizontal distance from array center")
    
    # Drop the temporary column if you don't want to keep it
    filtered_df = filtered_df.drop('distance_from_center', axis=1)
    return filtered_df

    
def filter_SNR(df, snr_min=12):
    """
    Remove data based on SNR threshold.
    GOOD: > 20
    OKAY: 12-20
    DIFFICULT(default): < 12
    
    Parameters:
    - df: DataFrame with shotdata
    - snr_min: Minimum SNR threshold (default 12)
    
    Returns:
    - Filtered DataFrame
    """
    if 'snr' not in df.columns:
        logger.logerr("SNR column not found, skipping filter")
        return df
    
    initial_count = len(df)
    # Filter based on SNR theshold greater than or equal to snr_min
    df = df[df['snr'] >= snr_min].copy()
    
    logger.loginfo(f"Removed {initial_count - len(df)} records with SNR < {snr_min}")
    return df

def filter_dbv(df, dbv_min=-36, dbv_max=-3):
    """
    Remove data based on DBV threshold.
    GOOD: -3 to -26
    OKAY: -26 to -36
    DIFFICULT (default): <-36 or >-3
    
    Parameters:
    - df: DataFrame with shotdata
    - dbv_min: Minimum DBV threshold (default -36)
    - dbv_max: Maximum DBV threshold (default -3) 
    
    Returns:
    - Filtered DataFrame
    """
    if 'dbv' not in df.columns:
        logger.logerr("DBV column not found, skipping filter")
        return df
    
    initial_count = len(df)
    df = df[(df['dbv'] >= dbv_min) & (df['dbv'] <= dbv_max)].copy()
    
    logger.loginfo(f"Removed {initial_count - len(df)} records with DBV < {dbv_max} and > {dbv_min}")
    return df

def filter_xc(df, xc_min=45):
    """
    Remove data based on XC threshold.
    GOOD: > 60
    OKAY: 45-60
    DIFFICULT (Default): < 45
    
    Parameters:
    - df: DataFrame with shotdata
    - xc_min: Minimum XC threshold (default 45)
    
    Returns:
    - Filtered DataFrame
    """
    if 'xc' not in df.columns:
        logger.logerr("XC column not found, skipping filter")
        return df
    
    initial_count = len(df)
    df = df[df['xc'] >= xc_min].copy()
    
    logger.loginfo(f"Removed {initial_count - len(df)} records with XC < {xc_min}")
    return df

def filter_acoustic_diagnostics(df, snr_min=12, dbv_min=-36, dbv_max=-3, xc_min=45):
    """
    Remove data based on acoustic diagnostics (SNR, DBV, XC)
    
    Parameters:
    - df: DataFrame with shotdata
    - snr_min: Minimum SNR threshold
    - dbv_max: Maximum DBV threshold
    - xc_min: Minimum XC threshold
    
    Quality thresholds:
    - Good: SNR>20, DBV(-3 to -26), XC>60
    - Okay: SNR(12-20), DBV(-26 to -36), XC(45-60)  
    - Difficult (default): SNR<12, DBV(<-36 or >-3), XC<45
    
    Returns:
    - Filtered DataFrame
    """

    initial_count = len(df)
    df = filter_SNR(df=df, snr_min=snr_min)
    df = filter_dbv(df=df, dbv_min=dbv_min, dbv_max=dbv_max)
    df = filter_xc(df=df, xc_min=xc_min)
    
    logger.loginfo(f"Total acoustic diagnostic filtering removed {initial_count - len(df)} records")
    return df

def good_acoustic_diagnostics(df):
    """
    Filter for "good" level acoustic diagnostics.
    
    Parameters:
    - df: DataFrame with shotdata
    
    Returns:
    - Filtered DataFrame with "good" acoustic diagnostics
    """
    return filter_acoustic_diagnostics(df, snr_min=20, dbv_min=-26, dbv_max=-3, xc_min=60)

def ok_acoustic_diagnostics(df):
    """
    Filter for "ok" level acoustic diagnostics.
    
    Parameters:
    - df: DataFrame with shotdata
    
    Returns:
    - Filtered DataFrame with "ok" level acoustic diagnostics
    """
    return filter_acoustic_diagnostics(df, snr_min=12, dbv_min=-36, dbv_max=-3, xc_min=45)

def difficult_acoustic_diagnostics(df):
    """
    Filter for "difficult" level acoustic diagnostics.
    
    Parameters:
    - df: DataFrame with shotdata
    
    Returns:
    - Filtered DataFrame with "difficult" level acoustic diagnostics
    """
    return filter_acoustic_diagnostics(df)

def filter_ping_replies(df, min_replies=3):
    """
    Require minimum number of replies for each ping (e.g., 3 replies for the 3 transponders).
    
    Parameters:
    - df: DataFrame with shotdata
    - min_replies: Minimum number of replies required (default 3)
    
    Returns:
    - Filtered DataFrame
    """
    if 'pingTime' not in df.columns:
        logger.logerr("pingTime column not found, skipping filter")
        return df
    
    # Count replies per ping time
    ping_counts = df['pingTime'].value_counts()
    
    # Get ping times that have at least min_replies
    valid_ping_times = ping_counts[ping_counts >= min_replies].index
    
    # Filter dataframe to only include pings with enough replies
    filtered_df = df[df['pingTime'].isin(valid_ping_times)].copy()
    
    removed_pings = len(ping_counts) - len(valid_ping_times)
    removed_records = len(df) - len(filtered_df)
    
    logger.loginfo(f"Removed {removed_pings} ping times with < {min_replies} replies ({removed_records} total records)")
    
    return filtered_df


# Example usage:
if __name__ == "__main__":
    # Load your CSV
    # example is NCC1
    df = pd.read_csv('/Users/terry/repos/seafloor_geodesy_notebooks/notebooks/shotdata_2024.csv')

    # Test the filters
    # filter_wg_distance_from_center(df=df, array_center_lat=41.6569428, array_center_lon=-124.93880652, max_distance_m=150)
    # filter_acoustic_diagnostics(df=df, snr_min=12, dbv_min=-36, dbv_max=-3, xc_min=45)
    # filter_minimum_replies(df=df, min_replies=3)
