import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime, timedelta
from typing import Optional,Dict,Union,List,Annotated
import os
import logging
import json
import pandera as pa
from pandera.typing import DataFrame
import pymap3d as pm
from warnings import warn
import numpy as np
import re
from pathlib import Path

from ..assets.constants import STATION_OFFSETS, TRIGGER_DELAY_SV2
from ..assets.file_schemas import SonardyneFile,NovatelFile,AssetType,AssetEntry
from ..assets.observables import AcousticDataFrame, PositionDataFrame,ShotDataFrame
from ..assets.logmodels import PositionData,RangeData,BestGNSSPOSDATA,get_traveltime,check_sequence_overlap,datetime_to_sod

logger = logging.getLogger(__name__)

def get_transponder_offsets(line: str) -> Dict[str, float]:
    """
    Extract the transponder offsets from a line of text.
    offsets are in milliseconds.

    Args:
        line (str): A line of text containing transponder offsets.

    Returns:
        Dict[str,float]: A dictionary

    Example:
        >>> line = "2003,327353,1527706553,2018/05/30 18:55:53.519 >CS:2010,TAT200,..."
        >>> AcousticDataFrame.get_transponder_offsets(line)
        {"2010":200}
    """
    if "NO_REPLY" in line:
        return None

    offset_dict: Dict[str, float] = STATION_OFFSETS.copy()
    # "2003,327353,1527706553,2018/05/30 18:55:53.519 >CS:2010,TAT200,..." -> ["2010","TAT200",...]
    parsed_line = line.split(">CS:")[1].split(",")
    transponder_id, offset = parsed_line[:2]
    # "TAT200" -> 200
    try:
        offset = float(offset.replace("TAT", ""))
    except ValueError:
        offset = 0.0
    offset_dict[transponder_id] = offset
    return offset_dict

def range_data_to_acousticdf(df:pd.DataFrame) -> pd.DataFrame:
    tt_array:np.ndarray = get_traveltime(df["range"].to_numpy(),df["tat"].to_numpy(),triggerDelay=TRIGGER_DELAY_SV2)
    df["tt"] = tt_array
    pingtime_array = df["time"] + timedelta(seconds=TRIGGER_DELAY_SV2)
    returntime_array = pingtime_array + pd.to_timedelta(tt_array,unit="s")
    df["pingTime"] = datetime_to_sod(pingtime_array)
    df["returnTime"] = datetime_to_sod(returntime_array)
    df = df.rename(columns={"time":"triggerTime"})
    df.triggerTime = df.triggerTime.dt.tz_localize("UTC")
    return df

def sonardyne_to_acousticdf(source: SonardyneFile) -> DataFrame[AcousticDataFrame]:
    """
    Read data from a file and return a validated dataframe.

    Args:
        file (str): Path to the file containing the acoustic data.
        mode (WaveGlider, optional): The mode of the WaveGlider. Defaults to WaveGlider.SV2.
        source (str, optional): The source of the data. Defaults to None.

    Raises:
        FileNotFoundError: If the file specified by `file` does not exist.

    Returns:
        Union[pd.DataFrame, pa.errors.SchemaErrors]:
            DataFrame containing the acoustic data if successfully validated,
            otherwise returns SchemaErrors.

    Example:
        >>> file = "tests/resources/test_sonardyne_raw.txt"
        >>> df = AcousticDataFrame.from_file(file)
        INFO:root:Processed 4.0 shots from "tests/resources/test_sonardyne_raw.txt"
        >>> print(df.head())
                                             PingTime      ReturnTime       tt  dbv  CorrelationScore
        TriggerTime          TransponderID
        2018-05-30 12:55:59  5209           58268.538890  58268.538942          4.447123             -21                85
                             5210           58268.538890  58268.538928          3.291827               0                85
                             5211           58268.538890  58268.538953          5.446601             -24                75
        2018-05-30 12:56:14  5209           58268.539064  58268.539115          4.452981             -18                75
                             5210           58268.539064  58268.539102          3.289823               0                90
                             5211           58268.539064  58268.539127          5.442233             -24                65
        2018-05-30 12:56:29  5209           58268.539237  58268.539289          4.458378             -15                65
                             5210           58268.539237  58268.539275          3.286848               0                85
                             5211           58268.539237  58268.539300          5.437492             -24                70
        2018-05-30 12:56:44  5209           58268.539411  58268.539463          4.463116             -18                85
                             5210           58268.539411  58268.539449          3.285197               0                90
                             5211           58268.539411  58268.539474          5.433356             -24                70

    """
    if not os.path.exists(source.local_path):
        response = f"File {source.local_path} not found"
        logger.error(response)
        raise FileNotFoundError(response)

    si_pattern = re.compile(">SI:")  # TODO take this out for now

    # get transponder offsets from file:
    # 2003,327353,1527706553,2018/05/30 18:55:53.519 >CS:2010,TAT200
    # offset_dict = {"2010": 200}
    tat_pattern = re.compile(">CS:")

    simultaneous_interrogation_set = []
    line_number = 0
    # Dictionary to store transponder time offsets
    main_offset_dict = STATION_OFFSETS.copy()
    with open(source.local_path) as sonardyne_file:
        while True:
            try:
                line = sonardyne_file.readline()
                line_number += 1
                if not line:
                    break
                found_ping = False
            except UnicodeDecodeError as e:
                error_msg = f"Acoustic Parsing:{e} | Error parsing FILE {source} at LINE {line_number}"
                logger.error(error_msg)
                pass

            # Update transponder time offsets if found
            if tat_pattern.search(line):
                if (offset_dict:=get_transponder_offsets(line)) is not None:
                    main_offset_dict.update(offset_dict)
         
            if si_pattern.search(line):
                try:
                    range_data: List[RangeData] = RangeData.from_sv2(line,main_offset_dict)
                    simultaneous_interrogation_set.extend(range_data)
                except ValidationError as e:
                    response = f"Error parsing into SimultaneousInterrogation from line {line_number} in {source}\n "
                    response += f"Line: {next_line}"
                    logger.error(response)
                    pass
                

    # Check if any Simultaneous Interrogation data was found
    if not simultaneous_interrogation_set:
        response = f"Acoustic: No Simultaneous Interrogation data in FILE {source}"
        logger.error(response)
        return None

    df = pd.DataFrame([x.model_dump() for x in simultaneous_interrogation_set]) 

    acoustic_df = range_data_to_acousticdf(df)

    unique_transponders: list = list(
        acoustic_df.reset_index()["transponderID"].unique()
    )
    shot_count: int = int(acoustic_df.shape[0] / len(unique_transponders))

    log_response = f"Acoustic Parser: {acoustic_df.shape[0]} shots from FILE {source.local_path} | {len(unique_transponders)} transponders | {shot_count} shots per transponder"
    logger.info(log_response)
    
    acoustic_df = check_sequence_overlap(acoustic_df)
    return AcousticDataFrame.validate(acoustic_df, lazy=True)

def novatel_to_positiondf(source:NovatelFile) -> DataFrame[PositionDataFrame]:
    if not os.path.exists(source.local_path):
        raise FileNotFoundError(
            f"IMU Parsing: The file {source.local_path} does not exist."
        )

    inspvaa_pattern = re.compile("#INSPVAA,")
    bestgnss_pattern = re.compile("#BESTGNSSPOSA,")

    data_list = []
    line_number = 0
    gnssMeta = BestGNSSPOSDATA()
    with open(source.local_path) as inspva_file:
        while True:
            try:
                line = inspva_file.readline()
                line_number += 1
                if not line:
                    break
                if re.search(bestgnss_pattern, line):
                
                    gnssMeta = BestGNSSPOSDATA.from_sv2(line)
           
                if re.search(inspvaa_pattern, line):
                    try:
                        position_data = PositionData.from_sv2_inspvaa(line)
                        # update gnss metadata uncertainty
                        position_data.sdx,position_data.sdy,position_data.sdz = gnssMeta.sdx,gnssMeta.sdy,gnssMeta.sdz
                        data_list.append(position_data.model_dump())
                    except Exception as e:
                        error_msg = f"IMU Parsing: An error occurred while parsing INVSPA data from FILE {source} at LINE {line_number} \n"
                        error_msg += f"Error: {line}"
                        logger.error(error_msg)
                        pass
            except UnicodeDecodeError as e:
                error_msg = f"Position Parsing:{e} | Error parsing FILE {source.local_path} at LINE {line_number}"
                logger.error(error_msg)
                pass
    df = pd.DataFrame(data_list).rename(columns={
        "sdx":"east_std",
        "sdy":"north_std",
        "sdz":"up_std",
        })
    return PositionDataFrame.validate(df, lazy=True)

def dev_merge_to_shotdata(acoustic: DataFrame[AcousticDataFrame], position:DataFrame[PositionDataFrame],**kwargs) -> DataFrame[ShotDataFrame]:
    """
    Merge acoustic, imu, and gnss data to create observation data.
    Args:
        acoustic (DataFrame[AcousticDataFrame]): Acoustic data frame.
        imu (DataFrame[IMUDataFrame]): IMU data frame.
        gnss (DataFrame[PositionDataFrame]): GNSS data frame.
    Returns:
        DataFrame[ObservationData]: Merged observation data frame.
    """


    acoustic["time"] = acoustic["triggerTime"]
    
    acoustic_min_time = acoustic["time"].min()
    acoustic_max_time = acoustic["time"].max()
    position_min_time = position["time"].min()
    position_max_time = position["time"].max()
    min_time = max(acoustic_min_time,position_min_time)
    max_time = min(acoustic_max_time,position_max_time)
    acoustic = acoustic[(acoustic["time"] >= min_time) & (acoustic["time"] <= max_time)]
    position = position[(position["time"] >= min_time) & (position["time"] <= max_time)]

    if acoustic.empty or position.empty:
        logger.error("No data found in the time range")
        raise ValueError("No data found in the time range")

    # sort
    acoustic.sort_values("triggerTime",inplace=True)
    position.sort_values("time",inplace=True)

    ping_keys_position = {
        'ant_e0':"east",
        'ant_n0':"north",
        'ant_u0':"up",
        'latitude':"latitude",
        'longitude':"longitude",
        'roll0': "roll",
        'pitch0': "pitch",
        'head0': "head"
    }
 

    return_keys_position = {
        'ant_e1':"east",
        'ant_n1':"north",
        'ant_u1':"up",
        'roll1': "roll",
        'pitch1': "pitch",
        'head1': "head"
    }

    def interp(x_new,x,y):
        # return CubicSpline(x,y)(x_new)
        return np.interp(x_new,x,y)
    
    # Convert pingtime and return time to datetime (units are seconds of day)
    acoustic_day_start = acoustic["time"].apply(lambda x: (x.replace(hour=0, minute=0, second=0,microsecond=0)).timestamp())    
    acoustic_return_dt = acoustic["returnTime"].to_numpy() + acoustic_day_start
    acoustic_ping_dt = acoustic["pingTime"].to_numpy() + acoustic_day_start

    output_df = acoustic.copy()
    for field,target in ping_keys_position.items():
        output_df[field] = interp(acoustic_ping_dt.to_numpy(),position["time"].apply(lambda x:x.timestamp()).to_numpy(),position[target].to_numpy())

    for field,target in return_keys_position.items():
        output_df[field] = interp(acoustic_return_dt.to_numpy(),position["time"].apply(lambda x:x.timestamp()).to_numpy(),position[target].to_numpy())

    output_df.rename(
        columns={
            "transponderID": "MT",
            "tt": "TT",
            "pingTime": "ST",
            "returnTime": "RT",
        },
        inplace=True,
    )

    # # Step 6. Get Lat/Lon of the glider at trigger time
    # pos_trigger = position_predictor.predict(output_df["triggertime"].apply(lambda x: x.timestamp()).values.reshape(-1,1))
    # lat_array,lon_array = pos_trigger[:,3],pos_trigger[:,4]

    output_df = output_df.loc[:, ~output_df.columns.str.contains("^unnamed")].drop(columns=["time"]).dropna().reset_index(drop=True)
    output_df["SET"] = "S01"
    output_df["LN"] = "L01"
    return ShotDataFrame.validate(output_df,lazy=True)

# def multiasset_to_shotdata(acoustic_assets:List[MultiAssetEntry],
    #                        position_assets:List[MultiAssetEntry],
    #                        working_dir:Path,
    #                        **kwargs) -> List[MultiAssetEntry]:
    # """
    # Merge acoustic, imu, and gnss data to create observation data.
    # Args:
    #     acoustic (DataFrame[AcousticDataFrame]): Acoustic data frame.
    #     imu (DataFrame[IMUDataFrame]): IMU data frame.
    #     gnss (DataFrame[PositionDataFrame]): GNSS data frame.
    # Returns:
    #     DataFrame[ObservationData]: Merged observation data frame.
    # """
    # assert all([x.type == AssetType.ACOUSTIC for x in acoustic_assets]), "All assets must be acoustic"
    # assert all([x.type == AssetType.POSITION for x in position_assets]), "All assets must be position"

    # acoustic_doy_map = {}
    # position_doy_map = {}
    # merged_doy_map = {}
    # for asset in acoustic_assets:
    #     doy = asset.timestamp_data_start.timetuple().tm_yday
    #     acoustic_doy_map[doy] = asset
    # for asset in position_assets:
    #     doy = asset.timestamp_data_start.timetuple().tm_yday
    #     position_doy_map[doy] = asset

    # output: List[MultiAssetEntry] = []
    # for doy,acoustic_asset in acoustic_doy_map.items():
    #     if doy in position_doy_map:
    #         position_df = pd.read_csv(position_doy_map[doy].local_path)
    #         acoustic_df = pd.read_csv(acoustic_asset.local_path)
    #         try:
    #             timestamp_data_start = None
    #             timestamp_data_end = None
    #             shot_df:DataFrame[ShotDataFrame] = dev_merge_to_shotdata(acoustic_df,position_df)
    #             for col in shot_df.columns:
    #                 if pd.api.types.is_datetime64_any_dtype(shot_df[col]):
    #                     timestamp_data_start = shot_df[col].min()
    #                     timestamp_data_end = shot_df[col].max()
    #             local_path = working_dir / f"{acoustic_asset.network}_{acoustic_assets.station}_{acoustic_assets.survey}_shot_data_{doy}.csv"
    #             shot_df.to_csv(local_path,index=False)
    #             new_multi_asset = MultiAssetEntry(
    #                 local_path = str(local_path),
    #                 type = AssetType.SHOTDATA,
    #                 network = acoustic_asset.network,
    #                 station = acoustic_asset.station,
    #                 survey = acoustic_asset.survey,
    #                 timestamp_data_start = timestamp_data_start,
    #                 timestamp_data_end = timestamp_data_end,
    #                 parent_id = f"{acoustic_asset.id},{position_doy_map[doy].id}"
    #             )
    #             output.append(new_multi_asset)

    #         except Exception as e:
    #             logger.error(f"Error merging acoustic and position data for DOY {doy} {e}")
    #             continue
    # return output