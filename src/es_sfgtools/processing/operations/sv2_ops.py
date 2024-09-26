import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime, timedelta
from typing import Optional
import os
import logging
import json
import pandera as pa
from pandera.typing import DataFrame
import pymap3d as pm
from warnings import warn

from ..assets.constants import STATION_OFFSETS
from ..assets.file_schemas import SonardyneFile
from ..assets.observables import AcousticDataFrame, PositionDataFrame
from ..assets.logmodels import PingData, SimultaneousInterrogation,PositionData


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


def from_simultaneous_interrogation(
    si_set: List[SimultaneousInterrogation],
) -> Union[pd.DataFrame, None]:
    """
    Generate a validated AcousticDataFrame from a list of SimultaneousInterrogation instances.
    """
    si_data_dicts: List[dict] = []
    for si_data in si_set:
        ping_data_dict = dict(si_data.pingData)
        for response in si_data.responses:
            response_dict = dict(response)
            si_data_dicts.append({**response_dict, **ping_data_dict})

    dataframe_pre = pd.DataFrame(si_data_dicts)
    column_order = [
        "transponderID",
        "triggerTime",
        "pingTime",
        "returnTime",
        "twoWayTravelTime",
        "decibalVoltage",
        "correlationScore",
    ]
    dataframe_pre = dataframe_pre[column_order]
    dataframe_pre["triggerTime"] = dataframe_pre["triggerTime"].apply(
        lambda x: pd.Timestamp(x)
    )

    return dataframe_pre


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
                                             PingTime      ReturnTime       TwoWayTravelTime  DecibalVoltage  CorrelationScore
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

    ping_pattern = re.compile("PING - Offset")
    si_pattern = re.compile(">SI:")  # TODO take this out for now

    # get transponder offsets from file:
    # 2003,327353,1527706553,2018/05/30 18:55:53.519 >CS:2010,TAT200
    # offset_dict = {"2010": 200}
    tat_pattern = re.compile(">CS:")

    simultaneous_interrogation_set: List[SimultaneousInterrogation] = []
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
                offset_dict: Union[Dict[str, float], None] = get_transponder_offsets(
                    line
                )
                if offset_dict is None:
                    continue
                main_offset_dict.update(offset_dict)
                pass

            if ping_pattern.search(line):
                try:
                    pingData: PingData = PingData.from_line(line)
                    found_ping = True
                except ValidationError as e:
                    response = f"Error parsing into PingData from line {line_number} in {source}\n "
                    response += f"Line: {line}"
                    logger.error(response)
                    found_ping = False
                    break

                while True and found_ping:
                    next_line = sonardyne_file.readline()
                    line_number += 1
                    if si_pattern.search(next_line):
                        try:
                            si_data: SimultaneousInterrogation = (
                                SimultaneousInterrogation.from_line(next_line, pingData)
                            )
                            # Apply the time offsets [ms] to the transponder data
                            si_data.apply_offsets(main_offset_dict)
                            simultaneous_interrogation_set.append(si_data)

                        except ValidationError as e:
                            response = f"Error parsing into SimultaneousInterrogation from line {line_number} in {source}\n "
                            response += f"Line: {next_line}"
                            logger.error(response)
                            pass
                        break
                    elif ping_pattern.search(next_line) or next_line == "":
                        break

                found_ping = False

    # Check if any Simultaneous Interrogation data was found
    if not simultaneous_interrogation_set:
        response = f"Acoustic: No Simultaneous Interrogation data in FILE {source}"
        logger.error(response)
        return None

    acoustic_df: pd.DataFrame = from_simultaneous_interrogation(
        simultaneous_interrogation_set
    )

    unique_transponders: list = list(
        acoustic_df.reset_index()["TransponderID"].unique()
    )
    shot_count: int = int(acoustic_df.shape[0] / len(unique_transponders))

    log_response = f"Acoustic Parser: {acoustic_df.shape[0]} shots from FILE {source.local_path} | {len(unique_transponders)} transponders | {shot_count} shots per transponder"
    logger.info(log_response)
    acoustic_df.triggerTime = acoustic_df.triggerTime.dt.tz_localize("UTC")
    # acoustic_df.ReturnTime = acoustic_df.ReturnTime.dt.tz_localize("UTC")
    return AcousticDataFrame.validate(acoustic_df, lazy=True)


def novatel_to_positiondf(source:NovatelFile) -> DataFrame[PositionDataFrame]:
    if not os.path.exists(source.local_path):
        raise FileNotFoundError(
            f"IMU Parsing: The file {source.local_path} does not exist."
        )

    inspvaa_pattern = re.compile("#INSPVAA,")
    data_list = []
    line_number = 0
    with open(source.local_path) as inspva_file:
        while True:
            try:
                line = inspva_file.readline()
                line_number += 1
                if not line:
                    break
                if re.search(inspvaa_pattern, line):
                    try:
                        position_data = PositionData.from_sv2(line)
                        data_list.append(position_data.model_dump())
                    else:
                        error_msg = f"IMU Parsing: An error occurred while parsing INVSPA data from FILE {source} at LINE {line_number} \n"
                        error_msg += f"Error: {line}"
                        logger.error(error_msg)
                        pass
            except UnicodeDecodeError as e:
                error_msg = f"Position Parsing:{e} | Error parsing FILE {source.local_path} at LINE {line_number}"
                logger.error(error_msg)
                pass
    df = pd.DataFrame(data_list)
    return PositionDataFrame.validate(df, lazy=True)

def dev_merge_to_shotdata(acoustic: DataFrame[AcousticDataFrame], imu: DataFrame[IMUDataFrame], gnss: DataFrame[PositionDataFrame]) -> DataFrame[ObservationData]:
    """
    Merge acoustic, imu, and gnss data to create observation data.
    Args:
        acoustic (DataFrame[AcousticDataFrame]): Acoustic data frame.
        imu (DataFrame[IMUDataFrame]): IMU data frame.
        gnss (DataFrame[PositionDataFrame]): GNSS data frame.
    Returns:
        DataFrame[ObservationData]: Merged observation data frame.
    """

    acoustic = acoustic.reset_index()
    acoustic.columns = acoustic.columns.str.lower()
    imu.columns = imu.columns.str.lower()
    gnss.columns = gnss.columns.str.lower()

    acoustic["triggertime"] = pd.to_datetime(acoustic["triggertime"])
    acoustic["time"] = acoustic["triggertime"]
    gnss["time"] = pd.to_datetime(gnss["time"])
    imu["time"] = pd.to_datetime(imu["time"])

    # sort
    acoustic.sort_values("triggertime",inplace=True)
    gnss.sort_values("time",inplace=True)
    imu.sort_values("time",inplace=True)

    ping_keys_gnss = {
        'ant_e0':"x",
        'ant_n0':"y",
        'ant_u0':"z",
        'latitude':"latitude",
        'longitude':"longitude",
    }
    ping_keys_imu = {
        'roll0':"roll",
        'pitch0':"pitch",
        'head0':"azimuth"
    }

    return_keys_gnss = {
        'ant_e1':"x",
        'ant_n1':"y",
        'ant_u1':"z"
    }
    return_keys_imu = {
        'roll1':"roll",
        'pitch1':"pitch",
        'head1':"azimuth"
    }


    def interp(x_new,x,y):
        # return CubicSpline(x,y)(x_new)
        return np.interp(x_new,x,y)
    
    # Convert pingtime and return time to datetime (units are seconds of day)
    acoustic_day_start = acoustic["time"].apply(lambda x: (x.replace(hour=0, minute=0, second=0,microsecond=0)).timestamp())    
    acoustic_return_dt = acoustic["returntime"].to_numpy() + acoustic_day_start
    acoustic_ping_dt = acoustic["pingtime"].to_numpy() + acoustic_day_start

    output_df = acoustic.copy()
    for field,target in ping_keys_gnss.items():
        output_df[field] = interp(acoustic_ping_dt.to_numpy(),gnss["time"].apply(lambda x:x.timestamp()).to_numpy(),gnss[target].to_numpy())
    for field,target in ping_keys_imu.items():
        output_df[field] = interp(acoustic_ping_dt.to_numpy(),imu["time"].apply(lambda x:x.timestamp()).to_numpy(),imu[target].to_numpy())

    for field,target in return_keys_gnss.items():
        output_df[field] = interp(acoustic_return_dt.to_numpy(),gnss["time"].apply(lambda x:x.timestamp()).to_numpy(),gnss[target].to_numpy())
    for field,target in return_keys_imu.items():
        output_df[field] = interp(acoustic_return_dt.to_numpy(),imu["time"].apply(lambda x:x.timestamp()).to_numpy(),imu[target].to_numpy())

    output_df.rename(
        columns={
            "transponderid": "MT",
            "twowaytraveltime": "TT",
            "pingtime": "ST",
            "returntime": "RT",
        },
        inplace=True,
    )

    # # Step 6. Get Lat/Lon of the glider at trigger time
    # pos_trigger = position_predictor.predict(output_df["triggertime"].apply(lambda x: x.timestamp()).values.reshape(-1,1))
    # lat_array,lon_array = pos_trigger[:,3],pos_trigger[:,4]

    output_df = output_df.loc[:, ~output_df.columns.str.contains("^unnamed")].drop(columns=["time"]).dropna().reset_index(drop=True)
    output_df["SET"] = "S01"
    output_df["LN"] = "L01"
    return ObservationData.validate(output_df,lazy=True)

