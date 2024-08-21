"""
Author: Franklyn Dunbar
Date: 2024-03-07
Email: franklyn.dunbar@earthscope.org
"""

import pandas as pd
from pydantic import BaseModel, Field,ValidationError
import pandera as pa
from pandera.typing import Series,DataFrame
from typing import List, Dict,Union,Optional
from enum import Enum
from datetime import datetime, timezone, timedelta
import julian
import os 
import re
import julian
import logging
import json
import pdb
from ..schemas.files import SonardyneFile,DFPO00RawFile,QCPinFile
from ..schemas.observables import AcousticDataFrame
logger = logging.getLogger(os.path.basename(__file__))

GNSS_START_TIME = datetime(1980, 1, 6, tzinfo=timezone.utc)  # GNSS start time
GNSS_START_TIME_JULIAN = julian.to_jd(GNSS_START_TIME.replace(tzinfo=None), "mjd")
GNSS_START_TIME_JULIAN_BOUNDS = julian.to_jd(
    GNSS_START_TIME.replace(tzinfo=None) + timedelta(days=365 * 500), "mjd"
)
TRIGGER_DELAY_SV3 = 0.2  # SV3 trigger delay in seconds
TRIGGER_DELAY_SV2 = 0.1  # SV2 trigger delay in seconds
ADJ_LEAP = 1.0  # this is the leap second adjustment TODO Ask James why this is there

STATION_OFFSETS = {"5209": 200, "5210": 320, "5211": 440, "5212": 560}
MASTER_STATION_ID = {"0": "5209", "1": "5210", "2": "5211", "3": "5212"}


class PingData(BaseModel):
    """
    Represents Ping Data with timestamps and related attributes.

    Attributes:
        TriggerTime (datetime): Time when the ping was triggered (modified Julian day) [float].
        PingTime (float): Time when the ping was received [float].
        PingOffset (float): Offset time between trigger and ping [s].
        ADJ_LEAP (float): Adjustment for leap time.
        TRIGGER_DELAY (float): Trigger delay time.
    """

    PingTime: float = Field(
        ge=0, le=24*3600, default=None
    )
    TriggerTime: datetime = Field(ge=GNSS_START_TIME, default=None)
    PingOffset: float = Field(ge=-60, le=60, default=None)
    ADJ_LEAP: float = ADJ_LEAP


    @classmethod
    def from_line(cls, line) -> "PingData":
        """
        Create a PingData instance from a line of text.

        Args:
            line (str): A line of text containing comma-separated values.
            mode (WaveGlider, optional): The mode of the WaveGlider. Defaults to WaveGlider.SV2.

        Returns:
            PingData: An instance of PingData created from the provided line.

        Example:
            >>> line = "2003,327374,1527706574,2018/05/30 18:56:14.697 PING - Offset = 0.000"
            >>> PingData.from_line(line)
            PingData(PingTime=58268.539063657634, TriggerTime=datetime.datetime(2018, 5, 30, 12, 56, 14), PingOffset=0.0, ADJ_LEAP=1.0, TRIGGER_DELAY=0.1)
        """
  
        TRIGGER_DELAY = TRIGGER_DELAY_SV2

        # "2003,327374,1527706574,2018/05/30 18:56:14.697 PING - Offset = 0.000" -> ["2003","327374","1527706574","2018/05/30 18:56:14.697 PING - Offset = 0.000"]
        ping_data_parsed: List[str] = line.split(",")
        # 2018/05/30 18:56:14.697 -> datetime.datetime(2018, 5, 30, 18, 56, 14, 697000)
        trigger_time: datetime = datetime.strptime(
            ping_data_parsed[-1].split("PING")[0].strip(), "%Y/%m/%d %H:%M:%S.%f"
        )

        # ["2003","327374","1527706574","2018/05/30 18:56:14.697 PING - Offset = 0.000"] -> ["PING - Offset = 0.000"] -> "0.000" -> 0.000
        ping_offset: float = float(ping_data_parsed[-1].split(" ")[-1])

        # Compute time delta
        time_delta: float = TRIGGER_DELAY

        # 2018-05-30 18:56:14 + 0.1 + 0.0 = 2018-05-30 18:56:14.1
        ping_time = (trigger_time - datetime(trigger_time.year,trigger_time.month,trigger_time.day)).total_seconds() + time_delta
        #ping_time: datetime = trigger_time + timedelta(seconds=time_delta)
    
        #ping_time_julian: float = julian.to_jd(ping_time, "mjd")

        return cls(
            TriggerTime=trigger_time, PingTime=ping_time, PingOffset=ping_offset
        )


class TransponderData(BaseModel):
    """
    Data class representing acoustic data measurements between a transducer and an indivudual transponder.

    Attributes:
        TransponderID (str): The unique identifier for the transponder.
        TwoWayTravelTime (float, optional): The two-way travel time in seconds [s].
        ReturnTime (float, optional): The return time julian day [days]
        DecibalVoltage (int): Signal relative to full scale voltage in dB.
        CorrelationScore (int): Correlation score.
    """

    TransponderID: str # Transponder ID
    TwoWayTravelTime: float = Field(ge=0.0, le=600)  # Two-way Travel time [seconds]
    ReturnTime: float = Field(
        ge=0, le=3600*24
    )  # Return time since the start of day (modified Julian day) [days]
    DecibalVoltage: int = Field(
        ge=-100, le=100
    )  # Signal relative to full scale voltage [dB]
    CorrelationScore: int = Field(ge=0, le=100)  # Correlation score

    SignalToNoise: Optional[float] = Field(ge=0, le=100,default=0)  # Signal to noise ratio

    TurnAroundTime: Optional[float] = Field(ge=0, le=100,default=0)  # Turn around time [ms]
    
    def correct_travel_time(self, offset: float):
        """
        Corrects the travel time by applying the given offset.

        Args:
            offset (float): The offset value in milliseconds.

        Returns:
            None
        """
        offset_seconds = offset / 1000.0
        offset_fractional_days = offset_seconds / 86400.0
        # self.ReturnTime -= offset_fractional_days
        self.TwoWayTravelTime -= offset_seconds


class SimultaneousInterrogation(BaseModel):
    # TODO rename to simultaneious interrogation
    responses: List[TransponderData]
    pingData: PingData

    def apply_offsets(self, offset_dict: Dict[str, float]):
        """
        Apply the given offsets to the transponder data.

        Args:
            offset_dict (Dict[str,float]): A dictionary of transponder offsets in milliseconds.

        Returns:
            None
        """
        for response in self.responses:
            transponder_id = response.TransponderID
            if transponder_id in offset_dict:
                response.correct_travel_time(offset_dict[transponder_id])

    @classmethod
    def from_line(
        cls, line, pingdata: PingData
    ) -> Union["SimultaneousInterrogation", Exception]:
        # Input line sample
        # 2003,327470,1527706670,2018/05/30 18:57:50.495 >SI:2010,INT1,IR5209;R4470626;[XC70,DBV-15],
        # IR5210;R3282120;[XC90,DBV0],IR5211;R5403623;[XC60,DBV-24]
        transponder_header = "IR"
        transponder_data_set: List[TransponderData] = []

        # parse transponder logs and ditch the header
        # 2003,327470,1527706670,2018/05/30 18:57:50.495 >SI:2010,INT1,IR5209;R4470626;[XC70,DBV-15],
        # IR5210;R3282120;[XC90,DBV0],IR5211;R5403623;[XC60,DBV-24]
        # -> ["5209;R4470626;[XC70,DBV-15],","5210;R3282120;[XC90,DBV0],","5211;R5403623;[XC60,DBV-24]"]
        transponder_logs = line.split(transponder_header)[1:]

        if not transponder_logs:
            return Exception(f"Expected 3 transponder logs, None Found")

        for transponder in transponder_logs:
            # "5210;R3282120;[XC90,DBV0]" -> "5209","R4470626","[XC70,DBV-15]"
            transponderID, travel_time, xc_db = transponder.split(";")

            # [XC70,DBV-15] -> "XC70","DBV-15"
            corr_score, dbv = xc_db.replace("[", "").replace("]", "").split(",")[:2]

            # "R4470626" -> 4470626
            travel_time = int(travel_time.replace("R", ""))

            # 4470626 -> 4.470626, convert from microseconds to seconds
            travel_time = travel_time / 1000000.000

            # Convert from seconds to fractional days
            #travel_time_days = travel_time / 86400.000

            # "DBV-15" -> -15
            dbv = int(dbv.replace("DBV", ""))

            # "XC70" -> "70"
            corr_score = corr_score.replace("XC", "")

            # Computing return time from transponder travel time [s] and pingtime[julian date]
            return_time = travel_time + pingdata.PingTime

            transponder_data = TransponderData(
                TransponderID=transponderID,
                TwoWayTravelTime=travel_time,
                ReturnTime=return_time,
                DecibalVoltage=dbv,
                CorrelationScore=int(corr_score),
            )
            transponder_data_set.append(transponder_data)

        simultaneous_interrogation = cls(
            responses=transponder_data_set, pingData=pingdata
        )

        return simultaneous_interrogation

def from_simultaneous_interrogation(si_set: List[SimultaneousInterrogation]
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
        "TransponderID",
        "TriggerTime",
        "PingTime",
        "ReturnTime",
        "TwoWayTravelTime",
        "DecibalVoltage",
        "CorrelationScore",
    ]
    dataframe_pre = dataframe_pre[column_order]
    dataframe_pre["TriggerTime"] = dataframe_pre["TriggerTime"].apply(
        lambda x: pd.Timestamp(x)
    )

    return dataframe_pre

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

@pa.check_types
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
    if not os.path.exists(source.location):
        response = f"File {source.location} not found"
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
    with open(source.location) as sonardyne_file:
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
                offset_dict: Union[Dict[str, float], None] = (
                    get_transponder_offsets(line)
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

    acoustic_df:pd.DataFrame = from_simultaneous_interrogation(
        simultaneous_interrogation_set
    )

    unique_transponders: list = list(
        acoustic_df.reset_index()["TransponderID"].unique()
    )
    shot_count: int = int(acoustic_df.shape[0] / len(unique_transponders))

    log_response = f"Acoustic Parser: {acoustic_df.shape[0]} shots from FILE {source.location} | {len(unique_transponders)} transponders | {shot_count} shots per transponder"
    logger.info(log_response)
    acoustic_df.TriggerTime = acoustic_df.TriggerTime.dt.tz_localize("UTC")
    # acoustic_df.ReturnTime = acoustic_df.ReturnTime.dt.tz_localize("UTC")
    return acoustic_df

@pa.check_types
def dfpo00_to_acousticdf(source: DFPO00RawFile) -> DataFrame[AcousticDataFrame]:
    processed = []
    with open(source.location) as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            if data.get("event") == "range":
                range_data = data.get("range", None)
                time_data = data.get("time", None)
                if range_data and time_data:
                    id: str = range_data.get("cn", "").replace("IR", "")
                    travel_time: float = range_data.get("range", 0)  # travel time [s]
                    tat: float = range_data.get("tat", 0)  # turn around time [ms]
                    travel_time_true = (
                        travel_time - (tat/1000) - TRIGGER_DELAY_SV2
                    )  # travel time corrected for turn around time and trigger delay

                    dbv = range_data.get("diag").get("dbv")[0]
                    xc = range_data.get("diag").get("xc")[0]
                    trigger_time: float = time_data.get(
                        "common", 0
                    )  # Time since GNSS start [s]
                    trigger_time_dt = datetime.fromtimestamp(trigger_time)
                    ping_time = trigger_time_dt + timedelta(seconds=ADJ_LEAP)
                    # Convert to Julian date
                    ping_time_julian = julian.to_jd(ping_time, "mjd")
                    travel_time_true_fdays = travel_time_true / 86400.000
                    return_time = ping_time_julian + travel_time_true_fdays

                processed.append(
                   
                   {
                    "TriggerTime": trigger_time_dt,
                    "TransponderID": id,
                    "TwoWayTravelTime": travel_time_true,
                    "ReturnTime": return_time,
                    "DecibalVoltage": dbv,
                    "CorrelationScore": xc,
                    "PingTime": ping_time_julian
                   }
                )
    df = pd.DataFrame(processed)
    return df

@pa.check_types
def qcpin_to_acousticdf(source:QCPinFile) -> DataFrame[AcousticDataFrame]:
    with open(source.location) as f:
        data = json.load(f)

    shot_data = []
    for key in data.keys():
        if key != "interrogation":
            range_data = data.get(key).get("range")
            time_data = data.get(key).get("time")

            id: str = range_data.get("cn", "").replace("IR", "")
            travel_time: float = range_data.get("range", 0)
            tat: float = range_data.get("tat", 0)
            travel_time_true = (
                travel_time - (tat/1000) # TODO might need to acct. for trig delay?
            )
            xc = range_data.get("diag").get("xc")[0]
            dbv = range_data.get("diag").get("dbv")[0]
            snr = range_data.get("diag").get("snr")[0]
            trigger_time: float = time_data.get("common", 0)
            trigger_time_dt = datetime.fromtimestamp(trigger_time)
            ping_time = trigger_time_dt + timedelta(seconds=TRIGGER_DELAY_SV3)

            shot_data.append(
                {
                    "TriggerTime": trigger_time_dt,
                    "TransponderID": id,
                    "TwoWayTravelTime": travel_time_true,
                    "ReturnTime": ping_time,
                    "DecibalVoltage": dbv,
                    "CorrelationScore": xc,
                    "PingTime": ping_time,
                    "SignalToNoise": snr
                }
            )
    df = pd.DataFrame(shot_data)
    return df
