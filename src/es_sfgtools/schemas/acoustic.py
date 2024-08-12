"""
Author: Franklyn Dunbar
Date: 2024-03-07
Email: franklyn.dunbar@earthscope.org
"""

import pandas as pd
from pydantic import BaseModel,Field,model_validator,ValidationError
import pandera as pa
from pandera.typing import Series,DataFrame
from datetime import datetime,timedelta
from typing import List,Union,Dict
from enum import Enum
import logging
import re 
import julian
import os
import json

# Importing constants from defaults.py
from .defaults import GNSS_START_TIME,ADJ_LEAP,TRIGGER_DELAY_SV2,TRIGGER_DELAY_SV3,STATION_OFFSETS
from .file_schemas import SonardyneFile
# Configure logging
logger = logging.getLogger(os.path.basename(__file__))

GNSS_START_TIME_JULIAN = julian.to_jd(GNSS_START_TIME.replace(tzinfo=None), 'mjd')
GNSS_START_TIME_JULIAN_BOUNDS = julian.to_jd(GNSS_START_TIME.replace(tzinfo=None) + timedelta(days=365*500),'mjd')

class WaveGlider(Enum):
    SV2 = TRIGGER_DELAY_SV2
    SV3 = TRIGGER_DELAY_SV3

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

    PingTime: float = Field(ge=GNSS_START_TIME_JULIAN,le=GNSS_START_TIME_JULIAN_BOUNDS,default=None) 
    TriggerTime: datetime = Field(ge=GNSS_START_TIME,default=None)
    PingOffset: float = Field(ge=-60,le=60,default=None)
    ADJ_LEAP: float = ADJ_LEAP
    wave_glider:WaveGlider = WaveGlider.SV2

    @classmethod
    def from_line(cls, line,mode:WaveGlider=WaveGlider.SV2) -> 'PingData':
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
        if mode == WaveGlider.SV3:
            TRIGGER_DELAY = TRIGGER_DELAY_SV3
        else:
            TRIGGER_DELAY = TRIGGER_DELAY_SV2

        # "2003,327374,1527706574,2018/05/30 18:56:14.697 PING - Offset = 0.000" -> ["2003","327374","1527706574","2018/05/30 18:56:14.697 PING - Offset = 0.000"]
        ping_data_parsed: List[str] = line.split(',')
        # 2018/05/30 18:56:14.697 -> datetime.datetime(2018, 5, 30, 18, 56, 14, 697000)
        trigger_time: datetime = datetime.strptime(
            ping_data_parsed[-1].split("PING")[0].strip(),'%Y/%m/%d %H:%M:%S.%f'
        )
        
        # ["2003","327374","1527706574","2018/05/30 18:56:14.697 PING - Offset = 0.000"] -> ["PING - Offset = 0.000"] -> "0.000" -> 0.000
        ping_offset: float =  float(ping_data_parsed[-1].split(" ")[-1])

        # Compute time delta
        time_delta: float = ADJ_LEAP + ping_offset
        # 2018-05-30 18:56:14 + 0.1 + 0.0 = 2018-05-30 18:56:14.1
        ping_time: datetime = trigger_time + timedelta(seconds=time_delta) 

        ping_time_julian:float = julian.to_jd(ping_time, 'mjd')

        return cls(TriggerTime=trigger_time, PingTime=ping_time_julian, PingOffset=ping_offset)

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
    TwoWayTravelTime: float = Field(ge=0.0,le=600) # Two-way Travel time [seconds]
    ReturnTime: float = Field(ge=GNSS_START_TIME_JULIAN,le=GNSS_START_TIME_JULIAN_BOUNDS) # Return time since the start of day (modified Julian day) [days]
    DecibalVoltage:int = Field(ge=-100,le=100) # Signal relative to full scale voltage [dB]
    CorrelationScore: int = Field(ge=0,le=100) # Correlation score

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
        #self.ReturnTime -= offset_fractional_days
        self.TwoWayTravelTime -= offset_seconds

class SimultaneousInterrogation(BaseModel):
    #TODO rename to simultaneious interrogation
    responses : List[TransponderData]
    pingData: PingData

    def apply_offsets(self,offset_dict:Dict[str,float]):
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
    def from_line(cls, line, pingdata:PingData) -> Union['SimultaneousInterrogation',Exception]:
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
            corr_score,dbv = xc_db.replace("[","").replace("]","").split(",")[:2] 

            # "R4470626" -> 4470626
            travel_time = int(travel_time.replace("R",""))

            # 4470626 -> 4.470626, convert from microseconds to seconds
            travel_time = travel_time / 1000000.000 

    
            # Convert from seconds to fractional days
            travel_time_days = travel_time / 86400.000

            # "DBV-15" -> -15
            dbv = int(dbv.replace("DBV",""))

            # "XC70" -> "70"
            corr_score = corr_score.replace("XC","")

            # Computing return time from transponder travel time [s] and pingtime[julian date]
            return_time = travel_time_days + pingdata.PingTime
            
        
            transponder_data = TransponderData(
                TransponderID=transponderID, 
                TwoWayTravelTime=travel_time,
                ReturnTime=return_time, 
                DecibalVoltage=dbv, 
                CorrelationScore=int(corr_score))
            transponder_data_set.append(transponder_data)
    
        simultaneous_interrogation = cls(responses=transponder_data_set, pingData=pingdata)

        return simultaneous_interrogation


class AcousticDataFrame(pa.DataFrameModel):
    """Handles the parsing and validation of acoustic data from a file.
    Attributes:
        TransponderID (Series[str]): Unique identifier for the transponder.
        TriggerTime (Series[datetime]): Time when the ping was triggered.
        PingTime (Series[float]): Time when ping was received (modified Julian day).
        ReturnTime (Series[float]): Return time in seconds since the start of day (modified Julian day).
        TwoWayTravelTime (Series[float]): Two-way travel time.
        DecibalVoltage (Series[int]): Signal relative to full scale voltage.
        CorrelationScore (Series[int]): Correlation score.
    
    Methods:
        from_simultaneous_interrogation(cls, si_set:List[SimultaneousInterrogation]) -> Union[pd.DataFrame,pa.errors.SchemaErrors]:
            Generate a validated AcousticDataFrame from a list of SimultaneousInterrogation instances.
        from_file(cls,file:str) ->  Union[pd.DataFrame,pa.errors.SchemaErrors]:
            Read data from a file and return a validated dataframe.

    """
    TransponderID: Series[str] = pa.Field(
        description="Unique identifier for the transponder", coerce=True)

    TriggerTime: Series[pd.Timestamp] = pa.Field(ge=GNSS_START_TIME.replace(tzinfo=None),coerce=True,
        description="Time when the ping was triggered [datetime]")

    PingTime: Series[float] = pa.Field(ge=GNSS_START_TIME_JULIAN, le=GNSS_START_TIME_JULIAN_BOUNDS,coerce=True,
        description="Time when ping was received (modified Julian day) [days]")

    ReturnTime: Series[float] = pa.Field(ge=GNSS_START_TIME_JULIAN, le=GNSS_START_TIME_JULIAN_BOUNDS,coerce=True,
        description="Return time in seconds since the start of day (modified Julian day) [days]")

    TwoWayTravelTime: Series[float] = pa.Field(ge=0.0, le=600,coerce=True,
        description="Two-way travel time [s]")

    DecibalVoltage: Series[int] = pa.Field(ge=-100, le=100,
        description="Signal relative to full scale voltage [dB]", coerce=True)

    CorrelationScore: Series[int] = pa.Field(ge=0, le=100,coerce=True,
        description="Correlation score")
    
    class Config:
        drop_invalid_rows = True
        coerce=True

    @classmethod
    def from_simultaneous_interrogation(cls, si_set:List[SimultaneousInterrogation]) -> Union[pd.DataFrame,None]:
        """
        Generate a validated AcousticDataFrame from a list of SimultaneousInterrogation instances.
        """
        si_data_dicts: List[dict] = []
        for si_data in si_set:
            ping_data_dict = dict(si_data.pingData)
            for response in si_data.responses:
                response_dict = dict(response)
                si_data_dicts.append({ **response_dict,**ping_data_dict})

        dataframe_pre = pd.DataFrame(si_data_dicts)
        column_order = ["TransponderID","TriggerTime","PingTime","ReturnTime","TwoWayTravelTime","DecibalVoltage","CorrelationScore"]
        dataframe_pre = dataframe_pre[column_order]
        dataframe_pre["TriggerTime"] = dataframe_pre["TriggerTime"].apply(lambda x: pd.Timestamp(x))

        try:
            dataframe_valid = cls.validate(dataframe_pre, lazy=True)
            #dataframe_valid.set_index(["TriggerTime",'TransponderID'], inplace=True)
            return dataframe_valid

        except pa.errors.SchemaErrors as e:
            logger.error(f"Error validating AcousticData in from_simultaneous_interrogation")
            logger.error(e.failure_cases)

            return None
        
    @classmethod
    def get_transponder_offsets(cls,line:str) -> Dict[str,float]:
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
        
        offset_dict: Dict[str,float] = STATION_OFFSETS.copy()
        # "2003,327353,1527706553,2018/05/30 18:55:53.519 >CS:2010,TAT200,..." -> ["2010","TAT200",...]
        parsed_line = line.split(">CS:")[1].split(",")
        transponder_id, offset = parsed_line[:2]
        # "TAT200" -> 200
        try:
            offset = float(offset.replace("TAT",""))
        except ValueError:
            offset = 0.0
        offset_dict[transponder_id] = offset
        return offset_dict

    @classmethod
    def load(cls,source:Union[SonardyneFile,str,pd.DataFrame]) -> DataFrame['AcousticDataFrame']:
        if isinstance(source,SonardyneFile):
            return cls.from_file(source.file)
        elif isinstance(source,str):
            df = pd.read_csv(source)
            return cls.validate(df,lazy=True)
        elif isinstance(source,pd.DataFrame):
            return cls.validate(source,lazy=True)
        else:
            raise ValueError("Source must be a SonardyneFile or a string path to a file")
    @classmethod
    def from_file(cls,file:str,mode:WaveGlider=WaveGlider.SV2,source:str=None) ->  Union[pd.DataFrame,pa.errors.SchemaErrors]:
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
        if not os.path.exists(file):
            response = (f"File {file} not found")
            logger.error(response)
            raise FileNotFoundError(response)
        
        if source is None:
            source = file

        ping_pattern = re.compile("PING - Offset")
        si_pattern = re.compile(">SI:") # TODO take this out for now

        # get transponder offsets from file:
        # 2003,327353,1527706553,2018/05/30 18:55:53.519 >CS:2010,TAT200
        # offset_dict = {"2010": 200}
        tat_pattern = re.compile(">CS:")

        simultaneous_interrogation_set: List[SimultaneousInterrogation] = []
        line_number = 0
        # Dictionary to store transponder time offsets
        main_offset_dict =  STATION_OFFSETS.copy()
        with open(file) as sonardyne_file:
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
                    offset_dict: Union[Dict[str,float],None] = cls.get_transponder_offsets(line)
                    if offset_dict is None:
                        continue
                    main_offset_dict.update(offset_dict)
                    pass


                if ping_pattern.search(line):
                    try:
                        pingData: PingData = PingData.from_line(line,mode=mode)
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
                                si_data: SimultaneousInterrogation = SimultaneousInterrogation.from_line(next_line, pingData)
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

        validation_attempt: Union[pd.DataFrame,None] = cls.from_simultaneous_interrogation(simultaneous_interrogation_set)
        if isinstance(validation_attempt,pd.DataFrame):
            validated_df = validation_attempt
            unique_transponders:list = list(validated_df.reset_index()["TransponderID"].unique())
            shot_count:int = int(validated_df.shape[0] / len(unique_transponders))

            log_response = f"Acoustic Parser: {validated_df.shape[0]} shots from FILE {source} in waveglider mode {mode.value} | {len(unique_transponders)} transponders | {shot_count} shots per transponder"
            logger.info(log_response)
            return validated_df
        else:

            logger.error(f"Error validating dataframe from {source}")
            return None

    @classmethod
    def from_dfo(cls,file:str,mode:WaveGlider=WaveGlider.SV3) -> DataFrame['AcousticDataFrame']:
        processed = []
        with open(file) as f:
            lines = f.readlines()
            for line in lines:
                data = json.loads(line)
                if data.get("event","") == "range":
                    range_data = data.get("range",None)
                    if range_data:
                        id:str = range_data.get("cn","").replace("IR","")
                        travel_time:float = range_data.get("range",0) # travel time [s]
                        tat:float = range_data.get("tat",0) # turn around time [ms]
                        travel_time_true = travel_time - tat - mode.value # travel time corrected for turn around time and trigger delay

                        dbv = range_data.get("diag").get("dbv")[0]
                        xc = range_data.get("diag").get("xc")[0]
                    time_data = data.get("time",None)
                    if time_data:
                        trigger_time:float = time_data.get("common",0) # Time since GNSS start [s]
                        trigger_time_dt = datetime.fromtimestamp(trigger_time)
                        ping_time = trigger_time_dt + timedelta(seconds=ADJ_LEAP)
                        # Convert to Julian date
                        ping_time_julian = julian.to_jd(ping_time, 'mjd')
                        travel_time_true_fdays  = travel_time_true / 86400.000
                        return_time = ping_time_julian + travel_time_true_fdays

                    processed.append(
                        dict(
                            TransponderData(TransponderID=id,TwoWayTravelTime=travel_time,ReturnTime=return_time,DecibalVoltage=dbv,CorrelationScore=xc)
                        ))
        df = pd.DataFrame(processed)
        return cls.validate(df,lazy=True)


if __name__ == "__main__":
    logging.basicConfig(filename='acoustic.log', level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_path = "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/logfiles/323843_001_20240509_185258_00041_DFOP00.raw"
    test_df = AcousticDataFrame.from_dfo(file_path)
