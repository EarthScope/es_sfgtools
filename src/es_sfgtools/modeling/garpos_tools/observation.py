"""
Author: Franklyn Dunbar
Date: 2024-05-01
Email: franklyn.dunbar@earthscope.org, franklyn.dunbar@umontana.edu
"""

# External imports
from typing import Optional, List, Union, Tuple
import pandera as pa
from pydantic import (
    BaseModel,
    field_validator,
    ValidationError,
    field_serializer,
    Field,
)
from pandera.typing import Series, DataFrame
from pandera.errors import SchemaErrors
import pandas as pd
import numpy as np
from datetime import datetime
import os
from configparser import ConfigParser
from pandera.typing import DataFrame
import julian
import pymap3d as pm

# Local Imports
from .schemas import GPPositionENU, GPPositionLLH, SoundVelocityProfile,GPTransponder,GPATDOffset
from ..schemas import AcousticDataFrame, IMUDataFrame, PositionDataFrame
from ..schemas.file_schemas import MasterFile,SeaBirdFile,NovatelFile,RinexFile,SonardyneFile,LeverArmFile,KinFile
from ..utils import CoordTransformer

from es_sfgtools.utils.loggers import GarposLogger as logger


def avg_transponder_position(
    transponders: List[GPTransponder],
) -> Tuple[GPPositionENU, GPPositionLLH]:
    """
    Calculate the average position of the transponders

    Args:
        transponders: List of transponders

    Returns:
        Tuple[PositionENU, PositionLLH]: Average position in ENU and LLH
    """
    pos_array_llh = []
    pos_array_enu = []
    for transponder in transponders:
        pos_array_llh.append(
            [
                transponder.position_llh.latitude,
                transponder.position_llh.longitude,
                transponder.position_llh.height,
            ]
        )
        pos_array_enu.append(transponder.position_enu.get_position())
    avg_pos_llh = np.mean(pos_array_llh, axis=0).tolist()
    avg_pos_enu = np.mean(pos_array_enu, axis=0).tolist()

    min_pos_llh = np.min(pos_array_llh, axis=0).tolist()

    out_pos_llh = GPPositionLLH(
        latitude=avg_pos_llh[0], longitude=avg_pos_llh[1], height=avg_pos_llh[2]
    )
    out_pos_enu = GPPositionENU.from_list(avg_pos_enu)

    return out_pos_enu, out_pos_llh


def merge_to_shotdata(
    acoustic: DataFrame[AcousticDataFrame],
    imu: DataFrame[IMUDataFrame],
    gnss: DataFrame[PositionDataFrame],
) -> pd.DataFrame:
    """
    Merge the acoustic, imu and gnss dataframes into a single dataframe

    Args:
    acoustic: AcousticDataFrame
    imu: IMUDataFrame
    gnss: PositionDataFrame

    Returns:
    pd.DataFrame: Merged dataframe
    """

    logger.loginfo("Merging acoustic, imu and gnss dataframes")

    acoustic = acoustic.reset_index()
    acoustic.columns = acoustic.columns.str.lower()
    imu.columns = imu.columns.str.lower()
    gnss.columns = gnss.columns.str.lower()

    # create a column for returntime in acoustic thats in datetime format
    # modified julian date -> datetime
    acoustic["time"] = acoustic["triggertime"]

    acoustic["returntime_dt"] = acoustic["returntime"].apply(
        lambda x: julian.from_jd(x, fmt="mjd")
    )
    acoustic["pingtime_dt"] = acoustic["pingtime"].apply(
        lambda x: julian.from_jd(x, fmt="mjd")
    )

    gnss["time"] = pd.to_datetime(gnss["time"])
    imu["time"] = pd.to_datetime(imu["time"])
    # TODO check if the dataframes have the
    # acoustic_timespan = [acoustic["pingtime"].min(), acoustic["pingtime"].max()]
    # imu_timespan = [imu.index.min(), imu.index.max()]
    # gnss_timespan = [gnss["time"].min(), gnss["time"].max()]
    # Merge the dataframes
    """
            IMU:
            ,Time,Latitude,Longitude,Height,NorthVelocity,EastVelocity,UpVelocity,Roll,Pitch,Azimuth
            0,2018-06-05 00:00:00.050,54.33236627535,-158.46946390983,12.6195,-0.0078,0.2226,-0.2324,-0.537771604,7.053161625,60.43386426
            1,2018-06-05 00:00:00.100,54.33236624129,-158.46946374432,12.6089,-0.0972,0.1926,-0.1779,-0.174584708,7.267122425,60.364148634
            2,2018-06-05 00:00:00.150,54.33236617274,-158.46946360741,12.6013,-0.188,0.1696,-0.1188,0.411828413,7.418472712,60.36250395
            3,2018-06-05 00:00:00.200,54.33236606896,-158.46946349718,12.5963,-0.2655,0.1306,-0.0671,1.123918267,7.526855965,60.388641956
            4,2018-06-05 00:00:00.250,54.33236593335,-158.46946342109,12.5938,-0.3355,0.0833,-0.0197,1.869285234,7.608214088,60.424503402
            5,2018-06-05 00:00:00.300,54.33236577218,-158.46946338987,12.5935,-0.391,0.0248,0.0265,2.563764239,7.672790742,60.466341231
            6,2018-06-05 00:00:00.350,54.33236558986,-158.46946340787,12.5957,-0.4335,-0.0411,0.0764,3.122817073,7.71598298,60.49379062
            7,2018-06-05 00:00:00.400,54.33236539136,-158.46946347742,12.6007,-0.4673,-0.1119,0.1269,3.490824818,7.730072669,60.49884816
            8,2018-06-05 00:00:00.450,54.33236518435,-158.46946360571,12.6088,-0.4808,-0.1876,0.1932,3.576496104,7.689833437,60.467685238
            9,2018-06-05 00:00:00.500,54.3323649729,-158.46946378839,12.6208,-0.4871,-0.2585,0.2708,3.396767019,7.563953759,60.409343007
            10,2018-06-05 00:00:00.550,54.33236476027,-158.46946402185,12.6369,-0.4891,-0.3229,0.3485,2.973482584,7.32876294,60.332332277

            Acoustic:
            TriggerTime,TransponderID,PingTime,ReturnTime,tt,dbv,CorrelationScore
            2018-06-05 00:00:14,5209,58274.000174768735,58274.00022122925,4.014189,-12,55
            2018-06-05 00:00:14,5210,58274.000174768735,58274.00022204121,4.084342,-12,50
            2018-06-05 00:00:14,5211,58274.000174768735,58274.000174768735,0.0,0,25
            2018-06-05 00:00:14,5212,58274.000174768735,58274.000174768735,0.0,0,25
            2018-06-05 00:00:29,5209,58274.000348379835,58274.00039485291,4.015274,-9,60
            2018-06-05 00:00:29,5210,58274.000348379835,58274.00039566915,4.085797,-9,40
            2018-06-05 00:00:29,5211,58274.000348379835,58274.00039847627,4.328332,-12,65
            2018-06-05 00:00:29,5212,58274.000348379835,58274.000348379835,0.0,0,25
            2018-06-05 00:00:44,5209,58274.000521990936,58274.000568462034,4.015103,-9,55

            GNSS:
            ,x,y,z,latitude,longitude,height,number_of_satellites,pdop,time
            0,-3467151.069,-1367881.2187,5158410.5605,54.33236416081,201.53052300364,10.6455,8,1.69,2018-06-05 00:00:00.199999
            1,-3467151.1217,-1367881.249,5158410.5177,54.33236349803,201.53052313896,10.6457,8,1.69,2018-06-05 00:00:00.399998
            2,-3467151.2402,-1367881.2424,5158410.5137,54.33236269053,201.53052237697,10.7054,8,1.69,2018-06-05 00:00:00.599997
            3,-3467151.4095,-1367881.211,5158410.5603,54.33236186872,201.53052097264,10.8283,8,1.69,2018-06-05 00:00:00.799996
            4,-3467151.6101,-1367881.1993,5158410.6043,54.33236076895,201.53051967334,10.9704,8,1.69,2018-06-05 00:00:00.999995
            5,-3467151.8074,-1367881.1811,5158410.6112,54.33235951384,201.53051830044,11.0791,8,1.69,2018-06-05 00:00:01.199993

            --->
            OUTPUT:
            ,SET,LN,MT,TT,ResiTT,TakeOff,gamma,flag,ST,ant_e0,ant_n0,ant_u0,head0,pitch0,roll0,RT,ant_e1,ant_n1,ant_u1,head1,pitch1,roll1
            0,S01,L01,M11,2.289306,0.0,0.0,0.0,False,30072.395125,-27.85291,1473.14423,14.73469,176.47,0.59,-1.39,30075.74594,-26.70998,1462.01803,14.32703,177.07,-0.5,-1.1
            1,S01,L01,M13,3.12669,0.0,0.0,0.0,False,30092.395725,-22.08296,1412.88729,14.59827,188.24,0.41,-2.13,30096.58392,-22.3514,1401.77938,14.65401,190.61,-0.1,-2.14
            2,S01,L01,M14,2.702555,0.0,0.0,0.0,False,30093.48579,-22.25377,1409.87685,14.67772,188.93,0.15,-1.7,30097.24985,-22.38458,1399.96509,14.55534,190.82,-0.39,-2.21
            3,S01,L01,M14,2.68107,0.0,0.0,0.0,False,30102.396135,-23.25514,1387.38992,14.75355,192.39,0.1,-1.79,30106.13871,-23.96613,1378.4627,14.58135,192.92,0.21,-1.7

    """

    # Merge GNSS and IMU data
    merged_gnss_imu = pd.merge_asof(
        left=gnss, right=imu.loc[:, ["time", "roll", "pitch", "azimuth"]], on="time"
    ).loc[
        :,
        [
            "time",
            "x",
            "y",
            "z",
            "latitude",
            "longitude",
            "height",
            "roll",
            "pitch",
            "azimuth",
        ],
    ]

    shot_trigger_merged = pd.merge_asof(
        left=acoustic.sort_values(by="pingtime_dt").drop(columns=["time"]),
        right=merged_gnss_imu,
        left_on="pingtime_dt",
        right_on="time",
    )
    shot_return_merged = pd.merge_asof(
        left=acoustic.sort_values(by="returntime_dt").drop(columns=["time"]),
        right=merged_gnss_imu,
        left_on="returntime_dt",
        right_on="time",
    )

    # rename shot_trigger columns
    shot_trigger_merged.rename(
        columns={
            "transponderid": "MT",
            "tt": "TT",
            "x": "ant_e0",
            "y": "ant_n0",
            "z": "ant_u0",
            "azimuth": "head0",
            "pitch": "pitch0",
            "roll": "roll0",
            "pingtime": "ST",
            "returntime": "RT",
        },
        inplace=True,
    )
    shot_return_merged.rename(
        columns={
            "transponderid": "MT",
            "tt": "TT",
            "x": "ant_e1",
            "y": "ant_n1",
            "z": "ant_u1",
            "azimuth": "head1",
            "pitch": "pitch1",
            "roll": "roll1",
            "returntime": "RT",
            "pingtime": "ST",
        },
        inplace=True,
    )

    shot_return_merged["time"] = shot_return_merged[
        "returntime_dt"
    ] - shot_return_merged.TT.apply(lambda x: pd.Timedelta(seconds=x))
    shot_trigger_merged["time"] = shot_trigger_merged["pingtime_dt"]

    output_df = pd.merge(
        left=shot_trigger_merged.loc[
            :,
            [
                "latitude",
                "longitude",
                "height",
                "ant_e0",
                "ant_n0",
                "ant_u0",
                "head0",
                "pitch0",
                "roll0",
                "triggertime",
                "MT",
                "TT",
            ],
        ],
        right=shot_return_merged,
        how="left",
        on=["triggertime", "MT", "TT"],
    ).dropna()

    # remove _x from lattitude and longitude
    output_df = output_df.rename(
        columns={
            "latitude_x": "latitude",
            "longitude_x": "longitude",
            "height_x": "height",
        }
    )

    output_df = output_df.loc[
        :,
        [
            "MT",
            "ST",
            "RT",
            "TT",
            "ant_e0",
            "ant_n0",
            "ant_u0",
            "head0",
            "pitch0",
            "roll0",
            "ant_e1",
            "ant_n1",
            "ant_u1",
            "head1",
            "pitch1",
            "roll1",
            "latitude",
            "longitude",
            "height",
        ],
    ]
    output_df = output_df.reset_index(drop=True)
    output_df["SET"] = "S01"
    output_df["LN"] = "L01"
    return output_df


class ObservationData(pa.DataFrameModel):
    """Observation data file schema

    Example data:

    ,SET,LN,MT,TT,ResiTT,TakeOff,gamma,flag,ST,ant_e0,ant_n0,ant_u0,head0,pitch0,roll0,RT,ant_e1,ant_n1,ant_u1,head1,pitch1,roll1
    0,S01,L01,M11,2.289306,0.0,0.0,0.0,False,30072.395125,-27.85291,1473.14423,14.73469,176.47,0.59,-1.39,30075.74594,-26.70998,1462.01803,14.32703,177.07,-0.5,-1.1
    1,S01,L01,M13,3.12669,0.0,0.0,0.0,False,30092.395725,-22.08296,1412.88729,14.59827,188.24,0.41,-2.13,30096.58392,-22.3514,1401.77938,14.65401,190.61,-0.1,-2.14
    2,S01,L01,M14,2.702555,0.0,0.0,0.0,False,30093.48579,-22.25377,1409.87685,14.67772,188.93,0.15,-1.7,30097.24985,-22.38458,1399.96509,14.55534,190.82,-0.39,-2.21
    3,S01,L01,M14,2.68107,0.0,0.0,0.0,False,30102.396135,-23.25514,1387.38992,14.75355,192.39,0.1,-1.79,30106.13871,-23.96613,1378.4627,14.58135,192.92,0.21,-1.7
    4,S01,L01,M11,2.218846,0.0,0.0,0.0,False,30103.4862,-23.57701,1384.73242,14.65861,192.62,-0.14,-1.5,30106.766555,-24.0478,1377.09283,14.68464,193.04,0.59,-1.81
    """

    set: Optional[Series[str]] = pa.Field(
        description="Set name", alias="SET", default="S01"
    )
    line: Optional[Series[str]] = pa.Field(
        description="Line name", alias="LN", default="L01"
    )
    transponder_id: Series[str] = pa.Field(
        description="Station name", alias="MT", coerce=True
    )

    travel_time: Series[float] = pa.Field(description="Travel time [sec]", alias="TT")

    transmission_time: Series[float] = pa.Field(
        description="Time of transmission of the acoustic signal in MJD [s]", alias="ST"
    )

    reception_time: Series[float] = pa.Field(
        description="Time of reception of the acoustic signal in MJD [s]", alias="RT"
    )

    ant_e0: Series[float] = pa.Field(
        description="Antenna position in east direction (ENU coords) at the time of the first measurement [m]"
    )

    ant_n0: Series[float] = pa.Field(
        description="Antenna position in north direction (ENU coords) at the time of the first measurement [m]"
    )

    ant_u0: Series[float] = pa.Field(
        description="Antenna position in up direction (ENU coords) at the time of the first measurement [m]"
    )

    head0: Series[float] = pa.Field(
        description="Antenna heading at the time of the first measurement [deg]"
    )

    pitch0: Series[float] = pa.Field(
        description="Antenna pitch at the time of the first measurement [deg]"
    )

    roll0: Series[float] = pa.Field(
        description="Antenna roll at the time of the first measurement [deg]"
    )

    ant_e1: Series[float] = pa.Field(
        description="Antenna position in east direction (ENU coords) at the time of the second measurement [m]"
    )

    ant_n1: Series[float] = pa.Field(
        description="Antenna position in north direction (ENU coords) at the time of the second measurement [m]"
    )

    ant_u1: Series[float] = pa.Field(
        description="Antenna position in up direction (ENU coords) at the time of the second measurement [m]"
    )

    head1: Series[float] = pa.Field(
        description="Antenna heading at the time of the second measurement [deg]"
    )

    pitch1: Series[float] = pa.Field(
        description="Antenna pitch at the time of the second measurement [deg]"
    )

    roll1: Series[float] = pa.Field(
        description="Antenna roll at the time of the second measurement [deg]"
    )

    flag: Series[bool] = pa.Field(
        default=False, description="Flag for mis-response in the data", coerce=True
    )
    latitude: Optional[Series[float]] = pa.Field(
        description="latitude of the antennae", alias="lat"
    )
    longitude: Optional[Series[float]] = pa.Field(
        description="longitude of the antennae", alias="lon"
    )

    gamma: Series[float] = pa.Field(
        default=0.0, description="Sound speed variation [m/s]", coerce=True
    )
    # These fields are populated after the model run
    ResiTT: Optional[Series[float]] = pa.Field(
        default=0.0, description="Residual travel time [ms]"
    )

    TakeOff: Optional[Series[float]] = pa.Field(
        default=0.0, description="Take off angle [deg]"
    )

    class Config:
        coerce = True
        add_missing_columns = True

    @classmethod
    def from_agi(
        cls,
        acoustic: DataFrame[AcousticDataFrame],
        imu: DataFrame[IMUDataFrame],
        gnss: DataFrame[PositionDataFrame],
    ) -> pd.DataFrame:
        df = merge_to_shotdata(acoustic, imu, gnss)
        return cls.validate(df,lazy=True)


class GarposObservation(BaseModel):
    campaign: str
    date_utc: datetime
    date_mjd: float
    ref_frame: str = "ITRF2014"
    shot_data: DataFrame[ObservationData]
    sound_speed_data: DataFrame[SoundVelocityProfile]

    @field_serializer("date_utc")
    def serialize_date(self, value):
        return str(value.isoformat())

    @field_serializer("shot_data")
    def serialize_shot_data(self, value):
        return value.to_json(orient="records")

    @field_serializer("sound_speed_data")
    def serialize_sound_speed_data(self, value):
        return value.to_json(orient="records")

    @field_validator("shot_data", mode="before")
    def validate_shot_data(cls, value):
        try:
            if isinstance(value, str):
                value = pd.read_json(value)

            return ObservationData.validate(value, lazy=True)
        except ValidationError as e:
            raise ValueError(f"Invalid shot data: {e}")

    @field_validator("sound_speed_data", mode="before")
    def validate_sound_speed_data(cls, value):
        try:
            if isinstance(value, str):
                value = pd.read_json(value)
            return SoundVelocityProfile.validate(value, lazy=True)
        except SchemaErrors as err:
            raise ValueError(f"Invalid sound speed data: {err.data}")

    @field_validator("date_utc", mode="before")
    def validate_date_utc(cls, value):
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError as e:
                raise ValueError(f"Invalid date format: {e}")
        return value

    @classmethod
    def from_file_schema(
        cls,
        gnss_data: Union[KinFile,str,pd.DataFrame],
        imu_data: Union[NovatelFile,str,pd.DataFrame],
        acoustic_data: Union[SonardyneFile,str,pd.DataFrame],
        svp_data: SeaBirdFile,
        master_file: MasterFile,
        campaign: str = "Test",
    
    ) -> "GarposObservation":
        """
        Create a GarposObservation object from the provided data files

        Args:
            gnss_data: Union[KinFile,str,pd.DataFrame]
            imu_data: Union[NovatelFile,str,pd.DataFrame]
            acoustic_data: Union[SonardyneFile,str,pd.DataFrame]
            svp_data: SeaBirdFile
            master_file: MasterFile
            campaign: str

        Returns:
            GarposObservation: GarposObservation object
        """
        
        logger.loginfo("Creating GarposObservation object from provided data")
        gnss_df:DataFrame[PositionDataFrame] = PositionDataFrame.load(gnss_data)

        # Correct GNSS data for longitude reference (i.e. 200 -> 200-360 = -160)
        gnss_df["longitude"] = gnss_df["longitude"].apply(
            lambda x: x - 360 if x > 180 else x
        )

        acoustic_df:pd.DataFrame = AcousticDataFrame.load(acoustic_data)
        imu_df = IMUDataFrame.load(imu_data)
        _, center_llh = master_file.load()
        coord_transformer = CoordTransformer(center_llh)

        # XYZ to ENU
        # 1. Subtract the origin from the ECEF coordinates, 2. Rotate the ECEF coordinates to ENU about origin lat,lon
        for idx, row in gnss_df.iterrows():
            x, y, z = coord_transformer.LLH2ENU(
                row["latitude"], row["longitude"], row["height"]
            )
            gnss_df.loc[idx, "x"] = x
            gnss_df.loc[idx, "y"] = y
            gnss_df.loc[idx, "z"] = z

        svp_df = svp_data.load()
        # Correct svp data for geoid undulation TODO might be wrong
        # svp_df["depth"] = svp_df["depth"] + center_llh.height

        date_utc = gnss_df.time.min()
        date_mjd = julian.to_jd(date_utc, fmt="mjd")
        shot_data: DataFrame[ObservationData] = merge_to_shotdata(
            acoustic=acoustic_df, imu=imu_df, gnss=gnss_df
        )

        # convert transponder ids to str
        shot_data["MT"] = shot_data["MT"].astype(str)
   
        return cls(
            campaign=campaign,
            date_utc=gnss_df.loc[0]["time"],
            date_mjd=date_mjd,
            shot_data=shot_data,
            sound_speed_data=svp_df,
        )


class GarposSite(BaseModel):
    name: str
    atd_offset: GPATDOffset
    center_enu: GPPositionENU
    center_llh: GPPositionLLH
    transponders: List[GPTransponder]
    delta_center_position: GPPositionENU

    @classmethod
    def from_file_schema(
        cls, master_file:MasterFile, lever_arm_file:LeverArmFile, name: str = "Test"
    ) -> "GarposSite":
        """ 
        Create a GarposSite object from a master file and lever arm file

        Args:
            master_file: MasterFile
            lever_arm_file: LeverArmFile
            name: str

        Returns:
            GarposSite: GarposSite object
        """

        logger.loginfo("Creating GarposSite object from master and lever arm files")
        transponder_list, center_llh = master_file.load()

        # Generate coord transformer from site center in llh
        coord_transformer = CoordTransformer(center_llh)

        # get transponder positions in ENU, rotated around the site center in llh
        for transponder in transponder_list:
            lat, lon, hgt = (
                transponder.position_llh.latitude,
                transponder.position_llh.longitude,
                transponder.position_llh.height,
            )

            e, n, u = coord_transformer.LLH2ENU(lat, lon, hgt)
            position_enu = GPPositionENU.from_list([e, n, u])
            transponder.position_enu = position_enu

        # Calculate the array center in ENU
        _, transponder_avg_llh = avg_transponder_position(transponder_list)
        #
        transponder_center_enu: List[float] = coord_transformer.LLH2ENU(
            transponder_avg_llh.latitude,
            transponder_avg_llh.longitude,
            transponder_avg_llh.height,
        )
        transponder_avg_enu = GPPositionENU.from_list(transponder_center_enu)
        atd_offset = lever_arm_file.load()
        delta_center_position = (
            GPPositionENU()
        )  # TODO add sigma_e,sigma_n values of 1.0 per james/john recc.
        delta_center_position.east.sigma = 1.0
        delta_center_position.north.sigma = 1.0
        delta_center_position.up.sigma = 0

        return cls(
            name=name,
            atd_offset=atd_offset,
            center_enu=transponder_avg_enu,
            center_llh=center_llh,
            transponders=transponder_list,
            delta_center_position=delta_center_position,
        )
