import re
import pandas as pd
import logging
import os
import pandera as pa
import numpy as np
from pandera.typing import DataFrame
from typing import Union
from pathlib import Path
from datetime import datetime,timedelta
from ..assets.file_schemas import AssetEntry,AssetType
from ..assets.observables import SoundVelocityDataFrame

from es_sfgtools.utils.loggers import ProcessLogger as logger

@pa.check_types
def ctd_to_soundvelocity(source: Union[AssetEntry,str,Path]) -> DataFrame[SoundVelocityDataFrame]:
    if isinstance(source,AssetEntry):
        assert source.type == AssetType.CTD
    else:
        source = AssetEntry(local_path=source,type=AssetType.CTD)
    df = pd.read_csv(
        source.local_path,
        sep=" ",
        header=None,
        float_precision="round_trip",
        dtype=np.float64,
        skiprows=1,
    )
    df = df.rename(columns={0: "depth", 1: "speed"})
    df["depth"] *= -1
    for row in df.itertuples():
        df.at[row.Index, "speed"] += row.Index / 1000
        df.at[row.Index, "speed"] += np.random.randint(0, 1000) / 100000

    return df


@pa.check_types(lazy=True)
def seabird_to_soundvelocity(
    source: Union[AssetEntry,str,Path], show_details: bool = True
) -> DataFrame[SoundVelocityDataFrame]:
    """
    Read the sound velocity profile from a file
    fmt = [ Depth [m], Latitude [deg],Longitude [deg],Temperatures [deg C], Salinity [PSU] ,Speed [m/s]]

        *END*
        3.000   54.34259 -158.42674     6.4264    32.2921    1473.07 0.0000e+00
        4.000   54.34268 -158.42679     6.5241    32.2461    1473.41 0.0000e+00
        5.000   54.34266 -158.42679     6.5006    32.2566    1473.35 0.0000e+00
        6.000   54.34266 -158.42680     6.5028    32.2570    1473.38 0.0000e+00
        7.000   54.34266 -158.42680     6.4974    32.2562    1473.37 0.0000e+00
        8.000   54.34268 -158.42680     6.4987    32.2564    1473.39 0.0000e+00
        9.000   54.34268 -158.42680     6.4986    32.2575    1473.41 0.0000e+00
        10.000   54.34268 -158.42680     6.4905    32.2679    1473.41 0.0000e+00
        11.000   54.34268 -158.42680     6.4714    32.2786    1473.36 0.0000e+00
        12.000   54.34268 -158.42680     6.4070    32.3043    1473.16 0.0000e+00
        13.000   54.34268 -158.42680     6.2915    32.3382    1472.76 0.0000e+00
        14.000   54.34268 -158.42683     6.2515    32.3469    1472.63 0.0000e+00
    ...
    """
    if isinstance(source,AssetEntry):
        assert source.type == AssetType.SEABIRD
    else:
        source = AssetEntry(local_path=source,type=AssetType.SEABIRD)

    with open(source.local_path, "r") as f:
        lines = f.readlines()
        data = []
        data_start = re.compile("\*END\*")
        while lines:
            line = lines.pop(0)
            if data_start.match(line):
                break
        if not lines:
            logger.logerr(f"No data found in the sound speed profile file {source.local_path}")
            return None

        for line in lines:

            values = line.split()
            data.append(
                {
                    "depth": float(values[0]),
                    "speed": float(values[5]),
                }
            )
        df = pd.DataFrame(data)
        response = f"Found SS data down to max depth of {df['depth'].max()} m\n"
        response += f"SS ranges from {df['speed'].min()} to {df['speed'].max()} m/s"
        logger.loginfo(response)
        if show_details:
            print(response)
    return df


MASTER_STATION_ID = {"0": "5209", "1": "5210", "2": "5211", "3": "5212"}



# def masterfile_to_siteconfig(
#     source: Union[AssetEntry,str,Path], show_details: bool = True
# ) -> Union[GPSiteConfig, None]:
#     """
#     Convert a MasterFile to a SiteConfig
#     """
#     if isinstance(source,AssetEntry):
#         assert source.type == AssetType.MASTER
#     else:
#         source = AssetEntry(local_path=source,type=AssetType.MASTER)

#     if not os.path.exists(source.local_path):
#         raise FileNotFoundError(f"File {source.local_path} not found")

#     logger.loginfo(f"Populating List[Transponder] and Site data from {source.local_path}")
#     transponders = []

#     lat_lon_line = re.compile(r"Latitude/Longitude array center")
#     non_alphabet = re.compile("[a-c,e-z,A-Z]")
#     geoid_undulation_pat = re.compile(r"Geoid undulation at sea surface point")

#     with open(source.local_path, "r") as f:
#         lines = f.readlines()[2:]

#         for line in lines:
#             if not non_alphabet.search(line):
#                 line_processed = line.strip().split()
#                 # 0    16       54.316094952    -158.461771055    -2096.780      0.200000d0       1479.900
#                 id = line_processed[0]
#                 lat = float(line_processed[2])
#                 lon = float(line_processed[3])
#                 height = float(line_processed[4])
#                 id = MASTER_STATION_ID[id]
#                 # TODO This is not the var
#                 offset = float(line_processed[5].replace("d0", ""))
#                 transponder_position = GPPositionLLH(
#                     latitude=lat, longitude=lon, height=height
#                 )
#                 transponder = GPTransponder(
#                     id=id, position_llh=transponder_position, tat_offset=offset
#                 )
#                 transponders.append(transponder)

#             if geoid_undulation_pat.search(line):
#                 # "+10.300d0           ! Geoid undulation at sea surface point"
#                 geoid_undulation = float(
#                     line.split()[0].replace("d0", "")
#                 )  # TODO verify sign

#             if lat_lon_line.search(line):
#                 # 54.3324d0 -158.4692d0   ! Latitude/Longitude array center (decimal degrees)
#                 line_processed = [
#                     float(x.replace("d0", ""))
#                     for x in line.split("!")[0].strip().split()
#                 ]
#                 lat, lon = line_processed[0], line_processed[1]
#                 center_llh = {
#                     "latitude": lat,
#                     "longitude": lon,
#                     "height": geoid_undulation,
#                 }
#                 break

#     if not center_llh:
#         logger.logerr("Latitude/Longitude array center not found in masterfile")
#         return
#     if not transponders:
#         logger.logerr("No transponders found in masterfile")
#         return
#     if geoid_undulation is None:
#         logger.logerr("Geoid undulation not found in masterfile")
#         return

#     # subtract geoid undulation from transponder height
#     for transponder in transponders:
#         transponder.position_llh.height += (
#             geoid_undulation  # TODO John things this might work
#         )

#     site_position_llh = GPPositionLLH(
#         latitude=center_llh["latitude"],
#         longitude=center_llh["longitude"],
#         height=center_llh["height"],
#     )
#     site = GPSiteConfig(position_llh=site_position_llh, transponders=transponders)
#     if show_details:
#         show_site_config(site_config=site)
#     return site


# def leverarmfile_to_atdoffset(
#     source: Union[AssetEntry,str,Path], show_details: bool = True
# ) -> GPATDOffset:
#     """
#     Read the ATD offset from a "lever_arms" file
#     format is [rightward,forward,downward] [m]


#     0.0 +0.575 -0.844

#     """
#     if isinstance(source,AssetEntry):
#         assert source.type == AssetType.LEVERARM
#     else:
#         source = AssetEntry(local_path=source,type=AssetType.LEVERARM)
        
#     with open(source.local_path, "r") as f:
#         line = f.readlines()[0]
#         values = [float(x) for x in line.split()]
#         forward = values[1]
#         rightward = values[0]
#         downward = values[2]
#     response = (
#         f"ATD offset (forward, rightward, downward): {forward}, {rightward}, {downward}"
#     )
#     logger.loginfo(response)

#     return GPATDOffset(forward=forward, rightward=rightward, downward=downward)

@pa.check_types(lazy=True)
def CTDfile_to_svp(source: Union[AssetEntry,str,Path]) -> DataFrame[SoundVelocityDataFrame]:

    if isinstance(source,AssetEntry):
        assert source.type == AssetType.CTD

        local_path = source.local_path
    else:
        local_path = source

    df = pd.read_csv(
        local_path, usecols=[0, 1], names=["depth", "speed"], sep="        "
    )
    df.depth = df.depth * -1

    return df
