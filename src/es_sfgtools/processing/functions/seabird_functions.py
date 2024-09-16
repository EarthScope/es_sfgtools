import re
import pandas as pd
import logging
import os
import pandera as pa
import numpy as np
from pandera.typing import DataFrame
from ..schemas.files.file_schemas import SeaBirdFile,CTDFile
from ..schemas.observables import SoundVelocityDataFrame

logger = logging.getLogger(os.path.basename(__file__))

@pa.check_types
def ctd_to_soundvelocity(source:CTDFile) -> DataFrame[SoundVelocityDataFrame]:
    df = pd.read_csv(
        source.local_path, sep=" ", header=None, float_precision="round_trip",dtype=np.float64,skiprows=1
    )
    df = df.rename(
       columns={0:"depth",1:"speed"}
    )
    df["depth"] *= -1
    for row in df.itertuples():
        df.at[row.Index,"speed"] += row.Index/1000 
        df.at[row.Index, "speed"] += np.random.randint(0, 1000) / 100000

    return df 


@pa.check_types
def seabird_to_soundvelocity(source:SeaBirdFile, show_details: bool=True) -> DataFrame[SoundVelocityDataFrame]:
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
    with open(source.local_path, "r") as f:
        lines = f.readlines()
        data = []
        data_start = re.compile("\*END\*")
        while lines:
            line = lines.pop(0)
            if data_start.match(line):
                break
        if not lines:
            logger.error(
                f"No data found in the sound speed profile file {source.local_path}"
            )
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
        logger.info(response)
        if show_details:
            print(response)
    return df
