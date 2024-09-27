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
from ..assets.observables import ShotDataFrame
from ..assets.file_schemas import DFPO00RawFile, QCPinFile
from ..assets.constants import TRIGGER_DELAY_SV3
from ..assets.logmodels import SV3InterrogationData,SV3ReplyData,get_traveltime,get_triggertime,check_sequence_overlap
logger = logging.getLogger(os.path.basename(__file__))





@pa.check_types
def check_df(df: pd.DataFrame) -> pd.DataFrame:
    df = check_sequence_overlap(df)
    return df


def dev_dfop00_to_shotdata(source: DFPO00RawFile) -> DataFrame[ShotDataFrame]:

    processed = []
    interrogation = None
    with open(source.local_path) as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            if data.get("event") == "interrogation":
                interrogation = SV3InterrogationData.from_dfopoo_line(data)

            if data.get("event") == "range" and interrogation is not None:
                range_data = SV3ReplyData.from_dfopoo_line(data)
                if range_data is not None:
                    processed.append((dict(interrogation) | dict(range_data)))
    df = pd.DataFrame(processed)
    return check_df(df)


def dev_qcpin_to_shotdata(source: QCPinFile) -> DataFrame[ShotDataFrame]:
    processed = []
    interrogation = None
    with open(source.local_path, "r") as f:
        try:
            data = json.load(f)
        except json.decoder.JSONDecodeError as e:
            logger.error(f"Error reading {source.local_path} {e}")
            return None
        for key, value in data.items():
            if key == "interrogation":
                interrogation = SV3InterrogationData.from_qcpin_line(value)
            else:
                if interrogation is not None:
                    range_data = SV3ReplyData.from_qcpin_line(value)
                    if range_data is not None:
                        processed.append((dict(interrogation) | dict(range_data)))
    df = pd.DataFrame(processed)
    if df.empty:
        return None
    return check_df(df)
