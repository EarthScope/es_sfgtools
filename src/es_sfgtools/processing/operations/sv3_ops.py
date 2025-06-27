import pandas as pd
from datetime import timedelta
from typing import Union
from pathlib import Path

import json
from pandera.typing import DataFrame
from ..assets.observables import ShotDataFrame
from ..assets.file_schemas import AssetEntry,AssetType
from ..assets.constants import TRIGGER_DELAY_SV3
from ..assets.logmodels import SV3InterrogationData,SV3ReplyData

from es_sfgtools.utils.loggers import ProcessLogger as logger

def dev_dfop00_to_shotdata(source: Union[AssetEntry,str,Path]) -> DataFrame[ShotDataFrame] | None:
    if isinstance(source,AssetEntry):
        assert source.type == AssetType.DFOP00

    else:
        source = AssetEntry(local_path=source,type=AssetType.DFOP00)

    processed = []
    interrogation = None
    with open(source.local_path) as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            if data.get("event") == "interrogation":
                interrogation = SV3InterrogationData.from_DFOP00_line(data)

            if data.get("event") == "range" and interrogation is not None:
                reply_data = SV3ReplyData.from_DFOP00_line(data)
                if reply_data is not None:
                    processed.append((dict(interrogation) | dict(reply_data)))
                else:
                    logger.logdebug(
                        f"Range data not found for interrogation at {interrogation.pingTime} in {source.local_path}"
                    )
    if not processed:
        return None
    df = pd.DataFrame(processed)
    df["isUpdated"] = False
    return df


def dev_qcpin_to_shotdata(source: Union[AssetEntry,str,Path]) -> DataFrame[ShotDataFrame]:
    if isinstance(source,AssetEntry):
        assert source.type == AssetType.QCPIN
    else:
        source = AssetEntry(local_path=source,type=AssetType.QCPIN)

    processed = []
    interrogation = None
    with open(source.local_path, "r") as f:
        try:
            data = json.load(f)
        except json.decoder.JSONDecodeError as e:
            logger.logerr(f"Error reading {source.local_path} {e}")
            return None
        for key, value in data.items():
            if key == "interrogation":
                interrogation = SV3InterrogationData.from_qcpin_line(value)
            else:
                if interrogation is not None:
                    range_data = SV3ReplyData.from_qcpin_line(value)
                    if range_data is not None:
                        processed.append((dict(interrogation) | dict(range_data)))
    if not processed:
        return None
    df = pd.DataFrame(processed)
    df["isUpdated"] = False
    return df
