# External imports
from pathlib import Path
import json
from pandera.typing import DataFrame
import pandas as pd
# Local imports
from ..data_models.log_models import SV3InterrogationData, SV3ReplyData
from ..data_models.observables import ShotDataFrame
from ..logging import ProcessLogger as logger

def dfop00_to_shotdata(source: str | Path) -> DataFrame[ShotDataFrame] | None:
    """
    Parses a DFOP00-formatted file and converts it into a pandas DataFrame containing acoustic ping-reply sequences.
    Args:
        source (str | Path): Path to the DFOP00-formatted file to be processed.
    Returns:
        DataFrame[ShotDataFrame] | None: DataFrame containing ping-reply sequences

    """
    
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

    if not processed:
        logger.logerr(f"No valid data found in {source.local_path}")
        return None
    df = pd.DataFrame(processed)
    df["isUpdated"] = False
    return ShotDataFrame(df)


def qcpin_to_shotdata(
    source: str | Path,
) -> DataFrame[ShotDataFrame] | None:

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
    return ShotDataFrame(df)
