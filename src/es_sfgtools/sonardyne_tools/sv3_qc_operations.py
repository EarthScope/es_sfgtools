from pathlib import Path
import json
from typing import Dict, List

import pandas as pd
from pandera.typing import DataFrame

from es_sfgtools.novatel_tools.rangea_parser import GNSSEpoch

from es_sfgtools.data_models.observables import ShotDataFrame
from es_sfgtools.data_models.sv3_models import NovatelInterrogationEvent, NovatelRangeEvent
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.sonardyne_tools.sv3_operations import (
    novatelInterrogation_to_garpos_interrogation,
    novatelReply_to_garpos_reply,
    merge_interrogation_reply,
)

def qcjson_to_shotdata(source: str | Path) -> DataFrame[ShotDataFrame] | None:
    """Convert a QC.pin file into a ShotDataFrame.

    Parameters
    ----------
    source : str | Path
        Path to the QC.pin file in JSON format.

    Returns
    -------
    DataFrame[ShotDataFrame] | None
        A validated ShotDataFrame if successful, else None.

    The QC file is expected to contain a single top-level .pin object with an
    "interrogation" entry and one or more range entries (keyed by
    transponder IDs). This function mirrors the logic of
    ``dfop00_to_shotdata`` by:

    - Parsing the interrogation block into ``SV3InterrogationData``
    - Parsing each range block into ``SV3ReplyData``
    - Merging interrogation and range data with ``merge_interrogation_reply``
    - Returning a validated ``ShotDataFrame``.
    """

    path = Path(source)

    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except UnicodeDecodeError:
        logger.logwarn(f"UTF-8 decoding failed for {path}, trying latin-1.")
        try:
            with open(path, encoding="latin-1") as f:
                raw = json.load(f)
        except (FileNotFoundError, PermissionError, json.JSONDecodeError) as e:
            logger.logerr(f"Error reading QC JSON {path} with latin-1: {e}")
            return None
    except (FileNotFoundError, PermissionError, json.JSONDecodeError) as e:
        logger.logerr(f"Error reading QC JSON {path}: {e}")
        return None

    interrogation_raw = raw.get("interrogation")
    if interrogation_raw is None:
        logger.logerr(f"QC JSON {path} is missing 'interrogation' block")
        return None

    try:
        interrogation_event = NovatelInterrogationEvent(**interrogation_raw)
        interrogation_parsed = novatelInterrogation_to_garpos_interrogation(
            interrogation_event
        )
    except Exception as e:  # noqa: BLE001
        logger.logerr(f"Failed to parse interrogation block in {path}: {e}")
        return None

    processed: list[dict] = []

    for key, value in raw.items():
        # Skip the interrogation entry and any non-dict values
        if key == "interrogation" or not isinstance(value, dict):
            continue

        if value.get("event") != "range":
            continue

        try:
            reply_event = NovatelRangeEvent(**value)
            reply_parsed = novatelReply_to_garpos_reply(reply_event)
            merged = merge_interrogation_reply(interrogation_parsed, reply_parsed)
        except Exception as e:  # noqa: BLE001
            #logger.logerr(f"Failed to parse/merge range entry '{key}' in {path}: {e}")
            continue

        if merged is not None:
            processed.append(merged)

    if not processed:
        logger.logerr(f"No valid range entries found in QC JSON {path}")
        return None

    df = pd.DataFrame(processed)
    df["isUpdated"] = False

    return ShotDataFrame.validate(df, lazy=True)

def batch_qc_by_day(dataframes:List[pd.DataFrame], date_column:str='pingTime') -> Dict[str, pd.DataFrame]:
    """Batch QC dataframes by day.

    Parameters
    ----------
    dataframes : List[pd.DataFrame]
        List of QC dataframes to be batched.
    date_column : str, optional
        Name of the column containing datetime information, by default 'timestamp'.

    Returns
    -------
    Dict[str, pd.DataFrame]
        Dictionary with keys as date strings (YYYY-MM-DD) and values as concatenated dataframes for that day.
    """
    from collections import defaultdict

    batched_data = defaultdict(list)

    for df in dataframes:
        if date_column not in df.columns:
            logger.logerr(f"DataFrame missing '{date_column}' column.")
            continue
        
        if df.empty:
            continue
        df['date'] = pd.to_datetime(df[date_column].apply(lambda x: x*1e9), utc=True).dt.date

        for date, group in df.groupby('date'):
            batched_data[str(date)].append(group.drop(columns=['date']))

    # Concatenate dataframes for each day
    for date in batched_data:
        batched_data[date] = pd.concat(batched_data[date], ignore_index=True)

    return dict(batched_data)

# def qcjson_to_gnssepochs(source: str | Path) -> List[GNSSEpoch] | None:
#     """Convert a QC.pin file into a list of GNSSEpoch objects.

#     Parameters
#     ----------
#     source : str | Path
#         Path to the QC.pin file in JSON format.

#     Returns
#     -------
#     List[GNSSEpoch] | None
#         A list of GNSSEpoch objects if successful, else None.
#     """
