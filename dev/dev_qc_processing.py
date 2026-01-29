from pathlib import Path
import json

import pandas as pd
from pandera.typing import DataFrame

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
            logger.logerr(f"Failed to parse/merge range entry '{key}' in {path}: {e}")
            continue

        if merged is not None:
            processed.append(merged)

    if not processed:
        logger.logerr(f"No valid range entries found in QC JSON {path}")
        return None

    df = pd.DataFrame(processed)
    df["isUpdated"] = False

    return ShotDataFrame.validate(df, lazy=True)


if __name__ == "__main__":
    qc_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/sample_qc.json"
    )
    shot_df = qcjson_to_shotdata(qc_path)
    if shot_df is not None:
        print(shot_df.head())
