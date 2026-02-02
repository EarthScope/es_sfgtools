from pathlib import Path
import json
from typing import Dict, List

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

from es_sfgtools.workflows.pipelines import qc_pipeline
from es_sfgtools.workflows.workflow_handler import WorkflowHandler

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

if __name__ == "__main__":
    # qc_path = Path(
    #     "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/sample_qc.json"
    # )
    # shot_df = qcjson_to_shotdata(qc_path)
    # if shot_df is not None:
    #     print(shot_df.head())

    qc_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/Misc/20250812/")
    qc_files = list(qc_dir.glob("*.pin"))
    # all_dfs = []
    # for qc_file in qc_files:
    #     try:
    #         df = qcjson_to_shotdata(qc_file)
    #         if df is not None and not df.empty:
    #             all_dfs.append(df)
    #     except Exception as e:
    #         logger.logerr(f"Error processing {qc_file}: {e}")

    # print(f"\n\nProcessed {len(all_dfs)} QC files.")
    # batched = batch_qc_by_day(all_dfs, date_column='pingTime')
    # for date, df in batched.items():
    #     print(f"Date: {date}, Number of Shots: {len(df)}")

    main_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/TestQC")
    main_dir.mkdir(parents=False, exist_ok=True)
    wfh = WorkflowHandler(directory=main_dir)
    network_id = "cascadia-gorda"
    station_id = "NTH1"
    campaign_id = "2025_A_1126"
    wfh.set_network_station_campaign(
        network_id=network_id,
        station_id=station_id,
        campaign_id=campaign_id,
    )
    wfh.ingest_add_local_data(
        directory_path=qc_dir,
    )
    wfh.ingest_catalog_archive_data()
    wfh.ingest_download_archive_data()
    
    qc_pipeline_wfh = qc_pipeline.QCPipeline(
        directory_handler=wfh.directory_handler,
        asset_catalog=wfh.asset_catalog,
    )
    qc_pipeline_wfh.set_network_station_campaign(
        network_id=network_id,
        station_id=station_id,
        campaign_id=campaign_id,
    )
    import time
    start_time = time.time()
    qc_pipeline_wfh.config = {"override": True}
    qc_pipeline_wfh.process_qc_files()
    end_time = time.time()
    print(f"\n\nQC processing time: {end_time - start_time:.2f} seconds")

    print("QC processing complete.")

    shotdata_uri =qc_pipeline_wfh.shotDataTDB.uri
    qc_mid_processor = wfh.midprocess_get_processor()

    qc_mid_processor.parse_surveys_qc(shotdata_uri=shotdata_uri)