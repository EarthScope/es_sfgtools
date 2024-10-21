from es_sfgtools.processing.assets.file_schemas import AssetEntry, AssetType, MultiAssetEntry,MultiAssetPre
from es_sfgtools.processing.assets.observables import ShotDataFrame, PositionDataFrame, AcousticDataFrame, GNSSDataFrame
import sqlalchemy as sa
from typing import List, Union, Callable, Dict
from pathlib import Path
import pandas as pd
from datetime import datetime
from collections import defaultdict
from .database import Assets, MultiAssets
from ..operations.gnss_ops import dev_merge_rinex, rinex_get_meta,dev_merge_rinex_multiasset
import logging

logger = logging.getLogger(__name__)

class PridePdpConfig:
    def __init__(self, config: Dict):
        self.config = config


ASSET_DF_MAP = {
    AssetType.POSITION: PositionDataFrame,
    AssetType.ACOUSTIC: AcousticDataFrame,
    AssetType.SHOTDATA: ShotDataFrame,
    AssetType.GNSS: GNSSDataFrame,
}

def create_multi_asset_dataframe(
    assets: List[AssetEntry], writedir: Path
) -> List[MultiAssetEntry]:

    assert (
        len(list(set([x.type for x in assets]))) == 1
    ), "All assets must be of the same type"
    assert (
        len(list(set([x.network for x in assets]))) == 1
    ), "All assets must be of the same network"
    assert (
        len(list(set([x.station for x in assets]))) == 1
    ), "All assets must be of the same station"
    assert (
        len(list(set([x.survey for x in assets]))) == 1
    ), "All assets must be of the same survey"

    response = f"\n *** Creating MultiAsset DataFrame: \n Creating MultiAsset from {len(assets)} for {assets[0].network} {assets[0].station} {assets[0].survey} {assets[0].type}"
    logger.info(response)

    dates = [x.timestamp_data_start for x in assets]
    dates.extend([x.timestamp_data_end for x in assets])
    dates = list(set([x.date() for x in dates if x is not None]))
    logger.info(f"Merging on dates {[str(x) for x in dates]}")
    date_asset_map = {}
    for date in dates:
        date_assets_id = ",".join(
            [str(x.id) for x in assets if ((x.timestamp_data_start.date() == date) or (x.timestamp_data_end.date() == date))]
        )
        date_asset_map[date] = date_assets_id

    merged_df = pd.concat([pd.read_csv(x.local_path) for x in assets])
    merged_df = ASSET_DF_MAP[assets[0].type].validate(merged_df,lazy=True)
    time_col = None
    for col in merged_df.columns:
        # get the date of the datetime column
        if pd.api.types.is_datetime64_any_dtype(merged_df[col]):
            temp_date: pd.Series = merged_df[col].apply(lambda x: x.date())
            time_col = col
            logger.info(f"Merging on column {col}")
            break
    if time_col is None:
        raise ValueError("No datetime column found in DataFrame")
    new_multi_asset_list = []
    for date, ids_str in date_asset_map.items():
        temp_df = merged_df[temp_date == date]
        timestamp_data_start = temp_df[time_col].min()
        timestamp_data_end = temp_df[time_col].max()
        local_path = writedir / f"{assets[0].type.value}_{ids_str}_{str(date)}.csv"
        temp_df.to_csv(local_path, index=False)

        logger.info(
            f"Writing DF of type {assets[0].type} with {temp_df.shape[0]} entries to {str(local_path)}"
        )

        new_multi_asset = MultiAssetEntry(
            local_path=str(local_path),
            type=assets[0].type,
            network=assets[0].network,
            station=assets[0].station,
            survey=assets[0].survey,
            timestamp_data_start=timestamp_data_start,
            timestamp_data_end=timestamp_data_end,
            parent_id=ids_str,
            timestamp_created=datetime.now(),
        )
        new_multi_asset_list.append(new_multi_asset)
    logger.info(f"\n *** Created {len(new_multi_asset_list)} MultiAssets \n")
    return new_multi_asset_list

def dev_create_multi_asset_dataframe(
        multi_asset_pre:MultiAssetPre,
        working_dir:Path) -> MultiAssetEntry:
    
    assert multi_asset_pre.child_type in [
        AssetType.POSITION,
        AssetType.ACOUSTIC,
        AssetType.SHOTDATA,
        AssetType.GNSS,
    ], f"AssetType {multi_asset_pre.child_type} not supported for MultiAsset creation"

    merged_df = pd.concat([pd.read_csv(x) for x in multi_asset_pre.source_paths])
    merged_df = ASSET_DF_MAP[multi_asset_pre.child_type].validate(merged_df,lazy=True)
    if merged_df.empty:
        raise ValueError(f"Empty DataFrame for {multi_asset_pre.network} {multi_asset_pre.station} {multi_asset_pre.survey} {multi_asset_pre.child_type}")
    time_col = None
    for col in merged_df.columns:
        # get the date of the datetime column
        if pd.api.types.is_datetime64_any_dtype(merged_df[col]):
            temp_date: pd.Series = merged_df[col].apply(lambda x: x.date())
            time_col = col
            logger.info(f"Merging on column {col}")
            break
    if time_col is None:
        raise ValueError("No datetime column found in DataFrame")
    timestamp_data_start = merged_df[time_col].min()
    timestamp_data_end = merged_df[time_col].max()
    ids_str = ",".join([str(x) for x in multi_asset_pre.parent_id])
    local_path = working_dir / f"{multi_asset_pre.network}_{multi_asset_pre.station}_{multi_asset_pre.survey}_{multi_asset_pre.child_type.value}_{ids_str}_{str(timestamp_data_start.date())}.csv"

    merged_df.to_csv(local_path, index=False)
    new_multi_asset = MultiAssetEntry(
        local_path=str(local_path),
        type=multi_asset_pre.child_type,
        network=multi_asset_pre.network,
        station=multi_asset_pre.station,
        survey=multi_asset_pre.survey,
        timestamp_data_start=timestamp_data_start,
        timestamp_data_end=timestamp_data_end,
        parent_id=ids_str,
        timestamp_created=datetime.now(),
    )
    return new_multi_asset





# def merge_multi_assets(
#         engine:sa.Engine,
#         network:str,
#         station:str,
#         survey:str,
#         parent_asset_types:List[AssetType],
#         child_asset_type:AssetType,
#         process_func:Callable[[pd.DataFrame],pd.DataFrame],
#         func_map:Dict[str,AssetType],
#         writedir:Path,
#         override:bool=False) -> Union[List[MultiAssetEntry],List[None]]:

#     new_multi_asset_list = []
#     asset_date_map = defaultdict(dict)
#     with engine.begin() as conn:

#         found_multi_assets = [MultiAssetEntry(**x._mapping) for x in  conn.execute(
#         sa.select(MultiAssets).where(
#             MultiAssets.network == network,
#             MultiAssets.station == station,
#             MultiAssets.survey == survey,
#             MultiAssets.type.in_([x.value for x in parent_asset_types]),
#         )
#         ).fetchall()]
#         if not found_multi_assets:
#             logger.error(f"No MultiAssets found for {network} {station} {survey} {[x.value for x in parent_asset_types]}")
#             return []
#         # Populate the asset_date_map with the MultiAssets
#         # dict[date][asset_type] = MultiAssetEntry
#         for multi_asset in found_multi_assets:
#             asset_date_map[multi_asset.timestamp_data_start.date()][multi_asset.type.value] = multi_asset

#         for date_key,asset_map in asset_date_map.items():
#             if len(asset_map) != len(parent_asset_types):
#                 logger.error(f"Missing MultiAsset for {network} {station} {survey} {parent_asset_types} {date_key}")
#                 continue
#             func_map_args = {k:asset_map[v.value] for k,v in func_map.items()}
#             for key,asset in func_map_args.items():
#                 if not asset.local_path.exists():
#                     logger.error(f"File {asset.local_path} does not exist")
#                     continue
#                 func_map_args[key] = pd.read_csv(asset.local_path)

#             merged_df: Union[pd.DataFrame,None] = process_func(**func_map_args)
#             if merged_df is None:
#                 continue
#             parent_id_string = ",".join([str(x.id) for x in asset_map.values()])
#             local_path = writedir / f"{network}_{station}_{survey}_{child_asset_type.value}_{str(date_key)}.csv"
#             merged_df.to_csv(local_path,index=False)
#             new_multi_asset = MultiAssetEntry(
#                 local_path = str(local_path),
#                 type = child_asset_type,
#                 network = network,
#                 station = station,
#                 survey = survey,
#                 timestamp_data_start = date_key,
#                 timestamp_data_end = date_key,
#                 parent_id = parent_id_string,
#             )
#             # Delete any existing MultiAssets for the given date
#             conn.execute(
#                 sa.delete(MultiAssets).where(
#                     MultiAssets.network == network,
#                     MultiAssets.station == station,
#                     MultiAssets.survey == survey,
#                     MultiAssets.timestamp_data_start == date_key,
#                     MultiAssets.type == child_asset_type.value
#                 )
#             )
#             conn.execute(
#                 sa.insert(MultiAssets).values(new_multi_asset.model_dump())
#             )
#             new_multi_asset_list.append(new_multi_asset)
#     return new_multi_asset_list
