from es_sfgtools.processing.assets import AssetEntry, AssetType, MultiAssetEntry
import sqlalchemy as sa
from typing import List, Union, Callable, Dict
from pathlib import Path
import pandas as pd
from datetime import datetime
from collections import defaultdict
from .database import Assets, MultiAssets
from ..operations.gnss_ops import dev_merge_rinex, rinex_get_meta
import logging

logger = logging.getLogger(__name__)


def create_multi_asset_rinex(
    engine: sa.Engine,
    network: str,
    station: str,
    survey: str,
    writedir: Path,
    override: bool = False,
) -> Union[List[MultiAssetEntry], List[None]]:

    new_multi_asset_list = []


def create_multi_asset_dataframe(
    engine: sa.Engine,
    network: str,
    station: str,
    survey: str,
    assetType: AssetType,
    writedir: Path,
    override: bool = False,
) -> Union[List[MultiAssetEntry], List[None]]:
    """Create a new MultiAsset entry in the database

    Args:
        connection (sa.engine.Connection): The database connection
        asset (AssetEntry): The asset entry to create the MultiAsset entry from

    Returns:
        MultiAssets: The newly created MultiAsset entry
    """
    assert assetType in [
        AssetType.POSITION,
        AssetType.ACOUSTIC,
        AssetType.SHOTDATA,
        AssetType.GNSS,
    ], f"AssetType {assetType} not supported for MultiAsset creation"
    new_multi_asset_list = []
    with engine.begin() as conn:
        # Get all the individual assets for the given network,station,survey, and asset type
        individual_assets = [
            AssetEntry(**x._mapping)
            for x in conn.execute(
                sa.select(Assets).where(
                    Assets.network == network,
                    Assets.station == station,
                    Assets.survey == survey,
                    Assets.type == assetType.value,
                )
            ).fetchall()
        ]
        individual_assets.sort(key=lambda x: x.timestamp_data_start)
        if not individual_assets:
            logger.error(
                f"No assets found for {network} {station} {survey} {assetType.value}"
            )
            return []
        dates = [x.timestamp_data_start for x in individual_assets]
        dates.extend([x.timestamp_data_end for x in individual_assets])
        dates = list(set([x.date() for x in dates if x is not None]))

        date_asset_map = {
            date: [
                x
                for x in individual_assets
                if x is not None
                and (
                    x.timestamp_data_start.date() == date
                )
            ]
            for date in dates
        }

        found_multi_assets = [
            MultiAssetEntry(**x._mapping)
            for x in conn.execute(
                sa.select(MultiAssets).where(
                    MultiAssets.network == network,
                    MultiAssets.station == station,
                    MultiAssets.survey == survey,
                    MultiAssets.parent_type == assetType.value,
                    MultiAssets.timestamp_data_start.in_(dates),
                )
            ).fetchall()
        ]

        if not override and found_multi_assets:
            while found_multi_assets:
                date_asset_map.remove(
                    found_multi_assets.pop().timestamp_data_start.date()
                )
        else:
            for multi_asset in found_multi_assets:
                multi_asset.local_path.unlink()
                conn.execute(
                    sa.delete(MultiAssets).where(MultiAssets.id == multi_asset.id)
                )
        if not date_asset_map:
            return []

        for date, assets in date_asset_map.items():
            if not assets:
                continue
            parent_id_string = ",".join([str(x.id) for x in assets])
            merged_df = pd.concat([pd.read_csv(x.local_path) for x in assets])
            for col in merged_df.columns:
                if pd.api.types.is_datetime64_any_dtype(merged_df[col]):
                    merged_df = merged_df[
                        merged_df[col].apply(lambda x: x.date() == date)
                    ]

            local_path = (
                writedir
                / f"{network}_{station}_{survey}_{assetType.value}_{str(date)}.csv"
            )
            merged_df.to_csv(local_path, index=False)
            new_multi_asset = MultiAssetEntry(
                local_path=str(local_path),
                type=assetType,
                network=network,
                station=station,
                survey=survey,
                timestamp_data_start=date,
                timestamp_data_end=date,
                parent_id=parent_id_string,
                timestamp_created=datetime.now(),
            )
            try:
                conn.execute(sa.insert(MultiAssets).values(new_multi_asset.model_dump()))
            except Exception as e:
                logger.error(f"Error inserting MultiAsset {new_multi_asset} {e}")
                continue
            new_multi_asset_list.append(new_multi_asset)

    return new_multi_asset_list


def create_multi_asset_rinex(
    engine: sa.engine,
    network: str,
    station: str,
    survey: str,
    working_dir: Path,
    ovveride: bool,
) -> Union[List[MultiAssetEntry], List[None]]:

    found_assets = []
    with engine.begin() as conn:
        for x in conn.execute(
            sa.select(MultiAssets).where(
                MultiAssets.network == network,
                MultiAssets.station == station,
                MultiAssets.survey == survey,
                MultiAssets.type == AssetType.RINEX.value,
            )
        ).fetchall():
            try:
                found_assets.append(rinex_get_meta(AssetEntry(**x._mapping)))
            except ValueError as e:
                logger.error(f"Error with RINEX file {x.local_path} {e}")
            
        if not found_assets:
            raise ValueError(
                f"No found assets of type {AssetType.RINEX.value} in Network {network}, Station {station}, Survey {survey}"
            )
        # create date asset map
        doy_asset_map = {}
        for asset in found_assets:
            doy_start, doy_end = (
                asset.timestamp_data_start.timetuple().tm_yday,
                asset.timestamp_data_end.timetuple().tm_yday,
            )
            doy_all_list = list(set(doy_start, doy_end))
            for doy in doy_all_list:
                doy_asset_map.setdefault(doy, []).append(asset)

        rinex_multi_assets = []
        for doy in doy_all_list:
            try:
                rinex_multi_assets.extend(
                    dev_merge_rinex(sources=doy_asset_map[doy], working_dir=working_dir)
                )
            except ValueError as e:
                logger.error(
                    f"Error merging RINEX files for {network} {station} {survey} {doy} {e}"
                )
                pass

        for multi_asset in rinex_multi_assets:
            if not ovveride:
                continue
            else:
                conn.execute(
                    sa.delete(MultiAssets).where(
                        MultiAssets.local_path == str(multi_asset.local_path)
                    )
                )
            conn.execute(sa.insert(MultiAssets).values(multi_asset.model_dump()))
    return rinex_multi_assets


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
