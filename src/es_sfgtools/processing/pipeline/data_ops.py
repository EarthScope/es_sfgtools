from es_sfgtools.processing.assets import AssetEntry,AssetType,MultiAssetEntry
import sqlalchemy as sa
from typing import List,Union
from pathlib import Path
import pandas as pd
from .database import Base,Assets,MultiAssets


def create_multi_asset(
        engine:sa.Engine,
        network:str,
        station:str,
        survey:str,
        assetType:AssetType,
        writedir:Path,
        override:bool=False) -> Union[List[MultiAssetEntry],List[None]]:
    """Create a new MultiAsset entry in the database

    Args:
        connection (sa.engine.Connection): The database connection
        asset (AssetEntry): The asset entry to create the MultiAsset entry from

    Returns:
        MultiAssets: The newly created MultiAsset entry
    """
    assert assetType in [AssetType.POSITION,AssetType.ACOUSTIC,AssetType.SHOTDATA],f"AssetType {assetType} not supported for MultiAsset creation"
    new_multi_asset_list = []
    with engine.begin() as conn:
        # Get all the individual assets for the given network,station,survey, and asset type
        individual_assets = [AssetEntry(**x._mapping) for x in conn.execute(
            sa.select(Assets).where(
                Assets.network == network,
                Assets.station == station,
                Assets.survey == survey,
                Assets.type == assetType.value
        )
        ).fetchall()
        ].sort(key=lambda x:x.timestamp_data_start)

        dates = [x.timestamp_data_start for x in individual_assets]
        dates.extend([x.timestamp_data_end for x in individual_assets])
        dates = list(set([x.date() for x in dates].sort()))

        date_asset_map = {
            date:[x for x in individual_assets if x.timestamp_data_start.date() == date or x.timestamp_data_end.date() == date] for date in dates
        }
      

        found_multi_asset = [MultiAssetEntry(**x._mapping) for x in  conn.execute(
            sa.select(MultiAssets).where(
                MultiAssets.network == network,
                MultiAssets.station == station,
                MultiAssets.survey == survey,
                MultiAssets.parent_type == assetType.value,
                MultiAssets.timestamp_data_start.in_(dates),
            )
        ).fetchall()]

    
       

        if not override and found_multi_asset:
            while found_multi_asset:
                dates.remove(
                    found_multi_asset.pop().timestamp_data_start.date()
                )
        else:
            for multi_asset in found_multi_asset:
                multi_asset.local_path.unlink()
                conn.execute(
                    sa.delete(MultiAssets).where(
                        MultiAssets.id == multi_asset.id
                    )
                )
        if not dates:
            return []
        
        for date,assets in date_asset_map.items():
            parent_id_string = ",".join([str(x.id) for x in assets])
            merged_df = pd.concat([pd.read_csv(x.local_path) for x in assets])
            for col in merged_df.columns:
                if pd.api.types.is_datetime64_any_dtype(merged_df[col]):
                    merged_df = merged_df[merged_df[col].apply(lambda x: x.date() == date)]

            local_path = writedir / f"{network}_{station}_{survey}_{assetType.value}_{str(date)}.csv"
            merged_df.to_csv(local_path,index=False)
            new_multi_asset = MultiAssetEntry(
                local_path = str(local_path),
                type = assetType,
                network = network,
                station = station,
                survey = survey,
                timestamp_data_start = date,
                timestamp_data_end = date,
                parent_ids = parent_id_string,
            )
            new_multi_asset_list.append(new_multi_asset)
    
    return new_multi_asset_list

            
                 



            

