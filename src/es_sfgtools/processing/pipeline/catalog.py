import logging
import sqlalchemy as sa
from typing import List,Dict
from datetime import datetime
from pathlib import Path
import pandas as pd
from es_sfgtools.processing.assets.file_schemas import AssetEntry, AssetType,MultiAssetEntry,MultiAssetPre
from .database import Base, Assets, MultiAssets, ModelResults

logger = logging.getLogger(__name__)


class Catalog:
    def __init__(self,db_path:Path):
        self.db_path = db_path
        self.engine = self.engine = sa.create_engine(
            f"sqlite+pysqlite:///{self.db_path}", poolclass=sa.pool.NullPool
        )
        Base.metadata.create_all(self.engine)
    
    def get_dtype_counts(self,network:str,station:str,survey:str,**kwargs) -> Dict[str,int]:
        with self.engine.begin() as conn:
            data_type_counts = [
                dict(row._mapping)
                for row in conn.execute(
                    sa.select(sa.func.count(Assets.type), Assets.type)
                    .where(
                        Assets.network.in_([network]),
                        Assets.station.in_([station]),
                        Assets.survey.in_([survey]),
                        Assets.local_path.is_not(None),
                    )
                    .group_by(Assets.type)
                ).fetchall()
            ]
            if len(data_type_counts) == 0:
                return {"Local files found": 0}
        return {x["type"]: x["count_1"] for x in data_type_counts}
    
    def get_assets(self,
                   network:str,
                   station:str,
                   survey:str,
                   asset_type:AssetType,
                   multiasset:bool=False) -> List[AssetEntry | MultiAssetEntry]:

        if multiasset:
            table = MultiAssets
            schema = MultiAssetEntry
        else:
            table = Assets
            schema = AssetEntry
        with self.engine.connect() as conn:
            query = sa.select(table).where(
                sa.and_(
                    table.network == network,
                    table.station == station,
                    table.survey == survey,
                    table.type == asset_type.value
                )
            )
            result = conn.execute(query).fetchall()
            return [schema(**row._mapping) for row in result]

    def get_single_entries_to_process(self,
                               network:str,
                               station:str,
                               survey:str,
                               parent_type:AssetType,
                               child_type:AssetType,
                               override:bool=False) -> List[AssetEntry]:

        parent_entries = self.get_assets(network,station,survey,parent_type)
        child_entries = self.get_assets(network,station,survey,child_type)
        parent_id_map = {entry.id:entry for entry in parent_entries}
        if not override:
            [parent_id_map.pop(child_entry.parent_id) for child_entry in child_entries if child_entry.parent_id in parent_id_map]
        return list(parent_id_map.values())

    def get_multi_entries_to_process(self,
                                 network:str,
                                 station:str,
                                 survey:str,
                                 parent_type:AssetType,
                                 child_type:AssetType,
                                 override:bool=False) -> List[MultiAssetPre]:

        parent_entries: List[AssetEntry] = self.get_assets(network,station,survey,parent_type,multiasset=False)
        doy_ma_map: Dict[datetime.date : MultiAssetPre] = MultiAssetPre.from_asset_list(parent_entries,child_type=child_type)
        # Search for the multiasset entries that are already in the database
        to_remove = []
        for doy,multiasset_pre in doy_ma_map.items():
            matching_entry = self.find_entry(multiasset_pre[0].to_multiasset())
            if matching_entry and not override:
                to_remove.append(doy)
        [doy_ma_map.pop(doy) for doy in to_remove]

        return [x[0] for x in doy_ma_map.values()]

    def find_entry(self,entry:AssetEntry | MultiAssetEntry) -> AssetEntry | MultiAssetEntry | None:
        table = Assets if isinstance(entry, AssetEntry) else MultiAssets
        schema = AssetEntry if isinstance(entry, AssetEntry) else MultiAssetEntry
        with self.engine.connect() as conn:
            results = conn.execute(sa.select(table).where(
                table.parent_id == ",".join([str(x) for x in entry.parent_id]),
                table.network == entry.network,
                table.station == entry.station,
                table.survey == entry.survey,
                table.type == entry.type.value)).fetchone()
            if results:
                return schema(**results._mapping)
        return None

    def add_or_update(self,entry: AssetEntry | MultiAssetEntry):
        if entry is None:
            return
        table = Assets if isinstance(entry, AssetEntry) else MultiAssets
    
        with self.engine.begin() as conn:

            try:
                conn.execute(
                    sa.insert(table).values(entry.model_dump()))
            except Exception as e:
                print(e)
                try:
                    conn.execute(
                        sa.update(table=table)
                        .where(table.local_path.is_(str(entry.local_path)))
                        .values(entry.model_dump()) 
                    )
                except Exception as e:
       
                    logger.error(f"Error adding or updating entry {entry}")
                    pass

    def query_catalog(self,
                      query:str) -> pd.DataFrame:
        with self.engine.begin() as conn:
            try:
                return pd.read_sql_query(query,conn)
            except sa.exc.ResourceClosedError:
                # handle queries that don't return results
                conn.execute(sa.text(query))
    
    def add_entry(self,entry:AssetEntry | MultiAssetEntry) -> bool:
        table = Assets if isinstance(entry, AssetEntry) else MultiAssets
        try:
            with self.engine.begin() as conn:
                conn.execute(sa.insert(table).values(entry.model_dump()))
            return True
        except sa.exc.IntegrityError:
            return False
