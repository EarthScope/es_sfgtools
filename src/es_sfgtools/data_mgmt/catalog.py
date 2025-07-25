import logging
import os
import sqlalchemy as sa
from typing import List,Dict,Union
from datetime import datetime
from pathlib import Path
import pandas as pd
from es_sfgtools.data_mgmt.file_schemas import AssetEntry, AssetType
from .database import Base, Assets, ModelResults, MergeJobs

from ..logging import ProcessLogger as logger


class PreProcessCatalog:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.engine = self.engine = sa.create_engine(
            f"sqlite+pysqlite:///{self.db_path}", poolclass=sa.pool.NullPool
        )
        Base.metadata.create_all(self.engine)

    def get_dtype_counts(self, network:str, station:str, campaign:str, **kwargs) -> Dict[str,int]:
        with self.engine.begin() as conn:
            data_type_counts = [
                dict(row._mapping)
                for row in conn.execute(
                    sa.select(sa.func.count(Assets.type), Assets.type)
                    .where(
                        Assets.network.in_([network]),
                        Assets.station.in_([station]),
                        Assets.campaign.in_([campaign]),
                        Assets.local_path.is_not(None),
                    )
                    .group_by(Assets.type)
                ).fetchall()
            ]
            if len(data_type_counts) == 0:
                return {"Local files found": 0}
        return {x["type"]: x["count_1"] for x in data_type_counts}

    def delete_entries(self, network: str, station: str, campaign: str, type: AssetType|str, where:str=None) -> None:
        """
        Deletes entries from the Assets table based on the specified criteria.
        Args:
            network (str): The network identifier for the assets to be deleted.
            station (str): The station identifier for the assets to be deleted.
            campaign (str): The campaign identifier for the assets to be deleted.
            type (AssetType | str): The type of asset to be deleted. Can be an AssetType enum or a string representation.
            where (str, optional): Additional SQL conditions to filter the assets to be deleted. Defaults to None.
        Returns:
            None
        Raises:
            KeyError: If the provided asset type string is invalid and cannot be mapped to an AssetType enum.
            Exception: If an error occurs during the deletion process.
   
        """

        if isinstance(type, str):
            try:
                type = AssetType(type)
            except KeyError:
                logger.logerr(f"Invalid asset type {type}")
                return False

        logger.logdebug(f" Deleting assets for {network} {station} {campaign} {str(type)}")

        with self.engine.begin() as conn:
            try:
                statement = sa.and_(
                    Assets.network == network,
                    Assets.station == station,
                    Assets.campaign == campaign,
                    Assets.type == type.value
                )
                if where:
                    statement = sa.and_(statement, sa.text(where))
                conn.execute(
                    sa.delete(Assets).where(statement)
                )
                return True
            except Exception as e:
                logger.logerr(f"Error deleting entries: {e}")
                return False
            
    def get_assets(self,
                   network: str,
                   station: str,
                   campaign: str,
                   type: AssetType|str) -> List[AssetEntry]:

        if isinstance(type,str):
            try:
                type = AssetType(type)
            except KeyError:
                logger.logerr(f"Invalid asset type {type}")
                return []

        logger.logdebug(f" Getting assets for {network} {station} {campaign} {str(type)}")

        with self.engine.connect() as conn:
            query = sa.select(Assets).where(
                sa.and_(
                    Assets.network == network,
                    Assets.station == station,
                    Assets.campaign == campaign,
                    Assets.type == type.value
                )
            )
            result = conn.execute(query).fetchall()
            out = []
            for row in result:
                try:
                    out.append(AssetEntry(**row._mapping))
                except Exception as e:
                    logger.logerr("Unable to add row, error: {}".format(e))
            return out
        
    def get_ctds(self, station: str, campaign: str) -> List[AssetEntry]:
        """
        Get all svp, ctd and seabird assets for a given station and campaign.
        
        Args:
            station (str): The station.
            campaign (str): The campaign.
            
        Returns:
            List[AssetEntry]: A list of AssetEntry objects.    
        """

        logger.logdebug(f" Getting ctds for {station} {campaign}")

        with self.engine.connect() as conn:
            query = sa.select(Assets).where(
                sa.and_(
                    Assets.station == station,
                    Assets.campaign == campaign,
                    Assets.type.in_([AssetType.CTD.value, AssetType.SEABIRD.value, AssetType.SVP.value])
                )
            )
            result = conn.execute(query).fetchall()
            out = []
            for row in result:
                try:
                    entry = AssetEntry(**row._mapping)
                    out.append(entry)
                except Exception as e:
                    logger.logerr("Unable to add row, error: {}".format(e))
            return out

    def get_local_assets(self,
                   network: str,
                   station: str,
                   campaign: str,
                   type: AssetType) -> List[AssetEntry]:
        """
        Get local assets for a given network, station, campaign, and type.
        
        Args:
            network (str): The network.
            station (str): The station.
            campaign (str): The campaign.
            type (AssetType): The asset type.
            
        Returns:
            List[AssetEntry]: A list of AssetEntry objects.    
        """

        logger.logdebug(f" Getting local assets for {network} {station} {campaign} {str(type)}")

        with self.engine.connect() as conn:
            query = sa.select(Assets).where(
                sa.and_(
                    Assets.network == network,
                    Assets.station == station,
                    Assets.campaign == campaign,
                    Assets.type == type.value,
                    Assets.local_path.isnot(None)
                )
            )
            result = conn.execute(query).fetchall()
            out = []
            for row in result:
                try:
                    out.append(AssetEntry(**row._mapping))
                except Exception as e:
                    logger.logerr("Unable to add row, error: {}".format(e))
            return out

    def get_single_entries_to_process(self,
                               network:str,
                               station:str,
                               campaign:str,
                               parent_type:AssetType,
                               child_type:AssetType = None,
                               override:bool=False) -> List[AssetEntry]:

        parent_entries = self.get_assets(network,station,campaign,parent_type)
        if child_type is None:
            if override:
                return parent_entries
            return [entry for entry in parent_entries if not entry.is_processed]

        child_entries = self.get_assets(network,station,campaign,child_type)
        parent_id_map = {entry.id:entry for entry in parent_entries}
        if not override:
            [parent_id_map.pop(child_entry.parent_id) for child_entry in child_entries if child_entry.parent_id in parent_id_map]

        return list(parent_id_map.values())

    def find_entry(self, entry: AssetEntry ) -> AssetEntry | None:

        with self.engine.connect() as conn:
            results = conn.execute(sa.select(Assets).where(
                Assets.parent_id == ",".join([str(x) for x in entry.parent_id]),
                Assets.network == entry.network,
                Assets.station == entry.station,
                Assets.campaign == entry.campaign,
                Assets.type == entry.type.value)).fetchone()
            if results:
                return AssetEntry(**results._mapping)
        return None

    def update_local_path(self, id, local_path: str):
        """ 
        Update the local path for an entry in the database. 
        
        Args:
            id (int): The id of the entry to update.
            local_path (str): The new local path.
        """
        try:
            logger.loginfo(f"Updating local path in catalog for id {id} to {local_path}")
            with self.engine.begin() as conn:
                conn.execute(
                    sa.update(Assets)
                    .where(Assets.id == id)
                    .values(local_path=local_path)
                )
        except Exception as e:
            logger.logerr(f"Error updating local path for id {id}: {e}")

    def remote_file_exist(self, network: str, station: str, campaign: str, type: AssetType, remote_path: str) -> bool:
        """
        Check if a remote file name exists in the catalog already as a local file name.
        
        Args:
            network (str): The network.
            station (str): The station.
            campaign (str): The campaign.
            type (AssetType): The asset type.
            remote_path (str): The remote path.
            
        Returns:
            bool: True if the file exists, False if not.
        """

        remote_file_name = os.path.basename(remote_path)

        with self.engine.connect() as conn:
            results = conn.execute(sa.select(Assets).where(
                sa.and_(
                Assets.network == network,
                Assets.station == station,
                Assets.campaign == campaign,
                Assets.type == type.value,
                Assets.local_path.like(f"%{remote_file_name}%")
            ))).fetchone()

            if results:
                return True

        return False

    def add_or_update(self, entry: AssetEntry ) -> bool:
        if entry is None:
            logger.logwarn("No entry to add or update")
            return

        with self.engine.begin() as conn:
            try:
                conn.execute(
                    sa.insert(Assets).values(entry.model_dump()))
                return True
            except Exception as e:
                try:

                    conn.execute(
                        sa.update(table=Assets)
                        .where(Assets.local_path.is_(str(entry.local_path)))
                        .values(entry.to_update_dict()) 
                    )
                    return True
                except Exception as e:
                    logger.logerr(f"Error adding or updating entry {entry} to catalog: {e}")
                    pass
        return False

    def query_catalog(self, query: str) -> pd.DataFrame:
        with self.engine.begin() as conn:
            try:
                return pd.read_sql_query(query, conn)
            except sa.exc.ResourceClosedError:
                # handle queries that don't return results
                conn.execute(sa.text(query))

    def add_entry(self, entry:AssetEntry) -> bool:
        try:
            with self.engine.begin() as conn:
                conn.execute(sa.insert(Assets).values(entry.model_dump()))
            return True
        except sa.exc.IntegrityError as e:
            logger.logdebug(f" Integrity error adding entry {entry} to catalog: {e}")
            return False

    def delete_entry(self, entry:AssetEntry) -> bool:

        logger.loginfo(f"Deleting entry {entry} from catalog")
        with self.engine.begin() as conn:
            try:
                conn.execute(sa.delete(Assets).where(
                    Assets.id == entry.id
                ))
                return True
            except Exception as e:
                logger.logerr(f"Error deleting entry {entry} | {e}")
        return False

    def add_merge_job(self,parent_type:str,child_type:str,parent_ids:List[int],**kwargs):
        # sort parent_ids to ensure that the order is consistent
        parent_ids.sort()
        parent_id_string = "-".join([str(x) for x in parent_ids])
        with self.engine.begin() as conn:
            conn.execute(sa.insert(MergeJobs).values({
                MergeJobs.parent_type.name: parent_type,
                MergeJobs.child_type.name: child_type,
                MergeJobs.parent_ids.name: parent_id_string
            }))

    def is_merge_complete(self,parent_type:str,child_type:str,parent_ids:List[int],**kwargs) -> bool:
        parent_ids.sort()
        parent_id_string = "-".join([str(x) for x in parent_ids])
        with self.engine.begin() as conn:
            results = conn.execute(sa.select(MergeJobs).where(
                MergeJobs.parent_type == parent_type,
                MergeJobs.child_type == child_type,
                MergeJobs.parent_ids == parent_id_string
            )).fetchone()
            if results:
                return True
        return False

