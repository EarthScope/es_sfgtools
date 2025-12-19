import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
import sqlalchemy as sa

from .schemas import AssetEntry,ConnectionInfo
from es_sfgtools.config.file_config import AssetType
from es_sfgtools.config.env_config import Environment, WorkingEnvironment

from es_sfgtools.logging import ProcessLogger as logger

from .tables import Assets, Base, MergeJobs

from .utils import get_db_connection_info

class PreProcessCatalogHandler:
    """
    A class to handle the preprocessing catalog.
    """
    def __init__(self, db_path: Path = None, db_config: ConnectionInfo = None):
        """Initializes the PreProcessCatalog.

        Parameters
        ----------
        db_path : Path
            The path to the database.
        db_config : ConnectionInfo
            The RDS database connection information.
        """
        match Environment.working_environment():
            case WorkingEnvironment.LOCAL | WorkingEnvironment.GEOLAB:
                if db_path is None:
                    raise ValueError("db_path must be provided for LOCAL environment.")
                self._build_local_sqlite(db_path)
            case WorkingEnvironment.ECS:
                if db_config is None:
                    db_config = get_db_connection_info()
                if db_config is None:
                    raise ValueError("db_config must be provided for GEOLAB or ECS environment.")
                self._create_rds_engine(db_config)
            case _:
                raise ValueError("Unsupported working environment.")

    def _build_local_sqlite(self, db_path: Path):
        """Builds a local SQLite database.

        Parameters
        ----------
        db_path : Path
            The path to the database.
        """
        self.db_path = db_path
        self.engine = self.engine = sa.create_engine(
            f"sqlite+pysqlite:///{self.db_path}", poolclass=sa.pool.NullPool
        )
        Base.metadata.create_all(self.engine)


    def _create_rds_engine(self, db_config: ConnectionInfo):
        """Create SQLAlchemy engine for RDS connection."""

        # Use the actual database name, not the instance identifier
        database_name = (
            getattr(db_config, "database_name", None) or "postgres"
        )  # Default to 'postgres'

        connection_string = (
            f"postgresql://{db_config.username}:{db_config.password}@"
            f"{db_config.host}:{db_config.port}/{database_name}"
        )

        logger.loginfo(f"Connecting to database: {database_name} on {db_config.host}")

        self.connection_info = db_config

        # Add connection timeout and error handling
        self.engine = sa.create_engine(
            connection_string,
            poolclass=sa.pool.QueuePool,
            connect_args={"connect_timeout": 30, "options": "-c timezone=utc"},
        )

        # Test connection before creating tables
        try:
            with self.engine.connect() as conn:
                result = conn.execute(sa.text("SELECT current_database()"))
                current_db = result.fetchone()[0]
                logger.loginfo(f"Successfully connected to database: {current_db}")
        except Exception as e:
            logger.logerr(f"Failed to connect to database: {e}")
            raise

        Base.metadata.create_all(self.engine)

    def get_dtype_counts(self, network:str, station:str, campaign:str, use_local:bool = True,**kwargs) -> Dict[str,int]:
        """Gets the counts of each data type for a given network, station, and campaign.

        Parameters
        ----------
        network : str
            The network name.
        station : str
            The station name.
        campaign : str
            The campaign name.
        use_local : bool, optional
            Whether to only count local assets, by default True.

        Returns
        -------
        Dict[str, int]
            A dictionary of data types and their counts.
        """
        with self.engine.begin() as conn:
            data_type_counts = [
                dict(row._mapping)
                for row in conn.execute(
                    sa.select(sa.func.count(Assets.type), Assets.type)
                    .where(
                        Assets.network.in_([network]),
                        Assets.station.in_([station]),
                        Assets.campaign.in_([campaign]),
                        Assets.local_path.is_not(None) if use_local else True,
                    )
                    .group_by(Assets.type)
                ).fetchall()
            ]
            if len(data_type_counts) == 0:
                return {}
        return {x["type"]: x["count_1"] for x in data_type_counts}

    def delete_entries(self, network: str, station: str, campaign: str, type: AssetType|str, where:str=None) -> None:
        """Deletes entries from the Assets table based on the specified criteria.

        Parameters
        ----------
        network : str
            The network identifier for the assets to be deleted.
        station : str
            The station identifier for the assets to be deleted.
        campaign : str
            The campaign identifier for the assets to be deleted.
        type : AssetType | str
            The type of asset to be deleted. Can be an AssetType enum or a
            string representation.
        where : str, optional
            Additional SQL conditions to filter the assets to be deleted, by
            default None.

        Raises
        ------
        KeyError
            If the provided asset type string is invalid and cannot be mapped
            to an AssetType enum.
        Exception
            If an error occurs during the deletion process.
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
        """Gets assets for a given network, station, campaign, and type.

        Parameters
        ----------
        network : str
            The network name.
        station : str
            The station name.
        campaign : str
            The campaign name.
        type : AssetType | str
            The asset type.

        Returns
        -------
        List[AssetEntry]
            A list of assets.
        """

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
        """Get all svp, ctd and seabird assets for a given station and campaign.

        Parameters
        ----------
        station : str
            The station.
        campaign : str
            The campaign.

        Returns
        -------
        List[AssetEntry]
            A list of AssetEntry objects.
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
    def get_assets(self,
                   network: str,
                   station: str,
                   campaign: str,
                   type: AssetType) -> List[AssetEntry]:
        """Get assets for a given network, station, campaign, and type.

        Parameters
        ----------
        network : str
            The network.
        station : str
            The station.
        campaign : str
            The campaign.
        type : AssetType
            The asset type.

        Returns
        -------
        List[AssetEntry]
            A list of AssetEntry objects.
        """

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
        
    def get_local_assets(self,
                   network: str,
                   station: str,
                   campaign: str,
                   type: AssetType) -> List[AssetEntry]:
        """Get local assets for a given network, station, campaign, and type.

        Parameters
        ----------
        network : str
            The network.
        station : str
            The station.
        campaign : str
            The campaign.
        type : AssetType
            The asset type.

        Returns
        -------
        List[AssetEntry]
            A list of AssetEntry objects.
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
        """Get single entries to process.

        Parameters
        ----------
        network : str
            The network name.
        station : str
            The station name.
        campaign : str
            The campaign name.
        parent_type : AssetType
            The parent asset type.
        child_type : AssetType, optional
            The child asset type, by default None.
        override : bool, optional
            Whether to override existing entries, by default False.

        Returns
        -------
        List[AssetEntry]
            A list of assets.
        """

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
        """Finds an entry in the database.

        Parameters
        ----------
        entry : AssetEntry
            The entry to find.

        Returns
        -------
        AssetEntry | None
            The entry if found, otherwise None.
        """

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
        """Update the local path for an entry in the database.

        Parameters
        ----------
        id : int
            The id of the entry to update.
        local_path : str
            The new local path.
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
        """Check if a remote file name exists in the catalog as a local file.

        Parameters
        ----------
        network : str
            The network.
        station : str
            The station.
        campaign : str
            The campaign.
        type : AssetType
            The asset type.
        remote_path : str
            The remote path.

        Returns
        -------
        bool
            True if the file exists, False if not.
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
        """Adds or updates an entry in the database.

        Parameters
        ----------
        entry : AssetEntry
            The entry to add or update.

        Returns
        -------
        bool
            True if the entry was added or updated, False otherwise.
        """
        if entry is None:
            logger.logwarn("No entry to add or update")
            return

        with self.engine.begin() as conn:
            try:
                conn.execute(
                    sa.insert(Assets).values(entry.to_update_dict())
                )
                return True
            except Exception as e:
                logger.logdebug(f" Entry may already exist, attempting update: {e}")
                try:
                    if Environment.working_environment() == WorkingEnvironment.LOCAL:
                        conn.execute(
                            sa.update(table=Assets)
                            .where(Assets.local_path == str(entry.local_path))
                            .values(entry.to_update_dict()) 
                        )
                        return True
                    else:
                        conn.execute(
                            sa.update(table=Assets)
                            .where(Assets.remote_path == str(entry.remote_path))
                            .values(entry.to_update_dict()) 
                        )
                        return True
                except Exception as e:
                    logger.logerr(f"Error adding or updating entry {entry} to catalog: {e}")
                    pass
        return False

    def query_catalog(self, query: str) -> pd.DataFrame:
        """Queries the catalog.

        Parameters
        ----------
        query : str
            The query to execute.

        Returns
        -------
        pd.DataFrame
            A dataframe with the results.
        """
        with self.engine.begin() as conn:
            try:
                return pd.read_sql_query(query, conn)
            except sa.exc.ResourceClosedError:
                # handle queries that don't return results
                conn.execute(sa.text(query))
    def _does_entry_exist(self, entry:AssetEntry) -> bool:
        """Checks if an entry exists in the database.

        Parameters
        ----------
        entry : AssetEntry
            The entry to check.

        Returns
        -------
        bool
            True if the entry exists, False otherwise.
        """
        with self.engine.begin() as conn:
            results = conn.execute(sa.select(Assets).where(
                Assets.local_path == str(entry.local_path),
                Assets.network == entry.network,
                Assets.station == entry.station,
                Assets.campaign == entry.campaign,
                Assets.type == entry.type.value)).fetchone()
            if results:
                return True
        return False

    def add_entry(self, entry:AssetEntry) -> bool:
        """Adds an entry to the database.

        Parameters
        ----------
        entry : AssetEntry
            The entry to add.

        Returns
        -------
        bool
            True if the entry was added, False otherwise.
        """
        if not self._does_entry_exist(entry):
            try:
                with self.engine.begin() as conn:
                    entry_model = entry.model_dump()
                    entry_model.pop("id",None)  # Remove id if present
                    conn.execute(sa.insert(Assets).values(entry_model))
                return True
            except sa.exc.IntegrityError as e:
                logger.logdebug(f" Integrity error adding entry {entry} to catalog: {e}")
                return False
        return False

    def delete_entry(self, entry:AssetEntry) -> bool:
        """Deletes an entry from the database.

        Parameters
        ----------
        entry : AssetEntry
            The entry to delete.

        Returns
        -------
        bool
            True if the entry was deleted, False otherwise.
        """

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
        """Adds a merge job to the database.

        Parameters
        ----------
        parent_type : str
            The parent asset type.
        child_type : str
            The child asset type.
        parent_ids : List[int]
            The parent asset IDs.
        """
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
        """Checks if a merge job is complete.

        Parameters
        ----------
        parent_type : str
            The parent asset type.
        child_type : str
            The child asset type.
        parent_ids : List[int]
            The parent asset IDs.

        Returns
        -------
        bool
            True if the merge job is complete, False otherwise.
        """
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
