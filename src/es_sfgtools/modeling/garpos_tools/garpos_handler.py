# GarposHandler class for processing and preparing shot data for the GARPOS model.

from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple, Union
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import json
import os

from es_sfgtools.data_mgmt.directory_handler import DirectoryHandler
from es_sfgtools.data_models.metadata.catalogs import StationData
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.data_models.metadata.campaign import Survey

from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposFixed,
    InversionParams,
    GarposInput,
)
try:
    from garpos import drive_garpos
except ImportError:
    # Handle the case where garpos is not available
    pass

from es_sfgtools.modeling.garpos_tools.functions import CoordTransformer,process_garpos_results
from es_sfgtools.utils.archive_pull import download_file_from_archive
from ...logging import GarposLogger as logger
from ...seafloor_site_tools.soundspeed_operations import CTDfile_to_svp, seabird_to_soundvelocity
from es_sfgtools.data_mgmt.catalog import PreProcessCatalog
from es_sfgtools.data_mgmt.file_schemas import AssetEntry, AssetType

from .garpos_runner import GarposRunner
from .data_prep import prepareShotData
from .garpos_results_processor import GarposResultsProcessor
from .garpos_config import DEFAULT_FILTER_CONFIG, DEFAULT_INVERSION_PARAMS


DEFAULT_SETTINGS_FILE_NAME = "default_settings.ini"
SVP_FILE_NAME = "svp.csv"
SURVEY_METADATA_FILE_NAME = "survey_meta.json"


class GarposHandler:
    """
    GarposHandler is a class that handles the processing and preparation of shot data for the GARPOS model.
    It includes methods for rectifying shot data, preparing shot data files, setting inversion parameters,
    generating observation parameter files, generating data files with fixed parameters, and running the GARPOS model.

    Note:
        Respective instances of this class are intended to be for individual stations only.

    Attributes:
        shotdata (TDBShotDataArray): Array containing shot data.
        site_config (SiteConfig): Configuration for the site.
        working_dir (Path): Working directory path. Typically set to the campaign directory.
        shotdata_dir (Path): Directory path for shot data.
        results_dir (Path): Directory path for results.
        inversion_params (InversionParams): Parameters for the inversion process.
        dates (list): List of unique dates from the shot data.
        coord_transformer (CoordTransformer): Coordinate transformer for converting coordinates.

    Methods:
        __init__(self, shotdata: TDBShotDataArray, site_config: SiteConfig, working_dir: Path):
            Initializes the GarposHandler with shot data, site configuration, and working directory.
        prep_shotdata(self, overwrite: bool = False):
            Prepares and saves shot data for each date in the object's date list.
        set_inversion_params(self, parameters: dict | InversionParams):
            Sets inversion parameters for the model.
        run_garpos(self, date_index: int = None, run_id: int | str = 0) -> None:
            Runs the GARPOS model for a specific date or for all dates.
    """

    def __init__(self,
                 main_directory: Path):
        """
        Initializes the class with shot data, site configuration, and working directory.
        Args:
            network (str): The network name.
            station (str): The station name.
            campaign (str): The campaign name.
            station_data (StationData): The station data containing shot data
            site_config (SiteConfig): The site configuration.
            working_dir (Path): The directory path to store results and shot data. *data handler sets this to the campaign directory*
        """

        self.garpos_fixed = GarposFixed()
        self.directory_handler = DirectoryHandler(location=main_directory)

        self.site: Site = None
        self.station_data = None

        self.working_dir = None

        self.network =  None
        self.station = None
        self.campaign = None
        self.current_survey = None
        self.coord_transformer = None

    def set_site_data(
        self,
        network: str,
        station: str,
        campaign: str,
        site: Site,
    ):
        self.network = network
        self.station = station
        self.campaign = campaign
        self.site = site
        self._setup_campaign()

        self.garpos_fixed._to_datafile(
            path=self.working_dir / DEFAULT_SETTINGS_FILE_NAME
        )
        logger.loginfo(
            f"Garpos Handler initialized with working directory: {self.working_dir}"
        )

    def _setup_campaign(self):
        """
        This function sets up the campaign by finding the campaign with the given name in the site metadata. Grab the array center coordinates and
        set up the coordinate transformer for the site. If the campaign is not found, raise a ValueError.

        Raises:
            ValueError: If the campaign with the given name is not found in the site metadata.
        """
        for campaign in self.site.campaigns:
            if campaign.name == self.campaign:
                self.campaign = campaign
                self.coord_transformer = CoordTransformer(
                    latitude=self.site.arrayCenter.latitude,
                    longitude=self.site.arrayCenter.longitude,
                    elevation=-float(
                        self.site.localGeoidHeight
                    ),  # use negatiive value to account for garpos error "ys is shallower than layer" TODO: <--?
                )
                self.current_survey = None

                logger.loginfo(
                    f"Campaign {self.campaign.name} set. Current campaign directory: {self.working_dir}"
                )
                self.directory_handler.build_station_directory(
                    network_name=self.network,
                    station_name=self.station,
                    campaign_name=self.campaign.name,
                )
                self.directory_handler[self.network][self.station][
                    self.campaign.name
                ].garpos.build()
                
                self.working_dir = self.directory_handler[self.network][self.station][self.campaign.name].garpos.location

                return

        raise ValueError(
            f"campaign {self.campaign} not found among: {[x.name for x in self.site.campaigns]}"
        )

    def load_sound_speed_data(
        self, local_svp: Optional[Path] = None, local_ctd: Optional[Path] = None
    ):
        """
        Load the sound speed profile from a local file or from the catalog.
        Args:
            local_svp (Path): The path to the local sound speed profile file. Default is None.
            local_ctd (Path): The path to the local CTD file. Default is None.
        """
        if local_svp:
            self.sound_speed_path = Path(local_svp)
            logger.loginfo(f"Using local sound speed profile found at {local_svp}..")
        elif local_ctd:
            logger.loginfo(
                f"Using local CTD file found at {local_ctd}, converting to sound speed profile.."
            )
            df = CTDfile_to_svp(source=local_ctd)
            df.to_csv(self.sound_speed_path, index=False)
            logger.loginfo(
                f"Converted {local_ctd} to sound velocity profile at {self.sound_speed_path}"
            )
        else:
            self._check_CTDs_in_catalog()

    def _check_CTDs_in_catalog(self, catalog_db_path: Optional[Path] = None):
        """
        This function will check the catalog database for SVP or CTD files related to the current campaign. If found and local, set as sound
        speed file or convert to SVP. If only remote, download it first and then set or convert to sound speed profile.

        If no files are found in catalog, raise an error.

        Args:
            catalog_db_path (Path): The path to the catalog database. Default is None. Will check in local working directory if not provided.

        Raises:
            ValueError: If no CTD files are found for the campaign in the catalog or if the catalog database path is not found or provided.
        """

        if not catalog_db_path:
            catalog_db_path = self.working_dir.parents[3] / "catalog.sqlite"
            logger.logdebug(
                f"Catalog database path not provided, checking for local catalog database at: {str(catalog_db_path)}"
            )
            if not catalog_db_path.exists():
                raise ValueError(
                    "No local SVP found and no catalog database path provided, "
                    "\n please provide the catalog database path to check for the CTD files or a local SVP file"
                )
            else:
                logger.logdebug(
                    f"Using local catalog database found at {catalog_db_path}.."
                )

        logger.loginfo(
            f"Checking catalog database for SVP, CTD, and SEABIRD files related to campaign {self.campaign_name}.."
        )
        catalog = PreProcessCatalog(db_path=catalog_db_path)

        ctd_assets: List[AssetEntry] = catalog.get_ctds(
            station=self.site.names[0], campaign=self.campaign_name
        )

        if not ctd_assets:
            raise ValueError(
                f"No SVP, CTD, or SEABIRD files found for campaign {self.campaign_name} in the catalog, "
                "use the data handler add_ctds_to_catalog() to catalog available CTD files, or provide a local SVP file"
            )

        for file in ctd_assets:
            logger.loginfo(
                f"Found {file.type} files related to campaign {self.campaign_name}"
            )

        preferred_types = [AssetType.SVP, AssetType.CTD, AssetType.SEABIRD]
        for preferred in preferred_types:
            for file in ctd_assets:
                if file.type == preferred:
                    if file.local_path is None and file.remote_path is not None:
                        local_path = self.working_dir / file.remote_path.split("/")[-1]
                        download_file_from_archive(
                            url=file.remote_path, dest_dir=str(self.working_dir)
                        )

                        if not local_path.exists():
                            raise ValueError(f"File {local_path} not downloaded")

                        logger.loginfo(f"Downloaded {file.remote_path} to {local_path}")
                        catalog.update_local_path(
                            id=file.id, local_path=str(local_path)
                        )

                    elif file.local_path is not None:
                        local_path = file.local_path
                        if not Path(local_path).exists():
                            logger.logwarn(
                                f"Local path {local_path} from catalog for file {file.id} does not exist, skipping this file."
                            )
                            continue

                    else:
                        continue

                    if preferred == AssetType.SVP:
                        logger.loginfo(
                            f"Using local sound speed profile found at {local_path}.."
                        )
                        self.sound_speed_path = local_path
                        return
                    elif preferred == AssetType.SEABIRD:
                        logger.loginfo(
                            f"Converting seabird file: {local_path} to sound velocity profile"
                        )
                        df = seabird_to_soundvelocity(source=local_path)
                    elif preferred == AssetType.CTD:
                        logger.loginfo(
                            f"Converting CTD file: {local_path} to sound velocity profile"
                        )
                        df = CTDfile_to_svp(source=local_path)
                    else:
                        raise ValueError(
                            f"Unknown file type {file.type} for file {local_path}"
                        )

                    df.to_csv(self.sound_speed_path)
                    logger.loginfo(
                        f"Converted {local_path} to sound velocity profile at {self.sound_speed_path}, adding to catalog"
                    )
                    catalog.add_entry(
                        AssetEntry(
                            local_path=str(self.sound_speed_path),
                            timestamp_created=datetime.now(),
                            type=AssetType.SVP,
                            network=file.network,
                            station=file.station,
                            campaign=file.campaign,
                        )
                    )
                    return

    def set_survey(self, name: str):
        """
        Set the current survey to the one with the given name.
        Args:
            name (str): The name of the survey to set as current.
        Raises:
            ValueError: If the survey with the given name is not found in the current campaign.
        """

        for survey in self.campaign.surveys:
            if survey.id == name:
                self.current_survey = survey
                logger.loginfo(
                    f"Current survey set to: {self.current_survey.id} {self.current_survey.start} - {self.current_survey.end}"
                )
                return
        raise ValueError(
            f"Survey {name} not found among: {[x.id for x in self.campaign.surveys]}"
        )

    def prep_shotdata(
        self,
        overwrite: bool = False,
        custom_filters: Optional[dict] = None,
    ):
        """
        Prepares and saves shot data for each survey in the campaign.
        Args:
            overwrite (bool): If True, overwrite existing files. Defaults to False.
            custom_filters (Optional[dict]): Custom filter settings to apply to the shot data. If None, use default filters.
        """
        prepareShotData(
            network_name=self.network,
            station_name=self.station,
            campaign_name=self.campaign.name,
            site=self.site,
            campaign=self.campaign,
            directory_handler=self.directory_handler,
            shotdata_filter_config=DEFAULT_FILTER_CONFIG,
            overwrite=overwrite,
            custom_filters=custom_filters
        )

    def get_obsfile_path(self, survey_id: str) -> Path:
        """
        Get the path to the observation file for a given survey.
        Args:
            survey_id (str): The ID of the survey.
        Returns:
            Path: The path to the observation file for the survey.
        """
        return self.directory_handler[self.network][self.station][self.campaign.name].garpos[survey_id].default_obsfile

    def get_results_dir(self, survey_id: str) -> Path:
        """
        Get the path to the results directory for a given survey.
        Args:
            survey_id (str): The ID of the survey.
        Returns:
            Path: The path to the results directory for the survey.
        """
        return self.directory_handler[self.network][self.station][self.campaign.name].garpos[survey_id].results_dir

    def set_inversion_params(self, parameters: dict | InversionParams):
        """
        Set inversion parameters for the model.
        This method updates the inversion parameters of the model using the key-value pairs
        provided in the `args` dictionary. Each key in the dictionary corresponds to an attribute
        of the `inversion_params` object, and the associated value is assigned to that attribute.

        Args:
            parameters (dict | InversionParams): A dictionary containing key-value pairs to update the inversion parameters or an InversionParams object.

        """

        if isinstance(parameters, InversionParams):
            self.garpos_fixed.inversion_params = parameters
        else:
            for key, value in parameters.items():
                setattr(self.garpos_fixed.inversion_params, key, value)

    def _run_garpos(self,
                    obsfile_path: Path,
                    results_dir: Path,
                    run_id: int | str = 0,
                    override: bool = False) -> Path:

        garpos_input = GarposInput.from_datafile(obsfile_path)
        results_path = results_dir / f"_{run_id}_results.json"

        if results_path.exists() and not override:
            print(f"Results already exist for {str(results_path)}")
            return None
        logger.loginfo(
            f"Running GARPOS model for {garpos_input.site_name}, {garpos_input.survey_id}. Run ID: {run_id}"
        )
        input_path = results_dir / f"_{run_id}_observation.ini"
        fixed_path = results_dir / f"_{run_id}_settings.ini"
        self.garpos_fixed._to_datafile(fixed_path)
        garpos_input.to_datafile(input_path)

        rf = drive_garpos(
            str(input_path),
            str(fixed_path),
            str(results_dir) + "/",
            f"{garpos_input.survey_id}_{run_id}",
            13,
        )
        return rf 

    def _run_garpos_survey(
        self,
        survey_id: str,
        run_id: int | str = 0,
        iterations: int = 1,
        override: bool = False,
    ) -> None:
        """
        Run the GARPOS model for a specific survey.
        Args:
            survey_id (str): The ID of the survey to run.
            run_id (int | str, optional): The run identifier. Defaults to 0.
            override (bool, optional): If True, override existing results. Defaults to False.
        Raises:
            ValueError: If the observation file does not exist.
        """
        logger.loginfo(f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}")

        self.set_survey(name=survey_id)

        results_dir = self.get_results_dir(survey_id=survey_id)
        obsfile_path = self.get_obsfile_path(survey_id=survey_id)

        if not obsfile_path.exists():
            raise ValueError(f"Observation file not found at {obsfile_path}")

        for i in range(iterations):
            logger.loginfo(f"Iteration {i+1} of {iterations} for survey {survey_id}")

            obsfile_path = self._run_garpos(
                obsfile_path=obsfile_path,
                results_dir=results_dir,
                run_id=f"{run_id}_{i}",
                override=override,
            )
        results = GarposInput.from_datafile(obsfile_path)
        process_garpos_results(results)

    def run_garpos(
        self,
        survey_id: Optional[str] = None,
        run_id: int | str = 0,
        iterations: int = 1,
        override: bool = False,
    ) -> None:
        """
        Run the GARPOS model for a specific date or for all dates.
        Args:
            survey_id (str, optional): The ID of the survey to run. Defaults to None.
            run_id (int | str, optional): The run identifier. Defaults to 0.
            override (bool, optional): If True, override existing results. Defaults to False.
        Returns:
            None
        """
        run_id = int(run_id) if isinstance(run_id, str) else run_id

        logger.loginfo(f"Running GARPOS model. Run ID: {run_id}")
        surveys = [s.id for s in self.campaign.surveys] if survey_id is None else [survey_id]

        for survey_id in surveys:
            logger.loginfo(
                f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}"
            )
            self._run_garpos_survey(
                survey_id=survey_id, run_id=run_id, override=override, iterations=iterations
            )

    def plot_ts_results(
        self,
        survey_id: str,
        run_id: int | str = 0,
        res_filter: float = 10,
        savefig: bool = False,
    ) -> None:
        """
        Plots the time series results for a given survey.
        Args:
            survey_id (str): ID of the survey to plot results for.
            run_id (int or str, optional): The run ID of the survey results to plot. Default is 0.
            res_filter (float, optional): The residual filter value to filter outrageous values (m). Default is 10.
            savefig (bool, optional): If True, save the figure. Default is False.

        Returns:
            None
        """
        self.set_survey(survey_id)
        self.garpos_results_processor.plot_ts_results(
            survey_id=survey_id,
            run_id=run_id,
            res_filter=res_filter,
            savefig=savefig
        )
