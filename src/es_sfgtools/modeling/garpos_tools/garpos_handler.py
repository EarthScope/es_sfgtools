# GarposHandler class for processing and preparing shot data for the GARPOS model.

from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple, Union
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import json
import os

from es_sfgtools.data_mgmt.directory_handler import DirectoryHandler,SurveyDir,GARPOSSurveyDir
from es_sfgtools.data_models.metadata.catalogs import StationData
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.data_models.metadata.campaign import Survey,Campaign

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



from .garpos_results_processor import GarposResultsProcessor
from .garpos_config import DEFAULT_FILTER_CONFIG, DEFAULT_INVERSION_PARAMS


DEFAULT_SETTINGS_FILE_NAME = "default_settings.ini"
SVP_FILE_NAME = "svp.csv"
SURVEY_METADATA_FILE_NAME = "survey_meta.json"


class GarposHandler:
    """
    Handles the processing and preparation of shot data for the GARPOS model.

    This class provides a high-level interface for running the GARPOS model, including
    data preparation, setting parameters, and processing results.

    Attributes:
        directory_handler (DirectoryHandler): Handles the directory structure.
        site (Site): The site metadata.
        network (str): The network name.
        station (str): The station name.
        campaign (Campaign): The campaign metadata.
        current_survey (Survey): The current survey being processed.
        coord_transformer (CoordTransformer): Coordinate transformer for the site.
        garpos_fixed (GarposFixed): Fixed parameters for the GARPOS model.
        sound_speed_path (Path): Path to the sound speed profile file.
        garpos_results_processor (GarposResultsProcessor): Processes and plots GARPOS results.
    """

    def __init__(self,
                 main_directory: Path,
                 site: Site):
        """
        Initializes the GarposHandler.

        Args:
            main_directory (Path): The main directory for data and results.
            site (Site): The site metadata.
        """

        self.directory_handler = DirectoryHandler(location=main_directory)
        self.assetCatalog = PreProcessCatalog(db_path=self.directory_handler.asset_catalog_db_path)
        self.site = site

        self.working_dir = None
        self.garpos_fixed = GarposFixed()

        self.currentCampaign: Campaign = None
        self.currentSurvey: Survey = None
        self.currentStation:str = None
        self.currentNetwork: str = None

    def setNetworkStationCampaign(self,network: str, station: str, campaign: str):
        self.currentCampaign = None
        self.currentStation = None
        self.currentNetwork = None
        self.currentSurvey = None
        self.working_dir = None

        for site_name in self.site.names:
            if site_name == station:
                self.currentStation = site_name
                break
        if self.currentStation is None:
            raise ValueError(f"Station {station} not found in site metadata.")

        for network_name in self.site.networks:
            if network_name == network:
                self.currentNetwork = network_name
                break
        if self.currentNetwork is None:
            raise ValueError(f"Network {network} not found in site metadata.")


        for campaignObj in self.site.campaigns:
            if campaignObj.name == campaign:
                self.currentCampaign = campaignObj
                break
        if self.currentCampaign is None:
            raise ValueError(f"Campaign {campaign} not found in site metadata.")

        self.directory_handler.build_station_directory(
            network_name=self.currentNetwork,
            station_name=self.currentStation,
            campaign_name=self.currentCampaign.name,
        )

        # self.working_dir = self.directory_handler[self.currentNetwork][self.currentStation][self.currentCampaign].garpos.location
        # self.garpos_fixed._to_datafile(path=self.directory_handler[self.currentNetwork][self.currentStation][self.currentCampaign].garpos.default_settings)
        # log_dir = self.directory_handler[self.currentNetwork][self.currentStation][self.currentCampaign].log_directory
        # logger.set_dir(log_dir)
        # self.directory_handler.save()

    def setSurvey(self, name: str):
        """
        Sets the current survey by name.

        Args:
            name (str): The name of the survey to set as current.

        Raises:
            ValueError: If the survey with the given name is not found in the current campaign.
        """
        self.currentSurvey = None
        if self.currentCampaign is None:
            raise ValueError("Current campaign is not set. Please set the campaign before setting a survey.")

        for survey in self.currentCampaign.surveys:
            if survey.id == name:
                self.currentSurvey = survey
                break

        if self.currentSurvey is None:
            raise ValueError(f"Survey {name} not found in campaign {self.currentCampaign.name}.")

        # self.directory_handler[self.currentNetwork][self.currentStation][self.currentCampaign]
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
            network_name=self.currentNetwork,
            station_name=self.currentStation,
            site=self.site,
            campaign=self.currentCampaign,
            directory_handler=self.directory_handler,
            shotdata_filter_config=DEFAULT_FILTER_CONFIG,
            overwrite=overwrite,
            custom_filters=custom_filters
        )

    def _prep_shotdata_survey(self, survey_id: str, override: bool = False):
        surveyDir : SurveyDir = self.directory_handler[self.currentNetwork][self.currentStation][self.currentCampaign.name].surveys[survey_id]
        garposSurveyDir : GARPOSSurveyDir = surveyDir.garpos

        obs_file = garposSurveyDir.default_obsfile
        if obs_file.exists() and not override:
            return
        
        
    def get_obsfile_path(self, survey_id: str) -> Path:
        """
        Get the path to the observation file for a given survey.
        Args:
            survey_id (str): The ID of the survey.
        Returns:
            Path: The path to the observation file for the survey.
        """
        return self.directory_handler[self.currentNetwork][self.currentStation][self.currentCampaign.name].garpos[survey_id].default_obsfile

    def get_results_dir(self, survey_id: str) -> Path:
        """
        Get the path to the results directory for a given survey.
        Args:
            survey_id (str): The ID of the survey.
        Returns:
            Path: The path to the results directory for the survey.
        """
        return self.directory_handler[self.currentNetwork][self.currentStation][self.currentCampaign.name].garpos[survey_id].results_dir

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

    def _run_garpos(
        self,
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

        self.setSurvey(name=survey_id)

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

        logger.loginfo(f"Running GARPOS model. Run ID: {run_id}")
        surveys = [s.id for s in self.currentCampaign.surveys] if survey_id is None else [survey_id]

        for survey_id in surveys:
            logger.loginfo(
                f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}"
            )
            self._run_garpos_survey(
                survey_id=survey_id, run_id=run_id, override=override, iterations=iterations
            )

    # def plot_ts_results(
    #     self,
    #     survey_id: str,
    #     run_id: int | str = 0,
    #     res_filter: float = 10,
    #     savefig: bool = False,
    # ) -> None:
    #     """
    #     Plots the time series results for a given survey.
    #     Args:
    #         survey_id (str): ID of the survey to plot results for.
    #         run_id (int or str, optional): The run ID of the survey results to plot. Default is 0.
    #         res_filter (float, optional): The residual filter value to filter outrageous values (m). Default is 10.
    #         savefig (bool, optional): If True, save the figure. Default is False.

    #     Returns:
    #         None
    #     """
    #     self.setSurvey(survey_id)
    #     self.garpos_results_processor.plot_ts_results(
    #         survey_id=survey_id,
    #         run_id=run_id,
    #         res_filter=res_filter,
    #         savefig=savefig
    #     )

# def load_sound_speed_data(
#     self,
#     local_svp: Optional[Path] = None,
#     local_ctd: Optional[Path] = None,
#     catalog_db_path: Optional[Path] = None
# ):
#     """
#     Load the sound speed profile from a local file or from the catalog.
#     Args:
#         local_svp (Path): The path to the local sound speed profile file. Default is None.
#         local_ctd (Path): The path to the local CTD file. Default is None.
#         catalog_db_path (Path): The path to the catalog database. Default is None.
#     """
#     if local_svp:
#         self.sound_speed_path = Path(local_svp)
#         logger.loginfo(f"Using local sound speed profile found at {local_svp}..")
#     elif local_ctd:
#         logger.loginfo(
#             f"Using local CTD file found at {local_ctd}, converting to sound speed profile.."
#         )
#         df = CTDfile_to_svp(source=local_ctd)
#         self.sound_speed_path = self.working_dir / SVP_FILE_NAME
#         df.to_csv(self.sound_speed_path, index=False)
#         logger.loginfo(
#             f"Converted {local_ctd} to sound velocity profile at {self.sound_speed_path}"
#         )
#     else:
#         self._check_CTDs_in_catalog(catalog_db_path)

# def _check_CTDs_in_catalog(self, catalog_db_path: Optional[Path] = None):
#    """
#         This function will check the catalog database for SVP or CTD files related to the current campaign. If found and local, set as sound
#         speed file or convert to SVP. If only remote, download it first and then set or convert to sound speed profile.

#         If no files are found in catalog, raise an error.

#         Args:
#             catalog_db_path (Path): The path to the catalog database. Default is None. Will check in local working directory if not provided.

#         Raises:
#             ValueError: If no CTD files are found for the campaign in the catalog or if the catalog database path is not found or provided.
#         """

#         if not catalog_db_path:
#             catalog_db_path = self.directory_handler.location / "catalog.sqlite"
#             logger.logdebug(
#                 f"Catalog database path not provided, checking for local catalog database at: {str(catalog_db_path)}"
#             )
#             if not catalog_db_path.exists():
#                 raise ValueError(
#                     "No local SVP found and no catalog database path provided, "
#                     "\n please provide the catalog database path to check for the CTD files or a local SVP file"
#                 )
#             else:
#                 logger.logdebug(
#                     f"Using local catalog database found at {catalog_db_path}.."
#                 )

#         logger.loginfo(
#             f"Checking catalog database for SVP, CTD, and SEABIRD files related to campaign {self.currentCampaign_name.name}.."
#         )
#         catalog = PreProcessCatalog(db_path=catalog_db_path)

#         ctd_assets: List[AssetEntry] = catalog.get_ctds(
#             station=self.currentStation_name, campaign=self.currentCampaign_name.name
#         )

#         if not ctd_assets:
#             raise ValueError(
#                 f"No SVP, CTD, or SEABIRD files found for campaign {self.currentCampaign_name.name} in the catalog, "
#                 "use the data handler add_ctds_to_catalog() to catalog available CTD files, or provide a local SVP file"
#             )

#         for file in ctd_assets:
#             logger.loginfo(
#                 f"Found {file.type} files related to campaign {self.currentCampaign_name.name}"
#             )

#         preferred_types = [AssetType.SVP, AssetType.CTD, AssetType.SEABIRD]
#         for preferred in preferred_types:
#             for file in ctd_assets:
#                 if file.type == preferred:
#                     if file.local_path is None and file.remote_path is not None:
#                         local_path = self.working_dir / file.remote_path.split("/")[-1]
#                         download_file_from_archive(
#                             url=file.remote_path, dest_dir=str(self.working_dir)
#                         )

#                         if not local_path.exists():
#                             raise ValueError(f"File {local_path} not downloaded")

#                         logger.loginfo(f"Downloaded {file.remote_path} to {local_path}")
#                         catalog.update_local_path(
#                             id=file.id, local_path=str(local_path)
#                         )

#                     elif file.local_path is not None:
#                         local_path = file.local_path
#                         if not Path(local_path).exists():
#                             logger.logwarn(
#                                 f"Local path {local_path} from catalog for file {file.id} does not exist, skipping this file."
#                             )
#                             continue

#                     else:
#                         continue

#                     if preferred == AssetType.SVP:
#                         logger.loginfo(
#                             f"Using local sound speed profile found at {local_path}.."
#                         )
#                         self.sound_speed_path = local_path
#                         return
#                     elif preferred == AssetType.SEABIRD:
#                         logger.loginfo(
#                             f"Converting seabird file: {local_path} to sound velocity profile"
#                         )
#                         df = seabird_to_soundvelocity(source=local_path)
#                     elif preferred == AssetType.CTD:
#                         logger.loginfo(
#                             f"Converting CTD file: {local_path} to sound velocity profile"
#                         )
#                         df = CTDfile_to_svp(source=local_path)
#                     else:
#                         raise ValueError(
#                             f"Unknown file type {file.type} for file {local_path}"
#                         )

#                     self.sound_speed_path = self.working_dir / SVP_FILE_NAME
#                     df.to_csv(self.sound_speed_path, index=False)
#                     logger.loginfo(
#                         f"Converted {local_path} to sound velocity profile at {self.sound_speed_path}, adding to catalog"
#                     )
#                     catalog.add_entry(
#                         AssetEntry(
#                             local_path=str(self.sound_speed_path),
#                             timestamp_created=datetime.now(),
#                             type=AssetType.SVP,
#                             network=file.network,
#                             station=file.station,
#                             campaign=file.campaign,
#                         )
#                     )
#                     return