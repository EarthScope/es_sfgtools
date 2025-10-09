"""
GarposHandler class for processing and preparing shot data for the GARPOS model.
"""

from pathlib import Path
from typing import Optional

from es_sfgtools.data_mgmt.directory_handler import (
    CampaignDir,
    DirectoryHandler,
    GARPOSSurveyDir,
    NetworkDir,
    SurveyDir,
)
from es_sfgtools.data_models.metadata.campaign import Campaign, Survey
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposFixed,
    GarposInput,
    InversionParams,
)

try:
    from garpos import drive_garpos
except ImportError:
    # Handle the case where garpos is not available
    pass

from es_sfgtools.modeling.garpos_tools.functions import process_garpos_results

from es_sfgtools.logging import GarposLogger as logger


class GarposHandler:
    """
    Handles the processing and preparation of shot data for the GARPOS model.

    This class provides a high-level interface for running the GARPOS model, including
    data preparation, setting parameters, and processing results.

    :ivar directory_handler: Handles the directory structure.
    :vartype directory_handler: DirectoryHandler
    :ivar site: The site metadata.
    :vartype site: Site
    :ivar network: The network name.
    :vartype network: str
    :ivar station: The station name.
    :vartype station: str
    :ivar campaign: The campaign metadata.
    :vartype campaign: Campaign
    :ivar current_survey: The current survey being processed.
    :vartype current_survey: Survey
    :ivar coord_transformer: Coordinate transformer for the site.
    :vartype coord_transformer: CoordTransformer
    :ivar garpos_fixed: Fixed parameters for the GARPOS model.
    :vartype garpos_fixed: GarposFixed
    :ivar sound_speed_path: Path to the sound speed profile file.
    :vartype sound_speed_path: Path
    :ivar garpos_results_processor: Processes and plots GARPOS results.
    :vartype garpos_results_processor: GarposResultsProcessor
    """

    def __init__(self,
                 directory_handler: DirectoryHandler,
                 site: Site):
        """
        Initializes the GarposHandler.

        :param main_directory: The main directory for data and results.
        :type main_directory: Path
        :param site: The site metadata.
        :type site: Site
        """

        self.directory_handler = directory_handler
        self.site: Site = site

        self.garpos_fixed = GarposFixed()

        self.directory_handler = directory_handler

        self.currentCampaign: Campaign = None
        self.currentSurvey: Survey = None

        self.currentStation: str = None
        self.currentNetwork: str = None

        self.currentNetworkDir: NetworkDir = None
        self.currentCampaignDir: CampaignDir = None
        self.currentSurveyDir: SurveyDir = None
        self.currentGarposSurveyDir: GARPOSSurveyDir = None

    def setNetwork(self, network_id: str):
        """
        Sets the current network.

        :param network_id: The ID of the network to set.
        :type network_id: str
        :raises ValueError: If the network is not found in the site metadata.
        """
        self.currentNetwork = None
        self.currentNetworkDir = None

        self.currentStation = None
        self.currentStationDir = None

        self.currentCampaign = None
        self.currentCampaignDir = None

        self.currentSurvey = None
        self.currentSurveyDir = None

        self.currentGarposSurveyDir = None

        # Set current network attributes
        for network_name in self.site.networks:
            if network_name == network_id:
                self.currentNetwork = network_name
                break
        if self.currentNetwork is None:
            raise ValueError(f"Network {network_id} not found in site metadata.")

        if (
            currentNetworkDir := self.directory_handler.networks.get(
                self.currentNetwork, None
            )
        ) is None:
            currentNetworkDir = self.directory_handler.add_network(
                name=self.currentNetwork
            )

        self.currentNetworkDir = currentNetworkDir

    def setStation(self, station_id: str):
        """
        Sets the current station.

        :param station_id: The ID of the station to set.
        :type station_id: str
        :raises ValueError: If the station is not found in the site metadata.
        """

        self.currentStation = None
        self.currentStationDir = None

        self.currentCampaign = None
        self.currentCampaignDir = None

        self.currentSurvey = None
        self.currentSurveyDir = None

        # Set current station attributes
        for station_name in self.site.names:
            if station_name == station_id:
                self.currentStation = station_name
                break
        if self.currentStation is None:
            raise ValueError(f"Station {station_id} not found in site metadata.")

        if (
            currentStationDir := self.currentNetworkDir.stations.get(
                self.currentStation, None
            )
        ) is None:
            raise ValueError(f"Station {station_id} not found in directory handler. Please run intermediate data processing to create station directory.")

        self.currentStationDir = currentStationDir

    def setCampaign(self, campaign_id: str):
        """
        Sets the current campaign.

        :param campaign_id: The ID of the campaign to set.
        :type campaign_id: str
        :raises ValueError: If the campaign is not found in the site metadata.
        """
        self.currentCampaign = None
        self.currentCampaignDir = None

        self.currentSurvey = None
        self.currentSurveyDir = None

        # Set current campaign attributes

        for campaign in self.site.campaigns:
            if campaign.name == campaign_id:
                self.currentCampaign = campaign
                break
        if self.currentCampaign is None:
            raise ValueError(f"Campaign {campaign_id} not found in site metadata.")

        if (
            currentCampaignDir := self.currentStationDir.campaigns.get(
                self.currentCampaign.name, None
            )
        ) is None:
            raise ValueError(f"Campaign {campaign_id} not found in directory handler. Please run intermediate data processing to create campaign directory.")

        self.currentCampaignDir = currentCampaignDir

    def setSurvey(self, survey_id: str):
        """
        Sets the current survey.

        :param survey_id: The ID of the survey to set.
        :type survey_id: str
        :raises ValueError: If the survey is not found in the current campaign.
        """
        assert isinstance(survey_id, str), "survey_id must be a string"

        self.currentSurvey = None
        self.currentSurveyDir = None
        # Set current survey attributes
        for survey in self.currentCampaign.surveys:
            if survey.id == survey_id:
                self.currentSurvey = survey
                break
        if self.currentSurvey is None:
            raise ValueError(
                f"Survey {survey_id} not found in campaign {self.currentCampaign.name}."
            )

        if (
            currentSurveyDir := self.currentCampaignDir.surveys.get(survey_id, None)
        ) is None:
            currentSurveyDir = self.currentCampaignDir.add_survey(name=survey_id)
        
        if currentSurveyDir.shotdata is None or not currentSurveyDir.shotdata.exists():
            raise ValueError(f"Shotdata for survey {survey_id} not found in directory handler. Please run intermediate data processing to create shotdata file.")
        
        self.currentSurveyDir = currentSurveyDir
      

        try:
            if self.currentSurveyDir.garpos.shotdata_rectified.exists():
                self.currentGarposSurveyDir = self.currentSurveyDir.garpos
                logger.set_dir(self.currentGarposSurveyDir.log_directory)
                return
        except Exception:
            pass
        raise ValueError(f"Rectified shotdata for survey {survey_id} not found in directory handler. Please run intermediate data processing to create rectified shotdata file.")


    def setNetworkStationCampaign(self, network: str, station: str, campaign: str):
        """
        Sets the network, station, and campaign for the handler.

        :param network: The network name.
        :type network: str
        :param station: The station name.
        :type station: str
        :param campaign: The campaign name.
        :type campaign: str
        :raises ValueError: If the station, network, or campaign is not found in the site metadata.
        """
        self.setNetwork(network)
        self.setStation(station)
        self.setCampaign(campaign)

    def set_inversion_params(self, parameters: dict | InversionParams):
        """
        Set inversion parameters for the model.

        This method updates the inversion parameters of the model using the key-value pairs
        provided in the `args` dictionary. Each key in the dictionary corresponds to an attribute
        of the `inversion_params` object, and the associated value is assigned to that attribute.

        :param parameters: A dictionary containing key-value pairs to update the inversion parameters or an InversionParams object.
        :type parameters: dict | InversionParams
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
        settings: Optional[GarposFixed] = None,
        run_id: int | str = 0,
        override: bool = False) -> Path:
        """
        Runs the GARPOS model.

        :param obsfile_path: The path to the observation file.
        :type obsfile_path: Path
        :param results_dir: The path to the results directory.
        :type results_dir: Path
        :param run_id: The run ID. Defaults to 0.
        :type run_id: int | str, optional
        :param override: If True, override existing results. Defaults to False.
        :type override: bool, optional
        :return: The path to the results file.
        :rtype: Path
        """
        garpos_fixed_params = settings if settings is not None else self.garpos_fixed
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
        garpos_fixed_params._to_datafile(fixed_path)
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
        settings: Optional[GarposFixed] = None,
        run_id: int | str = 0,
        iterations: int = 1,
        override: bool = False,
    ) -> None:
        """
        Run the GARPOS model for a specific survey.

        :param survey_id: The ID of the survey to run.
        :type survey_id: str
        :param run_id: The run identifier. Defaults to 0.
        :type run_id: int | str, optional
        :param iterations: The number of iterations to run. Defaults to 1.
        :type iterations: int, optional
        :param override: If True, override existing results. Defaults to False.
        :type override: bool, optional
        :raises ValueError: If the observation file does not exist.
        """
        logger.loginfo(f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}")

        self.setSurvey(survey_id=survey_id)

        results_dir_main = self.currentGarposSurveyDir.results_dir
        results_dir = results_dir_main / f"run_{run_id}"
        results_dir.mkdir(parents=True, exist_ok=True)

        obsfile_path = self.currentGarposSurveyDir.default_obsfile

        if not obsfile_path.exists():
            raise ValueError(f"Observation file not found at {obsfile_path}")

        for i in range(iterations):
            logger.loginfo(f"Iteration {i+1} of {iterations} for survey {survey_id}")

            obsfile_path = self._run_garpos(
                settings=settings,
                obsfile_path=obsfile_path,
                results_dir=results_dir,
                run_id=f"{i}",
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

        :param survey_id: The ID of the survey to run. Defaults to None.
        :type survey_id: str, optional
        :param run_id: The run identifier. Defaults to 0.
        :type run_id: int | str, optional
        :param iterations: The number of iterations to run. Defaults to 1.
        :type iterations: int, optional
        :param override: If True, override existing results. Defaults to False.
        :type override: bool, optional
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
    #     :param survey_id: ID of the survey to plot results for.
    #     :type survey_id: str
    #     :param run_id: The run ID of the survey results to plot. Default is 0.
    #     :type run_id: int or str, optional
    #     :param res_filter: The residual filter value to filter outrageous values (m). Default is 10.
    #     :type res_filter: float, optional
    #     :param savefig: If True, save the figure. Default is False.
    #     :type savefig: bool, optional
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
#     :param local_svp: The path to the local sound speed profile file. Default is None.
#     :type local_svp: Path, optional
#     :param local_ctd: The path to the local CTD file. Default is None.
#     :type local_ctd: Path, optional
#     :param catalog_db_path: The path to the catalog database. Default is None.
#     :type catalog_db_path: Path, optional
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
#
#         If no files are found in catalog, raise an error.
#
#         :param catalog_db_path: The path to the catalog database. Default is None. Will check in local working directory if not provided.
#         :type catalog_db_path: Path, optional
#         :raises ValueError: If no CTD files are found for the campaign in the catalog or if the catalog database path is not found or provided.
#         """
#
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
#
#         logger.loginfo(
#             f"Checking catalog database for SVP, CTD, and SEABIRD files related to campaign {self.currentCampaign_name.name}.."
#         )
#         catalog = PreProcessCatalog(db_path=catalog_db_path)
#
#         ctd_assets: List[AssetEntry] = catalog.get_ctds(
#             station=self.currentStation_name, campaign=self.currentCampaign_name.name
#         )
#
#         if not ctd_assets:
#             raise ValueError(
#                 f"No SVP, CTD, or SEABIRD files found for campaign {self.currentCampaign_name.name} in the catalog, "
#                 "use the data handler add_ctds_to_catalog() to catalog available CTD files, or provide a local SVP file"
#             )
#
#         for file in ctd_assets:
#             logger.loginfo(
#                 f"Found {file.type} files related to campaign {self.currentCampaign_name.name}"
#             )
#
#         preferred_types = [AssetType.SVP, AssetType.CTD, AssetType.SEABIRD]
#         for preferred in preferred_types:
#             for file in ctd_assets:
#                 if file.type == preferred:
#                     if file.local_path is None and file.remote_path is not None:
#                         local_path = self.working_dir / file.remote_path.split("/")[-1]
#                         download_file_from_archive(
#                             url=file.remote_path, dest_dir=str(self.working_dir)
#                         )
#
#                         if not local_path.exists():
#                             raise ValueError(f"File {local_path} not downloaded")
#
#                         logger.loginfo(f"Downloaded {file.remote_path} to {local_path}")
#                         catalog.update_local_path(
#                             id=file.id, local_path=str(local_path)
#                         )
#
#                     elif file.local_path is not None:
#                         local_path = file.local_path
#                         if not Path(local_path).exists():
#                             logger.logwarn(
#                                 f"Local path {local_path} from catalog for file {file.id} does not exist, skipping this file."
#                             )
#                             continue
#
#                     else:
#                         continue
#
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
#
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
