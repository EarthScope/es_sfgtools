"""
GarposHandler class for processing and preparing shot data for the GARPOS model.
"""

from pathlib import Path
from typing import Optional
import shutil

from es_sfgtools.data_mgmt.directorymgmt.directory_handler import (
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

from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.modeling.garpos_tools.functions import process_garpos_results
from es_sfgtools.modeling.garpos_tools.garpos_results_processor import GarposResultsProcessor

class GarposHandler:
    """Handles the processing and preparation of shot data for the GARPOS model.

    This class provides a high-level interface for running the GARPOS model,
    including data preparation, setting parameters, and processing results.

    Attributes
    ----------
    directory_handler : DirectoryHandler
        Handles the directory structure.
    site : Site
        The site metadata.
    network : str
        The network name.
    station : str
        The station name.
    campaign : Campaign
        The campaign metadata.
    current_survey : Survey
        The current survey being processed.
    coord_transformer : CoordTransformer
        Coordinate transformer for the site.
    garpos_fixed : GarposFixed
        Fixed parameters for the GARPOS model.
    sound_speed_path : Path
        Path to the sound speed profile file.
    garpos_results_processor : GarposResultsProcessor
        Processes and plots GARPOS results.
    """

    def __init__(
                 self,
                 directory_handler: DirectoryHandler,
                 site: Site):
        """Initializes the GarposHandler.

        Parameters
        ----------
        directory_handler : DirectoryHandler
            The directory handler.
        site : Site
            The site metadata.
        """

        self.directory_handler = directory_handler
        self.site: Site = site

        self.garpos_fixed = GarposFixed()

        self.directory_handler = directory_handler

        self.garpos_results_processor: GarposResultsProcessor = None

        self.currentCampaign: Campaign = None
        self.currentSurvey: Survey = None

        self.currentStation: str = None
        self.currentNetwork: str = None

        self.currentNetworkDir: NetworkDir = None
        self.currentCampaignDir: CampaignDir = None
        self.currentSurveyDir: SurveyDir = None
        self.currentGarposSurveyDir: GARPOSSurveyDir = None

    def setNetwork(self, network_id: str):
        """Sets the current network.

        Parameters
        ----------
        network_id : str
            The ID of the network to set.

        Raises
        ------
        ValueError
            If the network is not found in the site metadata.
        """
        self.currentNetwork = None
        self.currentNetworkDir = None

        self.currentStation = None
        self.currentStationDir = None

        self.currentCampaign = None
        self.currentCampaignDir = None
        self.garpos_results_processor = None

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
        """Sets the current station.

        Parameters
        ----------
        station_id : str
            The ID of the station to set.

        Raises
        ------
        ValueError
            If the station is not found in the site metadata.
        """

        self.currentStation = None
        self.currentStationDir = None

        self.currentCampaign = None
        self.currentCampaignDir = None
        self.garpos_results_processor = None

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
        """Sets the current campaign.

        Parameters
        ----------
        campaign_id : str
            The ID of the campaign to set.

        Raises
        ------
        ValueError
            If the campaign is not found in the site metadata.
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
        self.garpos_results_processor = GarposResultsProcessor(self.currentCampaignDir)

    def setSurvey(self, survey_id: str):
        """Sets the current survey.

        Parameters
        ----------
        survey_id : str
            The ID of the survey to set.

        Raises
        ------
        ValueError
            If the survey is not found in the current campaign.
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
        """Sets the network, station, and campaign for the handler.

        Parameters
        ----------
        network : str
            The network name.
        station : str
            The station name.
        campaign : str
            The campaign name.

        Raises
        ------
        ValueError
            If the station, network, or campaign is not found in the site
            metadata.
        """
        self.setNetwork(network)
        self.setStation(station)
        self.setCampaign(campaign)

    def set_inversion_params(self, parameters: dict | InversionParams):
        """Set inversion parameters for the model.

        This method updates the inversion parameters of the model using the
        key-value pairs provided in the `args` dictionary. Each key in the
        dictionary corresponds to an attribute of the `inversion_params`
        object, and the associated value is assigned to that attribute.

        Parameters
        ----------
        parameters : dict | InversionParams
            A dictionary containing key-value pairs to update the inversion
            parameters or an InversionParams object.
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
        custom_settings: Optional[dict] = None,
        run_id: int | str = 0,
        override: bool = False) -> Path:
        """Runs the GARPOS model.

        Parameters
        ----------
        obsfile_path : Path
            The path to the observation file.
        results_dir : Path
            The path to the results directory.
        custom_settings : Optional[dict], optional
            Custom GARPOS settings to apply, by default None.
        run_id : int | str, optional
            The run ID, by default 0.
        override : bool, optional
            If True, override existing results, by default False.

        Returns
        -------
        Path
            The path to the results file.
        """
        garpos_fixed_params = self.garpos_fixed.model_copy()
        if custom_settings is not None:
            for key, value in custom_settings.items():
                if hasattr(garpos_fixed_params.inversion_params, key):
                    setattr(garpos_fixed_params.inversion_params, key, value)
                else:
                    logger.logwarn(
                        f"Custom GARPOS setting {key} not found in GarposFixed schema, ignoring."
                    )
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
        custom_settings: Optional[dict] = None,
        run_id: int | str = 0,
        iterations: int = 1,
        override: bool = False,
    ) -> None:
        """Run the GARPOS model for a specific survey.

        Parameters
        ----------
        survey_id : str
            The ID of the survey to run.
        custom_settings : dict, optional
            Custom GARPOS settings to apply, by default None.
        run_id : int | str, optional
            The run identifier, by default 0.
        iterations : int, optional
            The number of iterations to run, by default 1.
        override : bool, optional
            If True, override existing results, by default False.

        Raises
        ------
        ValueError
            If the observation file does not exist.
        """
        logger.loginfo(f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}")

        try:
            self.setSurvey(survey_id=survey_id)
        except ValueError as e:
            logger.logwarn(f"Skipping survey {survey_id}: {e}")
            return

        results_dir_main = self.currentGarposSurveyDir.results_dir
        results_dir = results_dir_main / f"run_{run_id}"
        if results_dir.exists() and override:
            # Remove existing results directory if override is True
            shutil.rmtree(results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)

        obsfile_path = self.currentGarposSurveyDir.default_obsfile

        if not obsfile_path.exists():
            raise ValueError(f"Observation file not found at {obsfile_path}")

        initialInput = GarposInput.from_datafile(obsfile_path)

        for i in range(iterations):
            logger.loginfo(f"Iteration {i+1} of {iterations} for survey {survey_id}")

            obsfile_path = self._run_garpos(
                custom_settings=custom_settings,
                obsfile_path=obsfile_path,
                results_dir=results_dir,
                run_id=f"{i}",
                override=override,
            )
            if iterations > 1 and i < iterations - 1:
                iterationInput = GarposInput.from_datafile(obsfile_path)
                delta_position = iterationInput.delta_center_position.get_position()
                iterationInput.array_center_enu.east += delta_position[0]
                iterationInput.array_center_enu.north += delta_position[1]
                iterationInput.array_center_enu.up += delta_position[2]
                # zero out delta position for next iteration
                iterationInput.delta_center_position = initialInput.delta_center_position

                iterationInput.to_datafile(obsfile_path)

            # Add array delta center position to the array center enu

        results = GarposInput.from_datafile(obsfile_path)
        process_garpos_results(results)

    def run_garpos(
        self,
        survey_id: Optional[str] = None,
        run_id: int | str = 0,
        iterations: int = 1,
        override: bool = False,
        custom_settings: Optional[dict] = None, 
    ) -> None:
        """Run the GARPOS model for a specific date or for all dates.

        Parameters
        ----------
        survey_id : str, optional
            The ID of the survey to run, by default None.
        run_id : int | str, optional
            The run identifier, by default 0.
        iterations : int, optional
            The number of iterations to run, by default 1.
        override : bool, optional
            If True, override existing results, by default False.
        custom_settings : dict, optional
            Custom GARPOS settings to apply, by default None.
        """

        logger.loginfo(f"Running GARPOS model. Run ID: {run_id}")
        surveys = [s.id for s in self.currentCampaign.surveys] if survey_id is None else [survey_id]

        for survey_id in surveys:
            logger.loginfo(
                f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}"
            )
            self._run_garpos_survey(
                survey_id=survey_id, run_id=run_id, override=override, iterations=iterations, custom_settings=custom_settings
            )

    def plot_ts_results(
        self,
        survey_id: str = None,
        run_id: int | str = 0,
        res_filter: float = 10,
        savefig: bool = False,
        showfig: bool = True,
    ) -> None:
        """Plots the time series results for a given survey.

        Parameters
        ----------
        survey_id : str, optional
            ID of the survey to plot results for, by default None.
        run_id : int or str, optional
            The run ID of the survey results to plot, by default 0.
        res_filter : float, optional
            The residual filter value to filter outrageous values (m), by
            default 10.
        savefig : bool, optional
            If True, save the figure, by default False.
        showfig : bool, optional
            If True, display the figure, by default True.
        """
        surveys_to_process = []
        for survey in self.currentCampaign.surveys:
            if survey.id == survey_id or survey_id is None:
                surveys_to_process.append((survey.id,survey.type.value))

        for survey_id, survey_type in surveys_to_process:
            try:
                self.setSurvey(survey_id)
                self.garpos_results_processor.plot_ts_results(
                    survey_id=survey_id,
                    survey_type=survey_type,
                    run_id=run_id,
                    res_filter=res_filter,
                    savefig=savefig,
                    showfig=showfig,
                )
            except Exception as e:
                logger.logwarn(f"Skipping plotting for survey {survey_id}: {e}")
                continue

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