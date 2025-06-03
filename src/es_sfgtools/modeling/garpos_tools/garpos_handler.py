# GarposHandler class for processing and preparing shot data for the GARPOS model.

from pathlib import Path
from typing import List, Tuple, Union
from es_sfgtools.processing.operations.site_ops import CTDfile_to_svp, seabird_to_soundvelocity
from es_sfgtools.utils.archive_pull import download_file_from_archive
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import json
from matplotlib.colors import Normalize
from matplotlib.collections import LineCollection
import matplotlib.dates as mdates
import seaborn as sns
sns.set_theme()
import matplotlib.gridspec as gridspec
import os

from sfg_metadata.metadata.src.catalogs import StationData
from sfg_metadata.metadata.src.site import Site
from sfg_metadata.metadata.src.benchmark import Benchmark, Transponder


from es_sfgtools.processing.assets.observables import ShotDataFrame
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposFixed,
    InversionParams,
    GarposInput,
    GPTransponder,
    GPATDOffset,
    GPPositionENU,
    GPPositionLLH
)
from es_sfgtools.modeling.garpos_tools.functions import CoordTransformer, process_garpos_results, rectify_shotdata
from es_sfgtools.utils.loggers import GarposLogger as logger

from ...processing.assets.tiledb import TDBShotDataArray
from .load_utils import load_drive_garpos
from es_sfgtools.processing.pipeline.catalog import PreProcessCatalog
from es_sfgtools.processing.assets.file_schemas import AssetEntry, AssetType

try:
    drive_garpos = load_drive_garpos()
except Exception as e:
    from garpos import drive_garpos

colors = [
    "blue",
    "green",
    "red",
    "cyan",
    "magenta",
    "yellow",
    "black",
    "brown",
    "orange",
    "pink",
]

SHOTDATA_DIR_NAME = "shotdata"
RESULTS_DIR_NAME = "results"
DEFAULT_SETTINGS_FILE_NAME = "default_settings.ini"
SVP_FILE_NAME = "svp.csv"
SURVEY_METADATA_FILE_NAME = "survey_meta.json"
OBSERVATION_FILE_NAME = "observation.ini"

def avg_transponder_position(
    transponders: List[GPTransponder],
) -> Tuple[GPPositionENU, GPPositionLLH]:
    """
    Calculate the average position of the transponders

    Args:
        transponders: List of transponders

    Returns:
        Tuple[PositionENU, PositionLLH]: Average position in ENU and LLH
    """
    pos_array_llh = []
    pos_array_enu = []
    for transponder in transponders:
        pos_array_llh.append(
            [
                transponder.position_llh.latitude,
                transponder.position_llh.longitude,
                transponder.position_llh.height,
            ]
        )
        pos_array_enu.append(transponder.position_enu.get_position())
    avg_pos_llh = np.mean(pos_array_llh, axis=0).tolist()
    avg_pos_enu = np.mean(pos_array_enu, axis=0).tolist()

    min_pos_llh = np.min(pos_array_llh, axis=0).tolist()

    out_pos_llh = GPPositionLLH(
        latitude=avg_pos_llh[0], longitude=avg_pos_llh[1], height=avg_pos_llh[2]
    )
    out_pos_enu = GPPositionENU(
        east=avg_pos_enu[0], north=avg_pos_enu[1], up=avg_pos_enu[2]
    )

    return out_pos_enu, out_pos_llh


class GarposHandler:
    """
    GarposHandler is a class that handles the processing and preparation of shot data for the GARPOS model.
    It includes methods for rectifying shot data, preparing shot data files, setting inversion parameters,
    generating observation parameter files, generating data files with fixed parameters, and running the GARPOS model.

    Note:
        Respective instances of this class are intended to be for individual stations only.

    Attributes:
        LIB_DIRECTORY (str): Directory path for the RayTrace library.
        LIB_RAYTRACE (str): Path to the RayTrace library.
        shotdata (TDBShotDataArray): Array containing shot data.
        site_config (SiteConfig): Configuration for the site.
        working_dir (Path): Working directory path.
        shotdata_dir (Path): Directory path for shot data.
        results_dir (Path): Directory path for results.
        inversion_params (InversionParams): Parameters for the inversion process.
        dates (list): List of unique dates from the shot data.
        coord_transformer (CoordTransformer): Coordinate transformer for converting coordinates.

    Methods:
        __init__(self, shotdata: TDBShotDataArray, site_config: SiteConfig, working_dir: Path):
            Initializes the GarposHandler with shot data, site configuration, and working directory.
        _rectify_shotdata(self, shot_data: pd.DataFrame) -> pd.DataFrame:
        prep_shotdata(self, overwrite: bool = False):
            Prepares and saves shot data for each date in the object's date list.
        set_inversion_params(self, parameters: dict | InversionParams):
            Sets inversion parameters for the model.
        _input_to_datafile(self, shot_data: Path, path: Path, n_shot: int) -> None:
        _garposfixed_to_datafile(self, inversion_params: InversionParams, path: Path) -> None:
        _run_garpos(self, date: datetime, run_id: int | str = 0) -> GarposResults:
            Runs the GARPOS model for a given date and run ID.
        run_garpos(self, date_index: int = None, run_id: int | str = 0) -> None:
            Runs the GARPOS model for a specific date or for all dates.
    """

    def __init__(self, 
                 network: str,
                 station: str,
                 station_data: StationData,
                 site_data: Site,
                 working_dir: Path):
        """
        Initializes the class with shot data, site configuration, and working directory.
        Args:
            shotdata (TDBShotDataArray): The shot data array.
            site_config (SiteConfig): The site configuration.
            working_dir (Path): The working directory path.
        """

        self.garpos_fixed = GarposFixed()
        self.shotdata = TDBShotDataArray(station_data.shotdata)
        self.site = site_data
        self.working_dir = working_dir
        self.shotdata_dir = working_dir / SHOTDATA_DIR_NAME
        self.shotdata_dir.mkdir(exist_ok=True, parents=True)
        self.results_dir = working_dir / RESULTS_DIR_NAME
        self.results_dir.mkdir(exist_ok=True, parents=True)

        self.network = network
        self.station = station
        self.current_campaign = None
        self.current_survey = None
        self.coord_transformer = None

        self.garpos_fixed._to_datafile(path=self.working_dir/DEFAULT_SETTINGS_FILE_NAME)

        logger.loginfo(f"Garpos Handler initialized with working directory: {self.working_dir}")

    def set_campaign(self, name: str, catalog_db_path: Path = None):
        """
        Set the current campaign to the one with the given name, create the working directory for the campaign, 
        initialize the coordinate transformer, and set the sound speed profile file.

        Args:
            name (str): The name of the campaign to set as current.
            catalog_db_path (Path): The path to the catalog database. Default is None.
            local_svp (Path): The path to the local sound speed profile file. Default is None.

        Raises:
            ValueError: If the campaign with the given name is not found in the site data.
        """
        for campaign in self.site.campaigns:
            if campaign.name == name:
                self.current_campaign = campaign
                self.current_campaign_dir = self.working_dir / self.current_campaign.name
                self.current_campaign_dir.mkdir(exist_ok=True)
                self.coord_transformer = CoordTransformer(
                    latitude=self.site.arrayCenter.latitude,
                    longitude=self.site.arrayCenter.longitude,
                    elevation=-float(self.site.localGeoidHeight) # use negatiive value to account for garpos error "ys is shallower than layer"
                )
                self.current_survey = None

                # Set the path to the sound speed profile file
                self.sound_speed_path = self.current_campaign_dir / SVP_FILE_NAME
                
                # # If sound speed profile exists, use it, otherwise grab it from the catalog or the archive
                # if not self.sound_speed_path.exists():
                #     if local_svp: # TODO what if local CTD.. make them convert to use
                #         logger.loginfo(f"Using local sound speed profile found at {local_svp}..")
                #         self.sound_speed_path = local_svp
                #     else: 
                #         self._check_CTDs_in_catalog(campaign_name=name, catalog_db_path=catalog_db_path)

                logger.loginfo(
                    f"Campaign {name} set. Current campaign directory: {self.current_campaign_dir}"
                )

                return
            
        raise ValueError(
            f"campaign {name} not found among: {[x.name for x in self.site.campaigns]}"
        )
    
    def load_sound_speed_data(self, local_svp: Path = None, local_ctd: Path = None):
        """
        Load the sound speed profile from a local file or from the catalog.
        Args:
            local_svp (Path): The path to the local sound speed profile file. Default is None.
        """
        if local_svp:
            self.sound_speed_path = local_svp
            logger.loginfo(f"Using local sound speed profile found at {local_svp}..")
        elif local_ctd:
            logger.loginfo(f"Using local CTD file found at {local_ctd}, converting to sound speed profile..")
            df = CTDfile_to_svp(source=local_ctd)
            df.to_csv(self.sound_speed_path, index=False)
            logger.loginfo(f"Converted {local_ctd} to sound velocity profile at {self.sound_speed_path}")
        else:
            self._check_CTDs_in_catalog(campaign_name=self.current_campaign.name)
    
    def _check_CTDs_in_catalog(self, campaign_name: str, catalog_db_path: Path = None):
        """
        This function will check the catalog database for SVP or CTD files related to the current campaign. If found and local, set as sound 
        speed file or convert to SVP. If only remote, download it first and then set or convert to sound speed profile. 

        If no files are found in catalog, raise an error.
        
        Args:
            campaign_name (str): The name of the campaign to check for CTD files.
            catalog_db_path (Path): The path to the catalog database. Default is None. Will check in local working directory if not provided.

        Raises:
            ValueError: If no CTD files are found for the campaign in the catalog or if the catalog database path is not found or provided.
        """

        if not catalog_db_path:
            # Check if we can find it first based on the classic working directory structure, if not, raise error
            catalog_db_path = self.working_dir.parents[2]/"catalog.sqlite"
            logger.logdebug(f"Catalog database path not provided, checking for local catalog database at: {str(catalog_db_path)}")
            if not catalog_db_path.exists():
                raise ValueError("No local SVP found and no catalog database path provided, " \
                "\n please provide the catalog database path to check for the CTD files or a local SVP file")
            else:
                logger.logdebug(f"Using local catalog database found at {catalog_db_path}..")

        logger.loginfo(f"Checking catalog database for SVP, CTD, and SEABIRD files related to campaign {campaign_name}..")
        catalog = PreProcessCatalog(db_path=catalog_db_path)

        # Get the CTD files related to the current campaign
        ctd_assets: List[AssetEntry] = catalog.get_ctds(station=self.site.names[0], campaign=campaign_name)

        if not ctd_assets:
            raise ValueError(f"No SVP, CTD, or SEABIRD files found for campaign {campaign_name} in the catalog, " \
                             "use the data handler add_ctds_to_catalog() to catalog available CTD files, or provide a local SVP file")

        for file in ctd_assets:
            logger.loginfo(f"Found {file.type} files related to campaign {campaign_name}")

        # Prioritize SVP then CTD then Seabird  # TODO: ask which is preferred (ctd vs seabird)
        preferred_types = [AssetType.SVP, AssetType.CTD, AssetType.SEABIRD]       
        for preferred in preferred_types:
            for file in ctd_assets:
                if file.type == preferred:
                    # Check if the file is local or remote only
                    if file.local_path is None and file.remote_path is not None:
                        local_path = self.current_campaign_dir / file.remote_path.split("/")[-1]
                        download_file_from_archive(url=file.remote_path, dest_dir=self.current_campaign_dir)
                        
                        if not local_path.exists():
                            raise ValueError(f"File {local_path} not downloaded")
                        
                        logger.loginfo(f"Downloaded {file.remote_path} to {local_path}")
                        catalog.update_local_path(id=file.id, local_path=str(local_path))

                    elif file.local_path is not None:
                        local_path = file.local_path
                        
                    else:
                        continue

                    # Convert to sound velocity profile
                    if preferred == AssetType.SVP:
                        logger.loginfo(f"Using local sound speed profile found at {local_path}..")
                        self.sound_speed_path = local_path
                        return 
                    elif preferred == AssetType.SEABIRD:
                        logger.loginfo(f"Converting seabird file: {local_path} to sound velocity profile")
                        df = seabird_to_soundvelocity(source=local_path)
                    elif preferred == AssetType.CTD:
                        logger.loginfo(f"Converting CTD file: {local_path} to sound velocity profile")
                        df = CTDfile_to_svp(source=local_path)
                    else:
                        raise ValueError(f"Unknown file type {file.type} for file {local_path}")

                    df.to_csv(self.sound_speed_path)#, index=False)
                    logger.loginfo(f"Converted {local_path} to sound velocity profile at {self.sound_speed_path}, adding to catalog")
                    catalog.add_entry(AssetEntry(local_path=str(self.sound_speed_path), 
                                                 timestamp_created=datetime.now(), 
                                                 type=AssetType.SVP,
                                                 network=file.network,
                                                 station=file.station,
                                                 campaign=file.campaign,
                                                 ))
                    return  # Only process the first preferred file found
    

    def set_survey(self, name: str):
        """
        Set the current survey to the one with the given name.
        Args:
            name (str): The name of the survey to set as current.
        Raises:
            ValueError: If the survey with the given name is not found in the current campaign.
        """

        for survey in self.current_campaign.surveys:
            if survey.id == name:
                self.current_survey = survey
                logger.loginfo(
                    f"Current survey set to: {self.current_survey.id} {self.current_survey.start} - {self.current_survey.end}"
                )
                return
        raise ValueError(
            f"Survey {name} not found among: {[x.id for x in self.current_campaign.surveys]}"
        )

    def get_obsfile_path(self, campaign_name: str, survey_id: str) -> Path:
        """
        Get the path to the observation file for a given campaign and survey.
        Args:
            campaign_name (str): The name of the campaign.
            survey_id (str): The ID of the survey.
        Returns:
            obs_path (Path): The path to the observation file.
        """

        obs_path = self.working_dir / campaign_name / survey_id / OBSERVATION_FILE_NAME

        return obs_path
    

    def _get_array_dpos_center(self, transponders: List[GPTransponder]):
        """
        Get the average transponder position in ENU coordinates.
        Args:
            GPtransponders (List[GPTransponder]): List of GPTransponder objects.
        Returns:
            Tuple[GPPositionENU, GPPositionLLH]: Average transponder position in ENU and LLH coordinates.
        """
        _, array_center_llh = avg_transponder_position(transponders)
        array_dpos_center = self.coord_transformer.LLH2ENU(
            lat=array_center_llh.latitude,
            lon=array_center_llh.longitude,
            hgt=array_center_llh.height
        )

        return array_dpos_center
    
    def _create_GPTransponder(self, benchmark: Benchmark, transponder: Transponder) -> GPTransponder:
        """
        Create a GPTransponder object from a benchmark and transponder.
        Args:
            benchmark (Benchmark): The benchmark object.
            transponder (Transponder): The transponder object.
        Returns:
            GPTransponder: The created GPTransponder object.
        """

        # Set up transponder
        gp_transponder = GPTransponder(
        position_llh=GPPositionLLH(
            latitude=benchmark.aPrioriLocation.latitude,
            longitude=benchmark.aPrioriLocation.longitude,
            height=float(benchmark.aPrioriLocation.elevation),
        ),
        tat_offset = transponder.tat[0].value,
        id = transponder.address,
        name = benchmark.benchmarkID,
        )

        # Rectify to local coordinates
        gp_transponder_enu:Tuple = self.coord_transformer.LLH2ENU(
            lat=gp_transponder.position_llh.latitude,
            lon=gp_transponder.position_llh.longitude,
            hgt=gp_transponder.position_llh.height
        )
        gp_transponder_enu = GPPositionENU(
            east = gp_transponder_enu[0],
            north=gp_transponder_enu[1],
            up=gp_transponder_enu[2]
        )
        gp_transponder.position_enu = gp_transponder_enu

        return gp_transponder

    def prep_shotdata(self, overwrite: bool = False):
        """ 
        Prepares and saves shot data for each date in the object's date list.
        Args:
            overwrite (bool): If True, overwrite existing files. Defaults to False.
        """

        for survey in self.current_campaign.surveys:

            # Generate the path to the observation file
            obsfile_path = self.get_obsfile_path(
                campaign_name=self.current_campaign.name, survey_id=survey.id
            )
            # Check if the observation file already exists and skip if not overwriting
            if obsfile_path.exists() and not overwrite:
                continue

            # Read shotdata datafram and then check if the shot data is empty, if empty, skip..
            shot_data_queried: pd.DataFrame = self.shotdata.read_df(
                    start=survey.start, end=survey.end
                )
            if shot_data_queried.empty:
                print(f"No shot data found for survey {survey.id}")
                continue

            # Create the survey directory (CAMPAIGN/SURVEY_ID), if it doesn't exist
            survey_dir = self.current_campaign_dir / survey.id
            survey_dir.mkdir(exist_ok=True)

            survey_benchmarks = []
            for benchmark in self.site.benchmarks:
                if benchmark.name in survey.benchmarkIDs:
                    survey_benchmarks.append(benchmark)

            GPtransponders = []
            for benchmark in survey_benchmarks:
                # Find correct transponder, default to first
                #current_transponder = benchmark.get_transponder_by_datetime(survey.start)
                current_transponder = None
                while current_transponder is None:
                    if len(benchmark.transponders) == 1:
                        current_transponder = benchmark.transponders[0]
                        break

                    # If there are multiple transponders, check if the datetime is within the start and end dates
                    for transponder in benchmark.transponders:
                        if transponder.start <= survey.start:
                            if transponder.end is None or transponder.end >= survey.end:
                                current_transponder = transponder
                                break

                #current_transponder = benchmark.transponders[-1]    # TODO: this does not find a specifc transponder
                # for transponder in benchmark.transponders:
                #     if survey.start > transponder.start:
                #         current_transponder = transponder

                gp_transponder = self._create_GPTransponder(benchmark=benchmark, 
                                                       transponder=current_transponder)
                GPtransponders.append(gp_transponder)

            # Check if any transponders were found for the survey
            if len(GPtransponders) == 0:
                logger.logwarn(f"No transponders found for survey {survey.id}")
                continue

            # Get average transponder position
            array_dpos_center = self._get_array_dpos_center(GPtransponders)

            # Create shot data path with survey Id and type
            survey_type = survey.type.replace(" ", "")
            shot_data_path = (survey_dir/ f"{survey.id}_{survey_type}.csv")

            # Get day of year for start and end dates
            start_doy = survey.start.timetuple().tm_yday
            end_doy = survey.end.timetuple().tm_yday

            logger.loginfo("Preparing shot data")
            shot_data_rectified = rectify_shotdata(coord_transformer=self.coord_transformer,
                                                   shot_data=shot_data_queried)

            try:
                # shot_data_rectified = ShotDataFrame.validate(
                #     shot_data_rectified.reset_index(drop=True), lazy=True
                # )
                # Only use shotdata for transponders in the survey
                shot_data_rectified = shot_data_rectified[
                    shot_data_rectified.MT.isin([x.id for x in GPtransponders])
                ]

                shot_data_rectified.MT = shot_data_rectified.MT.apply(
                    lambda x: "M" + str(x) if str(x)[0].isdigit() else str(x)
                )

                shot_data_rectified.to_csv(str(shot_data_path))
                logger.loginfo(f"Shot data prepared and saved to {str(shot_data_path)}")


            except Exception as e:
                msg = (
                    f"Shot data for {survey.id} {survey_type} {start_doy} {end_doy} failed validation. "
                    f"Original error: {e}"
                )
                logger.logerr(msg)
                raise ValueError(msg) from e

            
            # Get soundspeed relative path
            rel_depth = len(shot_data_path.relative_to(self.sound_speed_path.parent).parts) -1
            ss_path = "../"*rel_depth + self.sound_speed_path.name 

            # Create the garpos input file
            garpos_input = GarposInput(
                site_name=self.site.names[0],
                campaign_id=self.current_campaign.name ,
                survey_id=survey.id ,
                site_center_llh=GPPositionLLH(
                    latitude=self.site.arrayCenter.latitude,
                    longitude=self.site.arrayCenter.longitude,
                    height=float(self.site.localGeoidHeight)
                ),
                array_center_enu=GPPositionENU(
                    east=array_dpos_center[0],
                    north=array_dpos_center[1],
                    up=array_dpos_center[2]
                ),
                transponders=GPtransponders,
                atd_offset=GPATDOffset(
                    forward=float(self.current_campaign.vessel.atdOffsets[0].x),
                    rightward=float(self.current_campaign.vessel.atdOffsets[0].y),
                    downward=float(self.current_campaign.vessel.atdOffsets[0].z),
                ),
                start_date=survey.start,
                end_date=survey.end,
                shot_data="./"+shot_data_path.name,
                delta_center_position=self.garpos_fixed.inversion_params.delta_center_position,
                sound_speed_data=ss_path,
                n_shot=len(shot_data_rectified)
            )
            garpos_input.to_datafile(obsfile_path)

            # Save the survey metadata
            with open(survey_dir/SURVEY_METADATA_FILE_NAME, 'w') as file:
                file.write(survey.model_dump_json(indent=2))

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
        obs_file_path:Path,
        results_dir:Path,
        run_id: int | str = 0,
        override: bool = False,
    ) -> None:
        """
        Run the GARPOS model for a given date and run ID.

        Args:
            date (datetime): The date for which to run the GARPOS model.
            run_id (int | str, optional): The run identifier. Defaults to 0.

        Returns:
            GarposResults: The results of the GARPOS model run.

        Raises:
            AssertionError: If the shot data file does not exist for the given date.

        This method performs the following steps:
        1. Extracts the year and day of year (DOY) from the given date.
        2. Constructs the path to the shot data file and checks its existence.
        3. Reads the shot data from the CSV file.
        4. Creates a results directory for the given year and DOY.
        5. Prepares input and settings files for the GARPOS model.
        6. Runs the GARPOS model using the prepared input and settings files.
        7. Processes the GARPOS model results.
        8. Saves the processed results to a JSON file.
        9. Saves the results DataFrame to a CSV file.
        """

        garpos_input = GarposInput.from_datafile(obs_file_path)
        results_path = results_dir / f"_{run_id}_results.json"
        results_df_path: Path = results_dir / f"_{run_id}_results_df.csv"

        if results_path.exists() and not override:
            print(f"Results already exist for {str(results_path)}")
            return

        logger.loginfo(
            f"Running GARPOS model for {garpos_input.site_name}, {self.current_survey.id}. Run ID: {run_id}"
        )

        results_dir.mkdir(exist_ok=True, parents=True)
        garpos_input.shot_data = results_dir.parent / garpos_input.shot_data.name
        garpos_input.sound_speed_data = self.sound_speed_path #obs_file_path.parent.parent.parent / garpos_input.sound_speed_data.name
        input_path = results_dir / f"_{run_id}_observation.ini"
        fixed_path = results_dir / f"_{run_id}_settings.ini"
        garpos_input.to_datafile(input_path)
        self.garpos_fixed._to_datafile(fixed_path)

        #print(f"Running GARPOS for {garpos_input.site_name}, {self.current_survey.id}")
        rf = drive_garpos(
            str(input_path),
            str(fixed_path),
            str(results_dir) + "/",
            f"{self.current_survey.id}_{run_id}",
            13,
        )

        results = GarposInput.from_datafile(rf)
        proc_results, results_df = process_garpos_results(results)

        results_df.to_csv(results_df_path, index=False)
        proc_results.shot_data = results_df_path
        with open(results_path, "w") as f:
            json.dump(proc_results.model_dump(), f, indent=4)

    def _run_garpos_survey(
        self, survey_id: str, run_id: int | str = 0, override: bool = False
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

        # Create the results directory for the survey
        results_dir = self.current_campaign_dir / survey_id / RESULTS_DIR_NAME
        results_dir.mkdir(exist_ok=True, parents=True)

        obsfile_path = self.get_obsfile_path(campaign_name=self.current_campaign.name,
                                             survey_id=survey_id)
        
        if not obsfile_path.exists():
            raise ValueError(f"Observation file not found at {obsfile_path}")

        self._run_garpos(obs_file_path=obsfile_path,
                         results_dir=results_dir,
                         run_id=run_id,
                         override=override)

    def run_garpos(
        self, campaign_id: str, survey_id: str = None, run_id: int | str = 0, override: bool = False
    ) -> None:

        """
        Run the GARPOS model for a specific date or for all dates.
        Args:
            campaign_id (str): The ID of the campaign to run.
            survey_id (str, optional): The ID of the survey to run. Defaults to None.
            run_id (int | str, optional): The run identifier. Defaults to 0.
            override (bool, optional): If True, override existing results. Defaults to False.
        Returns:
            None
        """
        if campaign_id != self.current_campaign.name:
            self.set_campaign(campaign_id)

        logger.loginfo(f"Running GARPOS model. Run ID: {run_id}")
        if survey_id is None:
            for survey in self.current_campaign.surveys:
                logger.loginfo(f"Running GARPOS model for survey {survey.id}. Run ID: {run_id}")
                self._run_garpos_survey(survey_id=survey.id, 
                                        run_id=run_id, 
                                        override=override)
                run_id += 1
        else:
            logger.loginfo(f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}")
            try:
                self._run_garpos_survey(survey_id=survey_id, 
                                        run_id=run_id, 
                                        override=override)
            except IndexError as e:
                logger.logerr(f"GARPOS model run failed for survey {survey_id}. Error: {e}")

    def plot_ts_results(
        self, 
        campaign_name: str = None, 
        survey_id: str= None, 
        run_id: int | str = 0, 
        res_filter: float = 10,
        savefig: bool = False
    ) -> None:
        """
        Plots the time series results for a given survey.
        Args:
            campaign_name (str, optional):The name of the campaign to plot results for. Default is None.
            survey_id (str): ID of the survey to plot results for.
            run_id (int or str, optional): The run ID of the survey results to plot. Default is 0.
            res_filter (float, optional): The residual filter value to filter outrageous values (m). Default is 10.
            savefig (bool, optional): If True, save the figure. Default is False.
        
        Returns:
            None

        Notes:
            - The function reads survey results from a JSON file and a CSV file.
            - It filters the results based on the residual range.
            - It generates multiple plots including scatter plots, line plots, box plots, and histograms.
            - The plots include information about the delta center position and transponder positions.
        """

        if campaign_name is not None:
            self.set_campaign(campaign_name)
        self.set_survey(survey_id)
        obsfile_path = self.get_obsfile_path(self.current_campaign.name,self.current_survey.id)

        results_dir = obsfile_path.parent / RESULTS_DIR_NAME
        results_path = results_dir / f"_{run_id}_results.json"
        with open(results_path, "r") as f:
            results = json.load(f)
            garpos_results = GarposInput(**results)
            arrayinfo = garpos_results.delta_center_position

        results_df_raw = pd.read_csv(garpos_results.shot_data)
        results_df_raw = ShotDataFrame.validate(results_df_raw, lazy=True)
        results_df_raw["time"] = results_df_raw.ST.apply(
            lambda x: datetime.fromtimestamp(x)
        )
        df_filter = results_df_raw["ResiRange"].abs() < res_filter
        results_df = results_df_raw[df_filter]
        unique_ids = results_df["MT"].unique()

        plt.figsize = (32, 32)
        plt.suptitle(f"Survey {survey_id} Results")
        gs = gridspec.GridSpec(13, 16)
        figure_text = "Delta Center Position\n"
        dpos = arrayinfo.get_position()
        figure_text += f"Array :  East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"
        for id, transponder in enumerate(garpos_results.transponders):
            dpos = transponder.delta_center_position.get_position()
            figure_text += f"TSP {transponder.id} : East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"

        print(figure_text)
        ax3 = plt.subplot(gs[6:, 8:])
        ax3.set_aspect("equal", "box")
        ax3.set_xlabel("East (m)")
        ax3.set_ylabel("North (m)", labelpad=-1)
        colormap_times = results_df_raw.ST.to_numpy()
        colormap_times_scaled = (colormap_times - colormap_times.min()) / 3600
        norm = Normalize(
            vmin=0,
            vmax=(colormap_times.max() - colormap_times.min()) / 3600,
        )
        sc = ax3.scatter(
            results_df_raw["ant_e0"],
            results_df_raw["ant_n0"],
            c=colormap_times_scaled,
            cmap="viridis",
            label="Vessel",
            norm=norm,
            alpha=0.25,
        )
        ax3.scatter(0, 0, label="Origin", color="magenta", s=100)
        ax1 = plt.subplot(gs[1:5, :])
        points = np.array(
            [
                mdates.date2num(results_df["time"]),
                np.zeros(len(results_df["time"])),
            ]
        ).T.reshape(-1, 1, 2)
        segments = np.concatenate([points[:-1], points[1:]], axis=1)
        lc = LineCollection(segments, cmap="viridis", norm=norm, linewidth=5, zorder=10)
        lc.set_array(colormap_times_scaled)
        ax1.add_collection(lc)
        for i, unique_id in enumerate(unique_ids):
            df = results_df[results_df["MT"] == unique_id].sort_values("time")
            ax1.plot(
                df["time"],
                df["ResiRange"],
                label=f"{unique_id}",
                color=colors[i],
                linewidth=1,
                zorder=i,
                alpha=0.75,
            )
        ax1.set_xlabel("Time - Month / Day / Hour")
        ax1.set_ylabel("Residuals - Range (M)", labelpad=-1)
        ax1.xaxis.set_label_position("top")
        ax1.xaxis.set_ticks_position("top")
        ax1.legend()
        for transponder in garpos_results.transponders:
            idx = unique_ids.tolist().index(transponder.id)
            ax3.scatter(
                transponder.position_enu.east,
                transponder.position_enu.north,
                label=f"{transponder.id}",
                color=colors[idx],
                s=100,
            )
        cbar = plt.colorbar(sc, label="Time (hr)", norm=norm)
        ax3.legend()
        ax2 = plt.subplot(gs[6:9, :7])
        resiRange = results_df_raw["ResiRange"]
        resiRange_np = resiRange.to_numpy()
        resiRange_filter = np.abs(resiRange_np) < 50
        resiRange = resiRange[resiRange_filter]
        flier_props = dict(marker=".", markerfacecolor="r", markersize=5, alpha=0.25)
        ax2.boxplot(resiRange.to_numpy(), vert=False, flierprops=flier_props)
        median = resiRange.median()
        # Get the 1st and 2nd interquartile range
        q1 = resiRange.quantile(0.25)
        q3 = resiRange.quantile(0.75)
        ax2.text(
            0.5,
            1.2,
            f"Median: {median:.2f} , IQR 1: {q1:.2f}, IQR 3: {q3:.2f}",
            fontsize=10,
            verticalalignment="center",
            horizontalalignment="center",
        )
        ax2.set_xlabel("Residual Range (m)", labelpad=-1)
        # Place ax2 x ticks on top
        ax2.yaxis.set_visible(False)
        ax2.set_title("Box Plot of Residual Range Values")
        bins = np.arange(-res_filter, res_filter, 0.05)
        counts, bins = np.histogram(resiRange_np, bins=bins, density=True)
        ax4 = plt.subplot(gs[10:, :7])
        ax4.sharex(ax2)
        ax4.hist(bins[:-1], bins, weights=counts, edgecolor="black")
        ax4.axvline(median, color="blue", linestyle="-", label=f"Median: {median:.3f}")
        ax4.set_xlabel("Residual Range (m)", labelpad=-1)
        ax4.set_ylabel("Frequency")
        ax4.set_title(
            f"Histogram of Residual Range Values, within {res_filter:.1f} meters"
        )
        ax4.legend()
        plt.show()
        if savefig:
            plt.savefig(
                results_dir / f"_{run_id}_results.png",
                dpi=300,
                bbox_inches="tight",
                pad_inches=0.1,
            )
