"""
This module contains the GarposDataPreparer class, which is responsible for preparing GARPOS input data.
"""

from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
from datetime import datetime

from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.data_models.metadata.benchmark import Benchmark, Transponder
from es_sfgtools.data_models.metadata.campaign import Survey, Campaign
from es_sfgtools.data_models.observables import ShotDataFrame
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposInput,
    GPTransponder,
    GPATDOffset,
    GPPositionENU,
    GPPositionLLH,
)
from es_sfgtools.modeling.garpos_tools.functions import CoordTransformer, rectify_shotdata
from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.tiledb_tools.tiledb_schemas import TDBShotDataArray
from es_sfgtools.modeling.garpos_tools.shot_data_utils import (
    filter_ping_replies,
    filter_wg_distance_from_center,
    good_acoustic_diagnostics,
    ok_acoustic_diagnostics,
    difficult_acoustic_diagnostics,
    filter_pride_residuals,
    DEFAULT_FILTER_CONFIG
)

SHOTDATA_DIR_NAME = "shotdata"
OBSERVATION_FILE_NAME = "observation.ini"

class NoShotDataError(Exception):
    """
    Custom exception raised when no shot data is found for a given survey.
    This exception is used to indicate that the shot data for a specific survey is empty or not available.
    """
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

class NoGPTranspondersError(Exception):
    """
    Custom exception raised when no GP transponders are found for a given survey.
    This exception is used to indicate that the GP transponders for a specific survey are empty or not available.
    """
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

class GarposDataPreparer:
    """
    A class to prepare GARPOS input data.
    """

    def __init__(
        self,
        site: Site,
        campaign: Campaign,
        station_data: dict,
        working_dir: Path,
        shotdata_filter_config: dict = DEFAULT_FILTER_CONFIG,
    ):
        """
        Initializes the GarposDataPreparer.

        Args:
            site (Site): The site metadata.
            campaign (Campaign): The campaign metadata.
            station_data (dict): The station data containing shot data.
            working_dir (Path): The working directory.
            shotdata_filter_config (dict, optional): The configuration for filtering shot data. Defaults to DEFAULT_FILTER_CONFIG.
        """
        self.site = site
        self.campaign = campaign
        self.station_data = station_data
        self.working_dir = working_dir
        self.shotdata_filter_config = shotdata_filter_config
        self.shotdata = TDBShotDataArray(station_data.shotdata)
        self.shotdata_dir = working_dir / SHOTDATA_DIR_NAME
        self.shotdata_dir.mkdir(exist_ok=True, parents=True)
        self.coord_transformer = CoordTransformer(
            latitude=self.site.arrayCenter.latitude,
            longitude=self.site.arrayCenter.longitude,
            elevation=-float(self.site.localGeoidHeight),
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
        if not self.shotdata_dir.exists():
            self.shotdata_dir.mkdir(parents=True, exist_ok=True)

        for survey in self.campaign.surveys:
            obsfile_path = self.get_obsfile_path(survey_id=survey.id)
            if obsfile_path.exists() and not overwrite:
                continue

            survey_dir = self.working_dir / survey.id
            survey_dir.mkdir(exist_ok=True)

            try:
                shot_data_queried: pd.DataFrame = self.save_shotdata_from_tiledb(
                    survey_ID=survey.id, start=survey.start, end=survey.end
                )
            except NoShotDataError as e:
                logger.logwarn(f"No shot data found for survey {survey.id}: {e.message}")
                continue

            shot_data_filtered = self.filter_shotdata(
                survey_type=survey.type,
                shot_data=shot_data_queried,
                start_time=survey.start,
                end_time=survey.end,
                custom_filters=custom_filters,
            )
            if shot_data_filtered.empty:
                logger.logwarn(
                    f"No shot data remaining after filtering for survey {survey.id}, skipping survey."
                )
                continue

            filtered_shotdata_path = (
                self.shotdata_dir / f"{survey.id}_{survey.type.replace(' ', '')}_shotdata_filtered.csv"
            )
            shot_data_filtered.to_csv(filtered_shotdata_path)

            try:
                GPtransponders = self._GP_Transponders_from_benchmarks(survey=survey)
                array_dpos_center = self._get_array_dpos_center(GPtransponders)
            except NoGPTranspondersError as e:
                continue

            try:
                shot_data_rectified = self._prepare_shotdata_for_garpos(
                    shot_data_path=filtered_shotdata_path,
                    survey=survey,
                    shot_data=shot_data_filtered,
                    GPtransponders=GPtransponders,
                )

                garpos_shotdata_filename = f"{survey.id}_{survey.type.replace(' ', '')}_shotdata.csv"
                garpos_shotdata_path = survey_dir / garpos_shotdata_filename
                shot_data_rectified.to_csv(garpos_shotdata_path)

            except ValueError as e:
                continue

            try:
                rel_depth = (
                    len(garpos_shotdata_path.relative_to(self.sound_speed_path.parent).parts) - 1
                )
                ss_path = "../" * rel_depth + self.sound_speed_path.name
            except ValueError:
                ss_path = str(self.sound_speed_path)

            garpos_input = self._prepare_garpos_input_from_survey(
                shot_data_path=garpos_shotdata_path,
                survey=survey,
                ss_path=ss_path,
                array_dpos_center=array_dpos_center,
                num_of_shots=len(shot_data_rectified),
                GPtransponders=GPtransponders,
            )
            garpos_input.to_datafile(obsfile_path)

    def get_obsfile_path(self, survey_id: str) -> Path:
        """
        Get the path to the observation file for a given survey.
        Args:
            survey_id (str): The ID of the survey.
        Returns:
            obs_path (Path): The path to the observation file.
        """
        obs_path = self.working_dir / survey_id / OBSERVATION_FILE_NAME
        return obs_path

    def save_shotdata_from_tiledb(self, survey_ID: str, start: datetime, end: datetime) -> pd.DataFrame:
        """
        Grab shot data from the TileDB array for a given date range and save it to a CSV file in the shotdata directory.
        Args:
            survey_ID (str): The ID of the survey to query shot data for.
            start (datetime): The start date of the query.
            end (datetime): The end date of the query.
        Returns:
            pd.DataFrame: The queried shot data as a DataFrame.
        Raises:
            NoShotDataError: If no shot data is found for the given survey ID and date range.:
        """
        shot_data_queried: pd.DataFrame = self.shotdata.read_df(start=start, end=end)
        if shot_data_queried.empty:
            raise NoShotDataError(f"No shot data found for {survey_ID} from {start} to {end}")

        shot_data_queried.to_csv(self.shotdata_dir / f"{survey_ID}_shotdata.csv")
        logger.loginfo(f"Shot data saved to {self.shotdata_dir}")

        return shot_data_queried

    def filter_shotdata(
        self,
        survey_type: str,
        shot_data: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        custom_filters: Optional[dict] = None,
    ) -> pd.DataFrame:
        """
        Filter the shot data based on the specified acoustic level and minimum ping replies.
        Args:
            shot_data (pd.DataFrame): The shot data to filter.
            acoustic_level (acoustic_filter_level): The level of acoustic filtering to apply.
            ping_replies (int): The minimum number of replies required for each ping.
        Returns:
            pd.DataFrame: The filtered shot data.
        """
        initial_count = len(shot_data)
        new_shot_data_df = shot_data.copy()

        if custom_filters:
            self.shotdata_filter_config = custom_filters
            logger.loginfo(f"Using custom filter configuration: {self.shotdata_filter_config}")

        acoustic_config = self.shotdata_filter_config.get("acoustic_filters", {})
        if acoustic_config.get("enabled", True):
            level = acoustic_config.get("level", "OK")
            if level == "GOOD":
                new_shot_data_df = good_acoustic_diagnostics(new_shot_data_df)
            elif level == "OK":
                new_shot_data_df = ok_acoustic_diagnostics(new_shot_data_df)
            elif level == "DIFFICULT":
                new_shot_data_df = difficult_acoustic_diagnostics(new_shot_data_df)
            else:
                logger.loginfo(f"No acoustic filtering applied, using original shot data")

        ping_replies_config = self.shotdata_filter_config.get("ping_replies", {})
        if ping_replies_config.get("enabled", True):
            min_replies = ping_replies_config.get("min_replies", 3)
            new_shot_data_df = filter_ping_replies(new_shot_data_df, min_replies=min_replies)

        if survey_type.lower() == "center":
            max_distance = self.shotdata_filter_config.get("max_distance_from_center", {})
            if max_distance.get("enabled", True):
                max_distance_from_center = max_distance.get("max_distance_m", None)
                if max_distance_from_center is not None:
                    new_shot_data_df = filter_wg_distance_from_center(
                        df=new_shot_data_df,
                        array_center_lat=self.site.arrayCenter.latitude,
                        array_center_lon=self.site.arrayCenter.longitude,
                        max_distance_m=max_distance_from_center,
                    )

        if self.shotdata_filter_config.get("pride_residuals", {}).get("enabled", True):
            max_wrms = self.shotdata_filter_config.get("pride_residuals", {}).get(
                "max_residual_mm", None
            )
            if max_wrms is not None:
                new_shot_data_df = filter_pride_residuals(
                    df=new_shot_data_df,
                    station_data=self.station_data,
                    start_time=start_time,
                    end_time=end_time,
                    max_wrms=max_wrms,
                )

        filtered_count = len(new_shot_data_df)
        logger.loginfo(
            f"Filtered {initial_count - filtered_count} records from shot data based on filtering criteria: {self.shotdata_filter_config}"
        )
        logger.loginfo(f"Remaining shot data records: {filtered_count}")
        return new_shot_data_df

    def _get_array_dpos_center(self, transponders: List[GPTransponder]):
        """
        Get the average transponder position in ENU coordinates.
        Args:
            GPtransponders (List[GPTransponder]): List of GPTransponder objects.
        Returns:
            Tuple[GPPositionENU, GPPositionLLH]: Average transponder position in ENU and LLH coordinates.
        """
        _, array_center_llh = self._avg_transponder_position(transponders)
        array_dpos_center = self.coord_transformer.LLH2ENU(
            lat=array_center_llh.latitude,
            lon=array_center_llh.longitude,
            hgt=array_center_llh.height,
        )

        return array_dpos_center

    def _avg_transponder_position(
        self, transponders: List[GPTransponder]
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
        avg_pos_llh = pd.DataFrame(pos_array_llh).mean().tolist()
        avg_pos_enu = pd.DataFrame(pos_array_enu).mean().tolist()

        out_pos_llh = GPPositionLLH(
            latitude=avg_pos_llh[0], longitude=avg_pos_llh[1], height=avg_pos_llh[2]
        )
        out_pos_enu = GPPositionENU(
            east=avg_pos_enu[0], north=avg_pos_enu[1], up=avg_pos_enu[2]
        )

        return out_pos_enu, out_pos_llh

    def _create_GPTransponder(
        self, benchmark: Benchmark, transponder: Transponder
    ) -> GPTransponder:
        """
        Create a GPTransponder object from a benchmark and transponder.
        Args:
            benchmark (Benchmark): The benchmark object.
            transponder (Transponder): The transponder object.
        Returns:
            GPTransponder: The created GPTransponder object.
        """
        gp_transponder = GPTransponder(
            position_llh=GPPositionLLH(
                latitude=benchmark.aPrioriLocation.latitude,
                longitude=benchmark.aPrioriLocation.longitude,
                height=float(benchmark.aPrioriLocation.elevation),
            ),
            tat_offset=transponder.tat[0].value,
            id=transponder.address,
            name=benchmark.benchmarkID,
        )

        gp_transponder_enu: Tuple = self.coord_transformer.LLH2ENU(
            lat=gp_transponder.position_llh.latitude,
            lon=gp_transponder.position_llh.longitude,
            hgt=gp_transponder.position_llh.height,
        )
        gp_transponder_enu = GPPositionENU(
            east=gp_transponder_enu[0],
            north=gp_transponder_enu[1],
            up=gp_transponder_enu[2],
        )
        gp_transponder.position_enu = gp_transponder_enu

        return gp_transponder

    def _GP_Transponders_from_benchmarks(self, survey: Survey) -> List[GPTransponder]:
        """
        Get GP transponders from the benchmarks in the survey.
        Args:
            survey (Survey): The survey object.
        Returns:
            List[GPTransponder]: List of GPTransponder objects for the survey.
        Raises:
            NoGPTranspondersError: If no transponders are found for the survey.
        """
        survey_benchmarks = []
        for benchmark in self.site.benchmarks:
            if benchmark.name in survey.benchmarkIDs:
                survey_benchmarks.append(benchmark)

        GPtransponders = []
        for benchmark in survey_benchmarks:
            if len(benchmark.transponders) == 1:
                current_transponder = benchmark.transponders[0]
            else:
                for transponder in benchmark.transponders:
                    if transponder.start <= survey.start:
                        if transponder.end is None or transponder.end >= survey.end:
                            current_transponder = transponder
                            break

            gp_transponder = self._create_GPTransponder(
                benchmark=benchmark, transponder=current_transponder
            )
            GPtransponders.append(gp_transponder)

        if len(GPtransponders) == 0:
            raise NoGPTranspondersError(f"No transponders found for survey {survey.id}")
        return GPtransponders

    def _prepare_shotdata_for_garpos(
        self, shot_data_path: Path, survey: Survey, shot_data: pd.DataFrame, GPtransponders: List[GPTransponder]
    ):
        """
        Prepare the shot data for GARPOS by rectifying it and saving it to a CSV file.
        Args:
            shot_data_path (Path): The path to save the shot data CSV file.
            survey (Survey): The survey object containing start and end dates.
            shot_data (pd.DataFrame): The shot data DataFrame to be prepared.
            GPtransponders (List[GPTransponder]): List of GPTransponder objects for the survey.
        Returns:
            pd.DataFrame: The rectified shot data DataFrame.
        Raises:
            ValueError: If the shot data fails validation.
        """
        start_doy = survey.start.timetuple().tm_yday
        end_doy = survey.end.timetuple().tm_yday

        logger.loginfo("Preparing shot data")
        shot_data_rectified = rectify_shotdata(
            coord_transformer=self.coord_transformer, shot_data=shot_data
        )
        shot_data_rectified.MT = shot_data_rectified.MT.replace(r"\D", "", regex=True)

        try:
            shot_data_rectified = shot_data_rectified[
                shot_data_rectified.MT.isin([x.id for x in GPtransponders])
            ]

            shot_data_rectified.MT = shot_data_rectified.MT.apply(
                lambda x: "M" + str(x) if str(x)[0].isdigit() else str(x)
            )
            shot_data_rectified = shot_data_rectified.sort_values(by=["ST", "MT"]).reset_index(
                drop=True
            )
            shot_data_rectified.to_csv(str(shot_data_path))
            logger.loginfo(f"Shot data prepared and saved to {str(shot_data_path)}")

        except Exception as e:
            start_date = datetime(year=survey.start.year, month=1, day=1) + timedelta(
                days=start_doy - 1
            )
            raise ValueError(
                f"Shot data for {survey.id}| {start_doy} {end_doy} | {start_date} failed validation. "
                f"Original error: {e}"
            ) from e

        return shot_data_rectified

    def _prepare_garpos_input_from_survey(
        self,
        shot_data_path: Path,
        survey: Survey,
        ss_path: str,
        array_dpos_center: Tuple[float, float, float],
        num_of_shots: int,
        GPtransponders: List[GPTransponder],
    ) -> GarposInput:
        """
        Prepare the GarposInput object from the survey and shot data.
        Args:
            shot_data_path (Path): The path to the shot data CSV file.
            survey (Survey): The survey object.
            ss_path (str): The relative path to the sound speed profile file.
            array_dpos_center (Tuple[float, float, float]): The average position of the transponders in ENU coordinates.
            num_of_shots (int): The number of shots in the shot data.
        Returns:
            GarposInput: The prepared GarposInput object.
        """
        garpos_input = GarposInput(
            site_name=self.site.names[0],
            campaign_id=self.campaign.name,
            survey_id=survey.id,
            site_center_llh=GPPositionLLH(
                latitude=self.site.arrayCenter.latitude,
                longitude=self.site.arrayCenter.longitude,
                height=float(self.site.localGeoidHeight),
            ),
            array_center_enu=GPPositionENU(
                east=array_dpos_center[0],
                north=array_dpos_center[1],
                up=array_dpos_center[2],
            ),
            transponders=GPtransponders,
            atd_offset=GPATDOffset(
                forward=float(self.campaign.vessel.atdOffsets[0].x),
                rightward=float(self.campaign.vessel.atdOffsets[0].y),
                downward=float(self.campaign.vessel.atdOffsets[0].z),
            ),
            start_date=survey.start,
            end_date=survey.end,
            shot_data=shot_data_path,
            sound_speed_data=ss_path,
            n_shot=num_of_shots,
        )

        return garpos_input
