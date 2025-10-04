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
from es_sfgtools.data_mgmt.directory_handler import DirectoryHandler
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


def prepareShotData(
        network_name: str,
        station_name: str,
        campaign_name: str,
        site: Site,
        campaign: Campaign,
        directory_handler: DirectoryHandler,
        custom_filters: dict = None,
        shotdata_filter_config: dict = DEFAULT_FILTER_CONFIG,
        overwrite: bool = False,
) -> None:
    garposCampaignDir = directory_handler[network_name][station_name][campaign_name].garpos

    obsfile = garposCampaignDir[campaign.name].default_obsfile
    if obsfile.exists() and not overwrite:
        logger.loginfo(f"Observation file {obsfile} already exists, skipping shot data preparation.")
        return

    coordTransformer = CoordTransformer(
        latitude=site.arrayCenter.latitude,
        longitude=site.arrayCenter.longitude,
        elevation=-float(site.localGeoidHeight),
    )
    shotDataSource = TDBShotDataArray(directory_handler[network_name][station_name].tiledb_directory.shot_data)
    

    for survey in campaign.surveys:
        garposCampaignDir.add_survey(survey.id)
        surveyDir = garposCampaignDir[survey.id]
        with open(surveyDir.survey_metadata, 'w') as f:
            f.write(survey.model_dump_json(indent=4, exclude_none=True))

        shot_data_queried: pd.DataFrame = shotDataSource.read_df(
            start=survey.start, end=survey.end
        )
        if shot_data_queried.empty:
            logger.logwarn(f"No shot data found for survey {survey.id} from {survey.start} to {survey.end}, skipping survey.")
            continue
        file_name_unfiltered = f"{survey.id}__{survey.type}_shotdata.csv"
        shot_data_queried.to_csv(garposCampaignDir.shotdata / file_name_unfiltered)

        shot_data_filtered = filter_shotdata(
            survey_type=survey.type,
            site=site,
            shot_data=shot_data_queried,
            kinPostionTDBUri=directory_handler[network_name][station_name].tiledb_directory.kin_position_data,
            start_time=survey.start,
            end_time=survey.end,
            custom_filters=custom_filters,
            filter_config=shotdata_filter_config
        )

        if shot_data_filtered.empty:
            logger.logwarn(
                f"No shot data remaining after filtering for survey {survey.id}, skipping survey."
            )
            continue
        file_name_filtered = garposCampaignDir.shotdata / f"{survey.id}__{survey.type}_shotdata_filtered.csv"
        shot_data_filtered.to_csv(file_name_filtered)

        try:
            GPtransponders = GP_Transponders_from_benchmarks(coord_transformer=coordTransformer, survey=survey, site=site)
            array_dpos_center = get_array_dpos_center(coordTransformer, GPtransponders)
        except NoGPTranspondersError as e:
            continue
        try:
            shotdata_out_path = surveyDir.location / f"{survey.id}__{survey.type}_shotdata_filtered_rectified.csv"
            shot_data_rectified = prepare_shotdata_for_garpos(
                coord_transformer=coordTransformer,
                shodata_out_path=shotdata_out_path,
                shot_data=shot_data_filtered,
                GPtransponders=GPtransponders,
            )
            surveyDir.shotdata = shotdata_out_path

        except ValueError as e:
            continue
            # Get the sound speed profile path

        garpos_input = prepare_garpos_input_from_survey(
            shot_data_path=surveyDir.shotdata,
            survey=survey,
            site=site,
            campaign=campaign,
            ss_path=garposCampaignDir.svp_file,
            array_dpos_center=array_dpos_center,
            num_of_shots=len(shot_data_rectified),
            GPtransponders=GPtransponders,
        )
        garpos_input.to_datafile(surveyDir.default_obsfile)

        # save the survey directory metadata
        directory_handler.save()

def filter_shotdata(
    survey_type: str,
    site: Site,
    shot_data: pd.DataFrame,
    kinPostionTDBUri:str,
    start_time: datetime,
    end_time: datetime,
    custom_filters: Optional[dict] = None,
    filter_config: Optional[dict] = DEFAULT_FILTER_CONFIG.copy(),
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
        filter_config = filter_config.update(custom_filters)
        logger.loginfo(f"Using custom filter configuration: {filter_config}")

    acoustic_config = filter_config.get("acoustic_filters", {})
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

    ping_replies_config = filter_config.get("ping_replies", {})
    if ping_replies_config.get("enabled", True):
        min_replies = ping_replies_config.get("min_replies", 3)
        new_shot_data_df = filter_ping_replies(
            new_shot_data_df, min_replies=min_replies
        )

    if survey_type.lower() == "center":
        max_distance = filter_config.get("max_distance_from_center", {})
        if max_distance.get("enabled", True):
            max_distance_from_center = max_distance.get("max_distance_m", None)
            if max_distance_from_center is not None:
                new_shot_data_df = filter_wg_distance_from_center(
                    df=new_shot_data_df,
                    array_center_lat=site.arrayCenter.latitude,
                    array_center_lon=site.arrayCenter.longitude,
                    max_distance_m=max_distance_from_center,
                )

    if filter_config.get("pride_residuals", {}).get("enabled", True):
        max_wrms = filter_config.get("pride_residuals", {}).get(
            "max_residual_mm", None
        )
        if max_wrms is not None:
            new_shot_data_df = filter_pride_residuals(
                df=new_shot_data_df,
                kinPostionTDBUri=kinPostionTDBUri,
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

def GP_Transponders_from_benchmarks(coord_transformer: CoordTransformer, survey: Survey, site: Site) -> List[GPTransponder]:
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
    for benchmark in site.benchmarks:
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

        gp_transponder = create_GPTransponder(
            coord_transformer=coord_transformer, benchmark=benchmark, transponder=current_transponder
        )
        GPtransponders.append(gp_transponder)

    if len(GPtransponders) == 0:
        raise NoGPTranspondersError(f"No transponders found for survey {survey.id}")
    return GPtransponders

def create_GPTransponder(
    coord_transformer:CoordTransformer, benchmark: Benchmark, transponder: Transponder
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

    gp_transponder_enu: Tuple = coord_transformer.LLH2ENU(
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

def get_array_dpos_center(coord_transformer: CoordTransformer, transponders: List[GPTransponder]):
    """
    Get the average transponder position in ENU coordinates.
    Args:
        GPtransponders (List[GPTransponder]): List of GPTransponder objects.
    Returns:
        Tuple[GPPositionENU, GPPositionLLH]: Average transponder position in ENU and LLH coordinates.
    """
    _, array_center_llh = avg_transponder_position(transponders)
    array_dpos_center = coord_transformer.LLH2ENU(
        lat=array_center_llh.latitude,
        lon=array_center_llh.longitude,
        hgt=array_center_llh.height,
    )

    return array_dpos_center

def avg_transponder_position(
    transponders: List[GPTransponder]
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

def prepare_shotdata_for_garpos(
    coord_transformer: CoordTransformer,
    shodata_out_path: Path,
    shot_data: pd.DataFrame,
    GPtransponders: List[GPTransponder],
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

    shot_data_rectified = rectify_shotdata(
        coord_transformer=coord_transformer, shot_data=shot_data
    )
    shot_data_rectified.MT = shot_data_rectified.MT.replace(r"\D", "", regex=True)

    shot_data_rectified = shot_data_rectified[
        shot_data_rectified.MT.isin([x.id for x in GPtransponders])
    ]

    shot_data_rectified.MT = shot_data_rectified.MT.apply(
        lambda x: "M" + str(x) if str(x)[0].isdigit() else str(x)
    )
    shot_data_rectified = shot_data_rectified.sort_values(
        by=["ST", "MT"]
    ).reset_index(drop=True)
    shot_data_rectified.to_csv(str(shodata_out_path))
    logger.loginfo(f"Shot data prepared and saved to {str(shodata_out_path)}")

    return shot_data_rectified

def prepare_garpos_input_from_survey(
    shot_data_path: Path,
    survey: Survey,
    site: Site,
    campaign: Campaign,
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
        site_name=site.names[0],
        campaign_id=campaign.name,
        survey_id=survey.id,
        site_center_llh=GPPositionLLH(
            latitude=site.arrayCenter.latitude,
            longitude=site.arrayCenter.longitude,
            height=float(site.localGeoidHeight),
        ),
        array_center_enu=GPPositionENU(
            east=array_dpos_center[0],
            north=array_dpos_center[1],
            up=array_dpos_center[2],
        ),
        transponders=GPtransponders,
        atd_offset=GPATDOffset(
            forward=float(campaign.vessel.atdOffsets[0].x),
            rightward=float(campaign.vessel.atdOffsets[0].y),
            downward=float(campaign.vessel.atdOffsets[0].z),
        ),
        start_date=survey.start,
        end_date=survey.end,
        shot_data=shot_data_path,
        sound_speed_data=ss_path,
        n_shot=num_of_shots,
    )

    return garpos_input
