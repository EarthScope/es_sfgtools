"""
This module contains the GarposDataPreparer class, which is responsible for preparing GARPOS input data.
"""

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from es_sfgtools.data_models.metadata.benchmark import Benchmark, Transponder
from es_sfgtools.data_models.metadata.campaign import Campaign, Survey, SurveyType, classify_survey_type
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.modeling.garpos_tools.functions import (
    CoordTransformer,
    rectify_shotdata,
)
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposInput,
    GPATDOffset,
    GPPositionENU,
    GPPositionLLH,
    GPTransponder,
)

from es_sfgtools.modeling.garpos_tools.garpos_config import GarposSiteConfig, CENTER_DRIVE_SITE_CONFIG, DEFAULT_SITE_CONFIG

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


def GP_Transponders_from_benchmarks(coord_transformer: CoordTransformer, survey: Survey, site: Site) -> List[GPTransponder]:
    """
    Get GP transponders from the benchmarks in the survey.

    :param coord_transformer: The coordinate transformer.
    :type coord_transformer: CoordTransformer
    :param survey: The survey object.
    :type survey: Survey
    :param site: The site metadata.
    :type site: Site
    :return: List of GPTransponder objects for the survey.
    :rtype: List[GPTransponder]
    :raises NoGPTranspondersError: If no transponders are found for the survey.
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

    :param coord_transformer: The coordinate transformer.
    :type coord_transformer: CoordTransformer
    :param benchmark: The benchmark object.
    :type benchmark: Benchmark
    :param transponder: The transponder object.
    :type transponder: Transponder
    :return: The created GPTransponder object.
    :rtype: GPTransponder
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

    :param coord_transformer: The coordinate transformer.
    :type coord_transformer: CoordTransformer
    :param transponders: List of GPTransponder objects.
    :type transponders: List[GPTransponder]
    :return: Average transponder position in ENU and LLH coordinates.
    :rtype: Tuple[GPPositionENU, GPPositionLLH]
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

    :param transponders: List of transponders.
    :type transponders: List[GPTransponder]
    :return: Average position in ENU and LLH.
    :rtype: Tuple[GPPositionENU, GPPositionLLH]
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

    :param coord_transformer: The coordinate transformer.
    :type coord_transformer: CoordTransformer
    :param shodata_out_path: The path to save the shot data CSV file.
    :type shodata_out_path: Path
    :param shot_data: The shot data DataFrame to be prepared.
    :type shot_data: pd.DataFrame
    :param GPtransponders: List of GPTransponder objects for the survey.
    :type GPtransponders: List[GPTransponder]
    :return: The rectified shot data DataFrame.
    :rtype: pd.DataFrame
    :raises ValueError: If the shot data fails validation.
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

    :param shot_data_path: The path to the shot data CSV file.
    :type shot_data_path: Path
    :param survey: The survey object.
    :type survey: Survey
    :param site: The site metadata.
    :type site: Site
    :param campaign: The campaign metadata.
    :type campaign: Campaign
    :param ss_path: The relative path to the sound speed profile file.
    :type ss_path: str
    :param array_dpos_center: The average position of the transponders in ENU coordinates.
    :type array_dpos_center: Tuple[float, float, float]
    :param num_of_shots: The number of shots in the shot data.
    :type num_of_shots: int
    :param GPtransponders: List of GPTransponder objects for the survey.
    :type GPtransponders: List[GPTransponder]
    :return: The prepared GarposInput object.
    :rtype: GarposInput
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


def apply_survey_config(config: GarposSiteConfig, garpos_input: GarposInput) -> GarposInput:
    """
    Apply the site configuration to the GarposInput object.

    :param config: The site configuration.
    :type config: GarposSiteConfig
    :param garpos_input: The GarposInput object to be modified.
    :type garpos_input: GarposInput
    :return: The modified GarposInput object with the site configuration applied.
    :rtype: GarposInput
    """
    garpos_input.delta_center_position.east_sigma = config.inversion_params.delta_center_position.east_sigma
    garpos_input.delta_center_position.north_sigma = config.inversion_params.delta_center_position.north_sigma
    garpos_input.delta_center_position.up_sigma = config.inversion_params.delta_center_position.up_sigma


    for transponder in garpos_input.transponders:
        transponder.position_enu.east_sigma = config.transponder_position_variance.east_sigma
        transponder.position_enu.north_sigma = config.transponder_position_variance.north_sigma
        transponder.position_enu.up_sigma = config.transponder_position_variance.up_sigma
    
    return garpos_input
    
