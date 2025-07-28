# External imports
from pathlib import Path
import json
from pandera.typing import DataFrame
import pandas as pd
import pymap3d as pm
# Local imports
from ..data_models.log_models import SV3InterrogationData, SV3ReplyData
from ..data_models.sv3_models import NovatelRangeEvent,NovatelInterrogationEvent
from ..data_models.observables import ShotDataFrame
from ..data_models.constants import LEAP_SECONDS,TRIGGER_DELAY_SV3
from ..logging import ProcessLogger as logger


def novatelInterrogation_to_garpos_interrogation(
        novatel_interrogation: NovatelInterrogationEvent
) -> SV3InterrogationData:

    """
    Converts a NovatelInterrogationEvent object to an SV3InterrogationData object.

    This function extracts GNSS and AHRS observation data from the input NovatelInterrogationEvent,
    transforms geodetic coordinates (latitude, longitude, height above ellipsoid) to ECEF coordinates,
    and populates an SV3InterrogationData instance with the relevant positional, orientation, and
    standard deviation values. The ping time is adjusted by subtracting LEAP_SECONDS.

    Args:
        novatel_interrogation (NovatelInterrogationEvent): The interrogation event containing GNSS and AHRS data.

    Returns:
        SV3InterrogationData: The converted interrogation data with ECEF coordinates and associated metadata.
    """
    east_ecef, north_ecef, up_ecef = pm.geodetic2ecef(
        float(novatel_interrogation.observations.GNSS.latitude), float(novatel_interrogation.observations.GNSS.longitude), float(novatel_interrogation.observations.GNSS.hae)
    )
    sv3Interrogation = SV3InterrogationData(
        head0=novatel_interrogation.observations.AHRS.h,
        pitch0=novatel_interrogation.observations.AHRS.p,
        roll0=novatel_interrogation.observations.AHRS.r,
        east0=east_ecef,
        north0=north_ecef,
        up0=up_ecef,
        east_std=novatel_interrogation.observations.GNSS.sdx,
        north_std=novatel_interrogation.observations.GNSS.sdy,
        up_std=novatel_interrogation.observations.GNSS.sdz,
        pingTime=novatel_interrogation.time.common,
    )
    return sv3Interrogation

def novatelReply_to_garpos_reply(
        novatel_reply: NovatelRangeEvent
) -> SV3ReplyData:

    """
    Converts a NovatelRangeEvent object to an SV3ReplyData object.
    This function extracts GNSS and AHRS observation data from the NovatelRangeEvent,
    converts geodetic coordinates (latitude, longitude, height above ellipsoid) to ECEF coordinates,
    calculates the travel time, and populates an SV3ReplyData object with the relevant fields.
    Args:
        novatel_reply (NovatelRangeEvent): The input event containing GNSS, AHRS, and range data.
    Returns:
        SV3ReplyData: The converted reply data containing transponder ID, orientation, position,
                      standard deviations, range, diagnostics, return time, and travel time.
    """
    east_ecef, north_ecef, up_ecef = pm.geodetic2ecef(
        float(novatel_reply.observations.GNSS.latitude), float(novatel_reply.observations.GNSS.longitude), float(novatel_reply.observations.GNSS.hae)
    )
    travelTime = float(novatel_reply.range.range) - float(novatel_reply.range.tat) - TRIGGER_DELAY_SV3
    sv3Reply = SV3ReplyData(
        transponderID=novatel_reply.range.cn,
        head1 = novatel_reply.observations.AHRS.h,
        pitch1 = novatel_reply.observations.AHRS.p,
        roll1 = novatel_reply.observations.AHRS.r,
        east1 = east_ecef,
        north1 = north_ecef,
        up1 = up_ecef,
        east_std=novatel_reply.observations.GNSS.sdx,
        north_std=novatel_reply.observations.GNSS.sdy,
        up_std=novatel_reply.observations.GNSS.sdz,
        range=novatel_reply.range.range,
        tat=novatel_reply.range.tat,
        snr=novatel_reply.range.diag.snr,
        dbv=novatel_reply.range.diag.dbv,
        xc=novatel_reply.range.diag.xc,
        returnTime=novatel_reply.time.common,
        tt=travelTime,
    )
    return sv3Reply

def merge_interrogation_reply(
    interrogation: SV3InterrogationData,
    reply: SV3ReplyData,
) -> dict | None:

    """
    Merges interrogation and reply data from SV3 devices into a single dictionary.
    Validates that the sum of pingTime, tt (two-way travel time), tat (turn-around time),
    and any trigger delay matches the reported returnTime in the reply data. If the
    calculated return time does not match the reply's returnTime within a small tolerance,
    an AssertionError is raised.
    Args:
        interrogation (SV3InterrogationData): The interrogation data object containing pingTime.
        reply (SV3ReplyData): The reply data object containing tt, tat, and returnTime.
    Returns:
        dict | None: A merged dictionary containing all fields from both interrogation and reply data.
                     Returns None if either input is None.
    Raises:
        AssertionError: If the calculated return time does not match the reply's returnTime.
    """
    # Validate that pingTime + tt + tat + TRIGGER_DELAY_SV3 equals returnTime
    # calculate original range
    range_original = float(reply.tt) + float(reply.tat)
    calc_return_time = (
        float(interrogation.pingTime)
        + range_original
    )
    assert abs(calc_return_time - float(reply.returnTime)) < 1e-6, (
        f"Calculated return time {calc_return_time} does not match reply return time {reply.returnTime}"
    )
    # Create a merged dictionary
    merged_data = dict(interrogation) | dict(reply)
    return merged_data


def dfop00_to_shotdata(source: str | Path) -> DataFrame[ShotDataFrame] | None:
    """
    Parses a DFOP00-format file containing Sonardyne event data and converts it into a ShotDataFrame.

    The function reads the specified file line by line, expecting each line to be a JSON object
    representing either an "interrogation" or "range" event. It processes and merges interrogation
    and range events, transforming them into a unified format suitable for geodetic analysis.

    Args:
        source (str | Path): Path to the DFOP00-format file containing event data.

    Returns:
        ShotDataFrame | None: A ShotDataFrame containing processed and merged event data,
        or None if no valid data was found or an error occurred during file reading.

    Raises:
        None explicitly, but logs errors for file access issues and data processing problems.
    """


    processed = []
    interrogation = None
    try:
        with open(source, encoding="utf-8") as f:
            lines = f.readlines()
    except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
        logger.logerr(f"Error reading {source}: {e}")
        return None
    for line in lines:
        data = json.loads(line)
        if data.get("event") == "interrogation":
            try:
                interrogation = NovatelInterrogationEvent(**data)
                interrogation_parsed = novatelInterrogation_to_garpos_interrogation(interrogation)
            except Exception as e:
                interrogation = None

        if data.get("event") == "range":
            try:
                reply_data = NovatelRangeEvent(**data)
                reply_data_parsed = novatelReply_to_garpos_reply(reply_data)

            except Exception as e:
                reply_data = None
            if reply_data is not None:
                try:
                    merged_data = merge_interrogation_reply(interrogation_parsed, reply_data_parsed)
                except AssertionError as e:
                    logger.logerr(f"Assertion error in merging data: {e}")
                    merged_data = None
                if merged_data is not None:
                    processed.append(merged_data)

    if not processed:
        logger.logerr(f"No valid data found in {source}")
        return None
    df = pd.DataFrame(processed)
    df["isUpdated"] = False
    return ShotDataFrame(df)

