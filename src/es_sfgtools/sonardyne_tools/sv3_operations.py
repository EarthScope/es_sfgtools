# External imports
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymap3d as pm
from pandera.typing import DataFrame

from ..data_models.community_standards import SFGDSTFSeafloorAcousticData, SFGDTSFSite
from ..data_models.constants import LEAP_SECONDS, TRIGGER_DELAY_SV3

# Local imports
from ..data_models.log_models import SV3InterrogationData, SV3ReplyData
from ..data_models.observables import ShotDataFrame
from ..data_models.sv3_models import NovatelInterrogationEvent, NovatelRangeEvent
from ..logging import ProcessLogger as logger


def novatelInterrogation_to_garpos_interrogation(
        novatel_interrogation: NovatelInterrogationEvent
) -> SV3InterrogationData:
    """Converts a NovatelInterrogationEvent to an SV3InterrogationData object.

    This function extracts GNSS and AHRS observation data from the input
    NovatelInterrogationEvent, transforms geodetic coordinates (latitude,
    longitude, height above ellipsoid) to ECEF coordinates, and populates an
    SV3InterrogationData instance with the relevant positional, orientation,
    and standard deviation values. The ping time is adjusted by subtracting
    LEAP_SECONDS.

    Parameters
    ----------
    novatel_interrogation : NovatelInterrogationEvent
        The interrogation event containing GNSS and AHRS data.

    Returns
    -------
    SV3InterrogationData
        The converted interrogation data with ECEF coordinates and associated
        metadata.
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
        east_std0=novatel_interrogation.observations.GNSS.sdx,
        north_std0=novatel_interrogation.observations.GNSS.sdy,
        up_std0=novatel_interrogation.observations.GNSS.sdz,
        pingTime=float(novatel_interrogation.time.common) + LEAP_SECONDS, #GPS time is ahead of UTC by 18 seconds
    )
    return sv3Interrogation

def novatelReply_to_garpos_reply(
        novatel_reply: NovatelRangeEvent
) -> SV3ReplyData:
    """Converts a NovatelRangeEvent object to an SV3ReplyData object.

    This function extracts GNSS and AHRS observation data from the
    NovatelRangeEvent, converts geodetic coordinates (latitude, longitude,
    height above ellipsoid) to ECEF coordinates, calculates the travel time,
    and populates an SV3ReplyData object with the relevant fields.

    Parameters
    ----------
    novatel_reply : NovatelRangeEvent
        The input event containing GNSS, AHRS, and range data.

    Returns
    -------
    SV3ReplyData
        The converted reply data containing transponder ID, orientation,
        position, standard deviations, range, diagnostics, return time,
        and travel time.
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
        east_std1=novatel_reply.observations.GNSS.sdx,
        north_std1=novatel_reply.observations.GNSS.sdy,
        up_std1=novatel_reply.observations.GNSS.sdz,
        range=novatel_reply.range.range,
        tat=novatel_reply.range.tat,
        snr=novatel_reply.range.diag.snr,
        dbv=novatel_reply.range.diag.dbv,
        xc=novatel_reply.range.diag.xc,
        returnTime=float(novatel_reply.time.common) + LEAP_SECONDS, #GPS time is ahead of UTC by 18 seconds
        tt=travelTime,
    )
    return sv3Reply

def merge_interrogation_reply(
    interrogation: SV3InterrogationData,
    reply: SV3ReplyData,
) -> dict | None:
    """Merges interrogation and reply data from SV3 devices.

    This is merged into a single dictionary. Validates that the sum of
    pingTime, tt (two-way travel time), tat (turn-around time), and any
    trigger delay matches the reported returnTime in the reply data. If the
    calculated return time does not match the reply's returnTime within a
    small tolerance, an AssertionError is raised.

    Parameters
    ----------
    interrogation : SV3InterrogationData
        The interrogation data object containing pingTime.
    reply : SV3ReplyData
        The reply data object containing tt, tat, and returnTime.

    Returns
    -------
    dict | None
        A merged dictionary containing all fields from both interrogation and
        reply data. Returns None if either input is None.

    Raises
    ------
    AssertionError
        If the calculated return time does not match the reply's returnTime.
    """
    #validate that tt > 0 (we actually got a range value in the reply)
    range = float(reply.tt) + float(reply.tat) + TRIGGER_DELAY_SV3
    assert (abs(range) > 1e-3), (f"Transponder {reply.transponderID} has range={abs(round(range,1))} for ping at {interrogation.pingTime} {datetime.fromtimestamp(float(interrogation.pingTime))}")
    
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
    """Parses a DFOP00-format file and converts it into a ShotDataFrame.

    The function reads the specified file line by line, expecting each line
    to be a JSON object representing either an "interrogation" or "range"
    event. It processes and merges interrogation and range events,
    transforming them into a unified format suitable for geodetic analysis.

    Parameters
    ----------
    source : str | Path
        Path to the DFOP00-format file containing event data.

    Returns
    -------
    ShotDataFrame | None
        A ShotDataFrame containing processed and merged event data, or None
        if no valid data was found or an error occurred during file reading.
    """


    processed = []
    interrogation = None
    try:
        with open(source, encoding="utf-8") as f:
            lines = f.readlines()
    except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
        logger.logerr(f"Error reading {source}: {e}")
        return None
    
    interrogation_parsed = None
    reply_data_parsed = None
    
    for line in lines:
        data = json.loads(line)
        if data.get("event") == "interrogation":
            try:
                interrogation = NovatelInterrogationEvent(**data)
                interrogation_parsed = novatelInterrogation_to_garpos_interrogation(interrogation)
                #logger.loginfo(f"Interrogation: pingTime: {interrogation_parsed.pingTime}")
            except Exception:
                interrogation_parsed = None

        if data.get("event") == "range":
            try:
                reply_data = NovatelRangeEvent(**data)
                reply_data_parsed = novatelReply_to_garpos_reply(reply_data)
                #logger.loginfo(f"Reply: \n  returnTime: {reply_data_parsed.returnTime}\n  tt: {reply_data_parsed.tt}")

            except Exception:
                reply_data_parsed = None

            if reply_data_parsed is not None and interrogation_parsed is not None:
                try:
                    merged_data = merge_interrogation_reply(interrogation_parsed, reply_data_parsed)
                    interrogation_parsed = None  # Reset interrogation after merging
                    reply_data_parsed = None  # Reset reply after merging
                except AssertionError as e:
                    logger.logerr(f"Assertion error in merging ping/reply data: {e}")
                    merged_data = None

                interrogation_parsed = None  # Reset interrogation after merging attempt  
                reply_data_parsed = None  # Reset reply after merging attempt  
                
                if merged_data is not None:
                    processed.append(merged_data)

    if not processed:
        logger.logerr(f"No valid data found in {source}")
        return None
    df = pd.DataFrame(processed)
    df["isUpdated"] = False
    return ShotDataFrame.validate(df,lazy=True)


def dfop00_to_cstd_shotdata(source: str | Path,atd_offset:list) -> SFGDSTFSeafloorAcousticData | None:
    """Parses a DFOP00-format file and converts it to SFGDSTFSeafloorAcousticData.

    The function reads the specified file line by line, expecting each line
    to be a JSON object representing either an "interrogation" or "range"
    event. It processes and merges interrogation and range events,
    transforming them into a unified format suitable for geodetic analysis.

    Parameters
    ----------
    source : str | Path
        Path to the DFOP00-format file containing event data.
    atd_offset : list
        Antenna to transponder offset [m] with [forward,rightward,downward].

    Returns
    -------
    SFGDSTFSeafloorAcousticData | None
        A SFGDSTFSeafloorAcousticData containing processed and merged event
        data, or None if no valid data was found or an error occurred during
        file reading.
    """

    shotdata = dfop00_to_shotdata(source)
    if shotdata is None:
        logger.logerr(f"Failed to convert {source} to ShotDataFrame")
        return None

    # Convert ShotDataFrame to SFGDSTFSeafloorAcousticData
    X_transmit = shotdata.east0.apply(lambda x: x+atd_offset[0])
    Y_transmit = shotdata.north0.apply(lambda x: x+atd_offset[1])
    Z_transmit = shotdata.up0.apply(lambda x: x+atd_offset[2])

    X_receive = shotdata.east1.apply(lambda x: x+atd_offset[0])
    Y_receive = shotdata.north1.apply(lambda x: x+atd_offset[1])
    Z_receive = shotdata.up1.apply(lambda x: x+atd_offset[2])

    df = pd.DataFrame(
        {
            "MT_ID": shotdata.transponderID,
            "TravelTime": shotdata.tt,
            "T_transmit": shotdata.pingTime,
            "X_transmit": X_transmit,
            "Y_transmit": Y_transmit,
            "Z_transmit": Z_transmit,
            "T_receive": shotdata.returnTime,
            "X_receive": X_receive,
            "Y_receive": Y_receive,
            "Z_receive": Z_receive,
            "roll0": shotdata.roll0,
            "pitch0": shotdata.pitch0,
            "heading0": shotdata.head0,
            "roll1": shotdata.roll1,
            "pitch1": shotdata.pitch1,
            "heading1": shotdata.head1,
            "ant_X0": shotdata.east0,
            "ant_Y0": shotdata.north0,
            "ant_Z0": shotdata.up0,
            "ant_X1": shotdata.east1,
            "ant_Y1": shotdata.north1,
            "ant_Z1": shotdata.up1,
            "aSNR":shotdata.snr,
            "dBV":shotdata.dbv,
            "acc":shotdata.xc,
            "ant_sigX0": shotdata.east_std0,
            "ant_sigY0": shotdata.north_std0,
            "ant_sigZ0": shotdata.up_std0,
            "ant_sigX1": shotdata.east_std1,
            "ant_sigY1": shotdata.north_std1,
            "ant_sigZ1": shotdata.up_std1,

        }
    )

    return SFGDSTFSeafloorAcousticData(df)