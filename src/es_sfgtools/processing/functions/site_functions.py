import logging
import re
import os
from typing import Union
from ..schemas.site_config.site_schemas import SiteConfig,Transponder,ATDOffset,PositionLLH
from ..schemas.files.file_schemas import MasterFile,LeverArmFile

logger = logging.getLogger(__name__)

MASTER_STATION_ID = {"0": "5209", "1": "5210", "2": "5211", "3": "5212"}


def masterfile_to_siteconfig(source:MasterFile) -> Union[SiteConfig,None]:
    """
    Convert a MasterFile to a SiteConfig
    """
    if not os.path.exists(source.location):
        raise FileNotFoundError(f"File {source.location} not found")

    loginfo = f"Populating List[Transponder] and Site data from {source.location}"
    logger.info(loginfo)
    transponders = []

    lat_lon_line = re.compile(r"Latitude/Longitude array center")
    non_alphabet = re.compile("[a-c,e-z,A-Z]")
    geoid_undulation_pat = re.compile(r"Geoid undulation at sea surface point")

    with open(source.location,'r') as f:
        lines = f.readlines()[2:]

        for line in lines:
            if not non_alphabet.search(line):
                line_processed = line.strip().split()
                # 0    16       54.316094952    -158.461771055    -2096.780      0.200000d0       1479.900
                id = line_processed[0]
                lat = float(line_processed[2])
                lon = float(line_processed[3])
                height = float(line_processed[4])
                id = MASTER_STATION_ID[id]
                # TODO This is not the var
                offset = float(line_processed[5].replace("d0",""))
                transponder_position = PositionLLH(latitude=lat,longitude=lon,height=height)
                transponder = Transponder(id=id,position_llh=transponder_position,tat_offset=offset)
                transponders.append(transponder)
                
            if geoid_undulation_pat.search(line):
                # "+10.300d0           ! Geoid undulation at sea surface point"
                geoid_undulation = float(line.split()[0].replace("d0","")) # TODO verify sign
            

            if lat_lon_line.search(line):
                # 54.3324d0 -158.4692d0   ! Latitude/Longitude array center (decimal degrees)
                line_processed = [
                    float(x.replace("d0", "")) for x in line.split("!")[0].strip().split()
                ]
                lat, lon = line_processed[0], line_processed[1]
                center_llh = {"latitude":lat,"longitude":lon,"height":geoid_undulation}
                break


    if not center_llh:
        logger.error("Latitude/Longitude array center not found in masterfile")
        return
    if not transponders:
        logger.error("No transponders found in masterfile")
        return
    if geoid_undulation is None:
        logger.error("Geoid undulation not found in masterfile")
        return

    # subtract geoid undulation from transponder height
    for transponder in transponders:
        transponder.position_llh.height += geoid_undulation # TODO John things this might work

    site_position_llh = PositionLLH(latitude=center_llh["latitude"],longitude=center_llh["longitude"],height=center_llh["height"])
    site = SiteConfig(
        position_llh=site_position_llh,
        transponders=transponders)

    return site

def leverarmfile_to_atdoffset(source: LeverArmFile) -> ATDOffset:
    """
    Read the ATD offset from a "lever_arms" file
    format is [rightward,forward,downward] [m]

    0.0 +0.575 -0.844

    """
    with open(source.location, "r") as f:
        line = f.readlines()[0]
        values = [float(x) for x in line.split()]
        forward = values[1]
        rightward = values[0]
        downward = values[2]
    return ATDOffset(forward=forward, rightward=rightward, downward=downward)