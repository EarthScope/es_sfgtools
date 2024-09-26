import logging
from ..assets.file_schemas import LeverArmFile
from ..assets.siteconfig import ATDOffset

logger = logging.getLogger(__name__)

def leverarmfile_to_atdoffset(source: LeverArmFile, show_details: bool=True) -> ATDOffset:
    """
    Read the ATD offset from a "lever_arms" file
    format is [rightward,forward,downward] [m]


    0.0 +0.575 -0.844

    """
    with open(source.local_path, "r") as f:
        line = f.readlines()[0]
        values = [float(x) for x in line.split()]
        forward = values[1]
        rightward = values[0]
        downward = values[2]
    response = f"ATD offset (forward, rightward, downward): {forward}, {rightward}, {downward}"
    logger.info(response)
    if show_details:
        print(response)
    return ATDOffset(forward=forward, rightward=rightward, downward=downward)
