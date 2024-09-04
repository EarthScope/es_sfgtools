from ..schemas.files.file_schemas import LeverArmFile
from ..schemas.site_config.site_schemas import ATDOffset

def leverarmfile_to_atdoffset(source: LeverArmFile) -> ATDOffset:
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
    return ATDOffset(forward=forward, rightward=rightward, downward=downward)
