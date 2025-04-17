import os
from es_sfgtools.modeling.garpos_tools.load_utils import load_drive_garpos,load_lib
from pathlib import Path

os.environ["GARPOS_PATH"] = str(Path("/Users/franklyndunbar/Project/garpos").resolve())
try:
    drive_garpos = load_drive_garpos()
    print(drive_garpos)
except Exception as e:

    from garpos import drive_garpos

F90_LIB,LIB_RT = load_lib()
print(F90_LIB)