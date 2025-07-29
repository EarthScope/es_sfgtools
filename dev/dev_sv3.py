from pathlib import Path
import logging

from es_sfgtools.sonardyne_tools.sv3_operations import dev_dfop00_to_shotdata


test_dfo = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/2022_A_1065/raw/329653_002_20220505_022443_00086_DFOP00.raw"
)

df = dev_dfop00_to_shotdata(test_dfo)