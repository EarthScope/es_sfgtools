"""Example script for converting NovAtel 000/770 binaries to RINEX.

This demonstrates how to construct a minimal MetadataModel and call the
high-level novatel_2rinex helper on a mix of NOV000.bin and NOV770.raw
files contained in a single directory.
"""

from pathlib import Path
from es_sfgtools.novatel_tools import (
    MetadataModel,
    novatel_2rinex,
)

'''
Step 1: Define metadata. 

The MetadataModel is a schema that is used to provide information about the RINEX header.
It is recommended to instantiate a MetadataModel object to ensure all required fields are provided. 
See the MetadataModel documentation for details on required and optional fields.

'''
metadata = MetadataModel(
    marker_name="NCC1",
    run_by="Franklyn Dunbar",
)

'''
Step 2: Define input files.

'''
novatel_path_dir = Path(
    "/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain/cascadia-gorda/NCC1/2025_A_1126/raw"
)
nov_bin = list(novatel_path_dir.glob("*NOV*.bin"))
nov_raw = list(novatel_path_dir.glob("*NOV*.raw"))
all_files = nov_bin + nov_raw

'''
Step 3: Define output directory that you want to write RINEX files to.
Note that if this is not provided, RINEX files will be written to the same directory
as the input NovAtel files.
'''
write_dir = Path(
    "/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain/cascadia-gorda/NCC1/2025_A_1126/processed"
)


rinex_paths = novatel_2rinex(
    files=all_files,
    writedir=write_dir,
    metadata=metadata,
)

print(f"{'='*40}\nGenerated {len(rinex_paths)} RINEX files from {len(all_files)} NovAtel files:")
for path in rinex_paths:
    print(f" - {path}")
