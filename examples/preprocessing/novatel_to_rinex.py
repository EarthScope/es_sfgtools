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
NETWORK = "cascadia-gorda"
STATION = "GCC1"
CAMPAIGN = "2023_A_1063"
PROJECT_DIRECTORY = Path("/path/to/project/directory")
metadata = MetadataModel(
    marker_name=STATION,
    run_by="Franklyn Dunbar",
)

'''
Step 2: Define input files.

'''
novatel_path_dir = PROJECT_DIRECTORY/NETWORK/STATION/CAMPAIGN/"raw"

nov_bin = list(novatel_path_dir.glob("*NOV*.bin"))
nov_raw = list(novatel_path_dir.glob("*NOV*.raw"))
all_files = nov_bin + nov_raw


'''
Step 3: Define output directory that you want to write RINEX files to.
Note that if this is not provided, RINEX files will be written to the same directory
as the input NovAtel files.
'''
write_dir = PROJECT_DIRECTORY/NETWORK/STATION/CAMPAIGN/"processed"

'''
Step 4 (Optional): Define decimation and parallelism.

The modulo_millis parameter controls epoch decimation. When set to a positive value,
only epochs where (epoch_time_ms % modulo_millis == 0) are kept. Loss-of-Lock 
Indicators (LLI) from skipped epochs are propagated to the next written epoch.

Examples:
  - modulo_millis=1000   -> 1 Hz output (keep epochs at 1-second intervals)
  - modulo_millis=15000  -> 15-second intervals
  - modulo_millis=0      -> no decimation (default)

The num_routines parameter controls parallel processing in the Go binary.
Higher values can speed up processing but use more memory.
'''
MODULO_MILLIS = 1000  # Set to 0 to disable decimation
NUM_ROUTINES = 1      # Number of concurrent goroutines (default: 1)


rinex_paths = novatel_2rinex(
    files=all_files,
    writedir=write_dir,
    metadata=metadata,
    modulo_millis=MODULO_MILLIS,
    num_routines=NUM_ROUTINES,
)

print(f"{'='*40}\nGenerated {len(rinex_paths)} RINEX files from {len(all_files)} NovAtel files:")
for path in rinex_paths:
    print(f" - {path}")
