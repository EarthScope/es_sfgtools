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


metadata = MetadataModel(
    marker_name="NCC1",
    run_by="Franklyn Dunbar",
)

write_dir = Path(
    "/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain/cascadia-gorda/NCC1/2025_A_1126/processed"
)
novatel_path_dir = Path(
    "/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain/cascadia-gorda/NCC1/2025_A_1126/raw"
)

# Example: convert individual NOV000 files
# for file in novatel_path_dir.glob("*NOV000.bin"):
#     rinex_paths = novatel_000_2rinex(
#         file=file,
#         writedir=write_dir,
#         metadata=metadata,
#     )
#     print(f"RINEX files created from {file}: {rinex_paths}")

# Example: convert individual NOV770 files
# for file in novatel_path_dir.glob("*NOV770.raw"):
#     rinex_paths = novatel_770_2rinex(
#         file=file,
#         writedir=write_dir,
#         metadata=metadata,
#     )
#     print(f"RINEX files created from {file}: {rinex_paths}")

# Recommended: use the high-level helper to convert all NOV000/NOV770 files
all_files = list(novatel_path_dir.glob("*NOV*.bin")) + list(
    novatel_path_dir.glob("*NOV*.raw")
)
import time
start_time = time.time()
rinex_paths = novatel_2rinex(
    files=all_files,
    writedir=write_dir,
    metadata=metadata,
)
end_time = time.time()
print(f"Generated {len(rinex_paths)} RINEX files in {end_time - start_time:.2f} seconds:")
for path in rinex_paths:
    print(f" - {path}")