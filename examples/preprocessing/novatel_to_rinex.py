from pathlib import Path
from es_sfgtools.novatel_tools import novatel_000_2rinex,MetadataModel,novatel_770_2rinex


metadata = MetadataModel(
    marker_name="NCC1",
    run_by="Franklyn Dunbar",
)
write_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain/cascadia-gorda/NCC1/2025_A_1126/processed")
novatel_path_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain/cascadia-gorda/NCC1/2025_A_1126/raw")

files = ["341517_001_20250913_041510_00013_NOV770.raw"]

for file in files:
    rinex_path = novatel_000_2rinex(
        file=novatel_path_dir / file,
        writedir=write_dir,
        metadata=metadata
    )
    print(f"RINEX file created at: {rinex_path}")
