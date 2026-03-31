"""Resolve GNSS products for RINEX files and run PRIDE-PPPAR processing."""

import logging
from pathlib import Path
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)-8s %(name)s — %(message)s",
)

from pride_ppp import PrideProcessor, ProcessingMode

pride_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain/Pride")
output_dir = Path(
    "/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain/cascadia-gorda/NCC1/2025_A_1126/intermediate"
)

if __name__ == "__main__":
    processor = PrideProcessor(
        pride_dir=pride_dir,
        output_dir=output_dir,
        mode=ProcessingMode.FINAL,
    )

    rinex_files = list(output_dir.glob("*.25o"))
    start_time = time.time()
    results = list(processor.process_batch(rinex_files, max_workers=10, override=True))

    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_hours = elapsed_time / 3600
    elapsed_minutes = (elapsed_time % 3600) / 60

    print(
        f"Processed {len(rinex_files)} files in {elapsed_hours:.2f} hours ({elapsed_minutes:.2f} minutes)."
    )
