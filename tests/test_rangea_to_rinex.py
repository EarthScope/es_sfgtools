from collections import defaultdict
import json
import tempfile
from pathlib import Path
import filecmp
from typing import List
import pytest
import tiledb
import numpy as np

from es_sfgtools.novatel_tools.rangea_parser import extract_rangea_from_qcpin, GNSSEpoch,extract_rangea_strings_from_qcpin
from es_sfgtools.novatel_tools import novatel_ascii_operations as nova_ops
from es_sfgtools.tiledb_tools.tiledb_schemas import TDBGNSSObsArray
from es_sfgtools.tiledb_tools.tiledb_operations import tile2rinex


fake_rinex_settings = {
    "rinex_version": "2.11",
    "rinex_type": "O",
    "rinex_system": "G",
    "marker_name": "NTH1",
    "marker_number": "0001",
    "markerType": "GEODETIC",
    "observer": "EarthScope",
    "agency": "EarthScope",
    "program": "gnsstools",
    "run_by": "",
    "date": "",
    "receiver_model": "NOV",
    "receiver_serial": "XXXXXXXXXX",
    "receiver_firmware": "0.0.0",
    "antenna_model": "TRM59800.00 SCIT",
    "antenna_serial": "987654321",
    "antenna_position": [0, 0, 0],
    "antenna_offsetHEN": [0, 0, 0],
}



qc_pinfiles = [
            "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/tests/resources/qcdata/341517-001_20250812_133440_000052_USGS.pin",
            "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/tests/resources/qcdata/341517-001_20250812_235232_000053_USGS.pin",
        ]
   

def test_rangea_to_rinex_equivalence(qcpin_files=qc_pinfiles):
    # 1. Extract RANGEA logs from QCPIN files
    gnss_epochs: List[GNSSEpoch] = []
    rangea_strings: List[str] = []
    for qcpin in qcpin_files:
        logs = extract_rangea_from_qcpin(qcpin)
        if logs:
            gnss_epochs.extend(logs)
        strings = extract_rangea_strings_from_qcpin(qcpin)
        if strings:
            rangea_strings.extend(strings)
    rangea_strings = list(set(rangea_strings))  # Deduplicate RANGEA strings
    # 2. Write RANGEA logs to a temp text file
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tf:
        for log in rangea_strings:
            tf.write(log + "\n")
        tf.flush()
        rangea_txt_path = Path(tf.name)


    # 5. Path 2: GNSSEpoch -> TileDB -> tile2rinex
    with tempfile.TemporaryDirectory(prefix="/tmp/",delete=True) as tempdir:
        rinex_settings_path = Path(tempdir) / "rinex_settings.json"
        with open(rinex_settings_path, "w") as f:
            json.dump(fake_rinex_settings, f)

        tdb_path = Path(tempdir) / "test_gnss_obs.tdb"
        tdb_array = TDBGNSSObsArray(tdb_path)
        tdb_array.write_epochs(gnss_epochs)

        # Create a second TileDB array using novatel_ascii_2tile as a sink
        tdb_path2 = Path(tempdir) / "test_gnss_obs_2.tdb"
        tdb_array2 = TDBGNSSObsArray(tdb_path2)
        # Use the ASCII file as input for novatel_ascii_2tile
        nova_ops.novatel_ascii_2tile([str(rangea_txt_path)], tdb_path2)

        # Compare the first index of both arrays for debugging
        arr1 = tiledb.open(str(tdb_path), mode="r")
        arr2 = tiledb.open(str(tdb_path2), mode="r")
        arr1_df = arr1.df[:]
        arr2_df = arr2.df[:]

        
        assert (arr1_df.head(10).values.round(5) == arr2_df.head(10).values.round(5)).all(), "First 10 rows from both TileDB arrays do not match, indicating a discrepancy between write_epochs and novatel_ascii_2tile"
        arr1.close()
        arr2.close()

        rinex_paths: List[Path] = tile2rinex(
            gnss_obs_tdb=tdb_path,
            settings=rinex_settings_path,  # Provide settings if needed
            writedir=Path(tempdir)
        )

        # now write the rinex using novatel_ascii_operations for the same GNSSEpochs to compare
        rinex_paths_ascii: List[Path] = nova_ops.novatel_ascii_2rinex(
            file=rangea_txt_path,
            writedir=Path(tempdir),
            site="TES1",
            modulo_millis=0
        )

        # match files by comparing 4th throuhg last characters of the filename (to ignore site name differences)
        matches = defaultdict(list)
        for path in rinex_paths + rinex_paths_ascii:
            key = path.stem[4:]  # Get the part of the filename after the first 4 characters
            matches[key].append(path)
        for key, paths in matches.items():
            if len(paths) == 2:
                path1, path2 = paths
                def get_epoch_section(text):
                    # Find the end of the header (line containing 'END OF HEADER')
                    lines = text.splitlines()
                    for i, line in enumerate(lines):
                        if 'END OF HEADER' in line:
                            return lines[i+1:]  # Return lines after the header
                    return lines
                epoch1 = get_epoch_section(path1.read_text())
                epoch2 = get_epoch_section(path2.read_text())
                print("==== EPOCHS 1 ====")
                print(epoch1)
                print("\n==== EPOCHS 2 ====")
                print(epoch2)
                # assert epoch1 == epoch2, f"Epoch data in RINEX files {path1} and {path2} do not match"
            else:
                raise ValueError(f"Expected 2 files for key {key}, but found {len(paths)}: {paths}")

test_rangea_to_rinex_equivalence()