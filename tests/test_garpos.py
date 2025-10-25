from pathlib import Path

import pandas as pd
from garpos import LIB_DIRECTORY, LIB_RAYTRACE

from es_sfgtools.modeling.garpos_tools import GarposFixed, GarposInput, GarposResults
from es_sfgtools.modeling.garpos_tools.functions import (
    datafile_to_garposinput,
    garposfixed_from_datafile,
    main,
)
from es_sfgtools.modeling.garpos_tools.schemas import ObservationData

TEST_DATA_DIR = Path(__file__).parent / "resources"/"garpos_run"

TEST_GARPOS_SHOTDATA = TEST_DATA_DIR / "SAGA.1903.kaiyo_k4-obs.csv"
TEST_GARPOS_FIXED = TEST_DATA_DIR / "Settings-fix.ini"
TEST_GARPOS_SVP = TEST_DATA_DIR / "SAGA.1903.kaiyo_k4-svp.csv"
TEST_GARPOS_CONFIG = TEST_DATA_DIR / "SAGA.1903.kaiyo_k4-initcfg.ini"


class TestGarpos:

    def test_garpos_input(self):
        garpos_input = datafile_to_garposinput(TEST_GARPOS_CONFIG)
        assert isinstance(garpos_input, GarposInput), "GarposInput not created"

    def test_garpos_fixed(self):
        garpos_fixed = garposfixed_from_datafile(TEST_GARPOS_FIXED)
        assert isinstance(garpos_fixed, GarposFixed), "GarposFixed object not created"

    def test_garpos_run(self):
        garpos_input = datafile_to_garposinput(TEST_GARPOS_CONFIG)
        garpos_fixed = garposfixed_from_datafile(TEST_GARPOS_FIXED)
        garpos_fixed.lib_directory = str(LIB_DIRECTORY)
        garpos_fixed.lib_raytrace = str(LIB_RAYTRACE)
        garpos_input.observation.shot_data = ObservationData.validate(pd.read_csv(TEST_GARPOS_SHOTDATA))
        garpos_input.observation.sound_speed_data = pd.read_csv(TEST_GARPOS_SVP)
        garpos_results = main(input=garpos_input, fixed=garpos_fixed)

        assert isinstance(garpos_results, GarposResults), "GarposResults object not created"

if __name__ == "__main__":
    test = TestGarpos()

    test.test_garpos_run()
