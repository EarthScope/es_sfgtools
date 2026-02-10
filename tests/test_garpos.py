from pathlib import Path
import os
import pandas as pd
import pytest
from es_sfgtools.modeling.garpos_tools.load_utils import load_lib
load_lib()
from garpos import LIB_DIRECTORY, LIB_RAYTRACE,drive_garpos
import subprocess

TEST_DATA_DIR = Path(__file__).parent / "resources"/"garpos_run"

TEST_GARPOS_SHOTDATA = TEST_DATA_DIR / "SAGA.1903.kaiyo_k4-obs.csv"
TEST_GARPOS_FIXED = TEST_DATA_DIR / "Settings-fix.ini"
TEST_GARPOS_SVP = TEST_DATA_DIR / "SAGA.1903.kaiyo_k4-svp.csv"
TEST_GARPOS_CONFIG = TEST_DATA_DIR / "SAGA.1903.kaiyo_k4-initcfg.ini"


class TestGarposInstallation:
    """Tests to verify garpos is properly installed and functional."""

    def test_garpos_import(self):
        """Verify garpos can be imported."""
        import garpos
        assert garpos is not None, "garpos module failed to import"

    def test_garpos_lib_directory_exists(self):
        """Verify the garpos library directory exists."""
        assert Path(LIB_DIRECTORY).exists(), f"LIB_DIRECTORY not found: {LIB_DIRECTORY}"

    def test_garpos_lib_raytrace_exists(self):
        """Verify the garpos raytrace library exists."""
        assert (Path(LIB_DIRECTORY) / LIB_RAYTRACE).exists(), f"LIB_RAYTRACE not found: {LIB_RAYTRACE}"

    def test_garpos_raytrace_is_loadable(self):
        """Verify the raytrace shared library can be loaded."""
        import ctypes
        try:
            lib = ctypes.CDLL(str(Path(LIB_DIRECTORY) / LIB_RAYTRACE))
            assert lib is not None, "Failed to load raytrace library"
        except OSError as e:
            pytest.fail(f"Failed to load raytrace library: {e}")

    def test_drive_garpos(self):
        """Verify the drive_garpos function can be called."""
        try:
            drive_garpos()
        except Exception as e:
            assert isinstance(e,TypeError), f"drive_garpos raised an unexpected exception: {e}"

    def test_garpos_run(self, capsys):
        # navigate to the garpos directory to run the test
        current_dir = Path.cwd()
        garpos_dir = Path(os.environ["GARPOS_PATH"])
        # Get the absolute path to the demo.sh script
        garpos_dir_true = garpos_dir.resolve()
        demo_sh = garpos_dir_true / "sample" / "demo.sh"
        if not demo_sh.exists():
            raise FileNotFoundError(f"demo.sh not found in GARPOS_PATH: {demo_sh}")
        try:            
            result = subprocess.run(["bash", str(demo_sh)], cwd=demo_sh.parent, check=True, capture_output=True, text=True)
            # Use capsys to show output even when pytest captures stdout
            with capsys.disabled():
                print("\n=== GARPOS demo.sh stdout ===")
                print(result.stdout)
                if result.stderr:
                    print("=== GARPOS demo.sh stderr ===")
                    print(result.stderr)
        except subprocess.CalledProcessError as e:
            pytest.fail(f"Failed to run demo.sh: {e}")

# class TestGarpos:

#     def test_garpos_input(self):
#         garpos_input = GarposInput.from_datafile(TEST_GARPOS_CONFIG)
#         assert isinstance(garpos_input, GarposInput), "GarposInput not created"

#     def test_garpos_fixed(self):
#         garpos_fixed = GarposFixed.from_datafile(TEST_GARPOS_FIXED)
#         assert isinstance(garpos_fixed, GarposFixed), "GarposFixed object not created"

#     def test_garpos_run(self):
#         garpos_input = GarposInput.from_datafile(TEST_GARPOS_CONFIG)
#         garpos_fixed = GarposFixed.from_datafile(TEST_GARPOS_FIXED)
#         garpos_fixed.lib_directory = str(LIB_DIRECTORY)
#         garpos_fixed.lib_raytrace = str(LIB_RAYTRACE)
#         garpos_input.observation.shot_data = ObservationData.validate(pd.read_csv(TEST_GARPOS_SHOTDATA))

if __name__ == "__main__":
    test_installation = TestGarposInstallation()
    test_installation.test_garpos_import()
    test_installation.test_garpos_lib_directory_exists()    
