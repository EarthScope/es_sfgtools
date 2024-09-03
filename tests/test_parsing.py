import os
import pandas as pd
import pandera as pa
from pandera.errors import SchemaError
from datetime import datetime, timezone
from pathlib import Path
import sys
import pdb
# Local Imports

from es_sfgtools.processing.schemas import files as file_schemas
from es_sfgtools.processing.schemas import observables as obs_schemas
from es_sfgtools.processing.schemas import site_config as site_schemas
from es_sfgtools.processing.functions import *

# Set test resource paths
RESOURCES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources")
TEST_NOVATEL_RAW = os.path.join(RESOURCES, "garpos_etl/test_novatel_raw.txt")
TEST_NOVATEL_PROCESSED = os.path.join(RESOURCES, "garpos_etl/inspva_from_novatel.csv")
TEST_SONARDYNE_RAW = os.path.join(RESOURCES, "garpos_etl/test_sonardyne_raw.txt")
TEST_SONARDYNE_PROCESSED = os.path.join(RESOURCES, "garpos_etl/acoustic_from_sondardyne.csv")
TEST_GNSS_PPP_POSITION= os.path.join(RESOURCES, "garpos_etl/test_position_raw.txt")

TEST_NOVATEL_BINARY = os.path.join(RESOURCES, "garpos_etl/323843_001_20240313_212100_00012_NOV770.raw")
TEST_MASTERFILE = os.path.join(RESOURCES, "garpos_etl/IVB1.master")
TEST_LEVERARM = os.path.join(RESOURCES, "garpos_etl/lever_arms")
TEST_SEABIRD = os.path.join(
    RESOURCES, "garpos_etl/skq201811s_ctd002svpavg.txt"
)
TEST_DFPO00_RAW = os.path.join(
    RESOURCES,
    "garpos_etl/323843_001_20240503_205027_00033_DFOP00.raw",
)

# Check if test resources can be found
assert os.path.exists(TEST_NOVATEL_RAW), f"File {TEST_NOVATEL_RAW} not found"
assert os.path.exists(TEST_NOVATEL_PROCESSED), f"File {TEST_NOVATEL_PROCESSED} not found"
assert os.path.exists(TEST_SONARDYNE_RAW), f"File {TEST_SONARDYNE_RAW} not found"
# assert os.path.exists(TEST_SONARDYNE_PROCESSED), f"File {TEST_SONARDYNE_PROCESSED} not found"
assert os.path.exists(
    TEST_GNSS_PPP_POSITION
), f"File {TEST_GNSS_PPP_POSITION} not found"


class TestIMUDataFrame:
    def test_novatel_parsing(self):
        """
        Test the parsing functionality of the IMUDataFrame.from_file() class method.
        """
        file_path = TEST_NOVATEL_RAW
        novatel_file = file_schemas.NovatelFile(location=file_path)
        imu_df_raw = novatel_to_imudf(novatel_file)
        imudataframe = obs_schemas.IMUDataFrame.validate(imu_df_raw,lazy=True)
        assert imudataframe is not None, "IMUDataFrame is not None"

    def test_dfo_parsing(self):
        """
        Test the parsing functionality of the IMUDataFrame.from_file() class method.
        """
        file_path = TEST_DFPO00_RAW
        dfo_file = file_schemas.DFPO00RawFile(location=file_path)
        imu_df_raw = dfpo00_to_imudf(dfo_file)
        assert imu_df_raw.shape[0] > 0, "IMUDataFrame has no data"

        imudataframe = obs_schemas.IMUDataFrame.validate(imu_df_raw,lazy=True)
        assert imudataframe is not None, "IMUDataFrame is None"

    def test_validation(self):
        """
        Test the validation of IMUDataFrame.

        This method tests the validation of IMUDataFrame by passing in a fake dataframe
        with invalid data and a real dataframe with valid data. It asserts that the
        validation fails for the fake dataframe and passes for the real dataframe.
        """
        file_path = TEST_NOVATEL_PROCESSED

        fake_dataframe = pd.DataFrame({
            "Time": [datetime(1880,1,1,tzinfo=timezone.utc), datetime(3000,1,1,tzinfo=timezone.utc), datetime(2024,1,1,tzinfo=timezone.utc)],
            "Latitude": [100, -100, 0],
            "Longitude": [0, 999, 0],
            "Height": [0, -300, 0],
            "NorthVelocity": [0, 100, 0],
            "EastVelocty": [0, 0, -1000],
            "Up_Vel": [0, 0, 1110],
            "Roll": [0, 1110, 0],
            "Pitch": [1110, 0, 0],
            "Azimuth": [0, 1110, 0]
        })

        try:
            imu_dataframe = obs_schemas.IMUDataFrame(fake_dataframe)
            assert False, "IMUDataFrame validation failed"
        except SchemaError as e:
            assert True, "IMUDataFrame validation failed"

        real_dataframe = pd.read_csv(file_path, sep=",")

        try:
            imu_dataframe = obs_schemas.IMUDataFrame(real_dataframe)
            assert True, "IMUDataFrame validation passed"
        except SchemaError as e:
            assert False, "IMUDataFrame validation failed"

class TestAcousticParsing:

    def test_sonardyne(self):
        """
        Test the parsing functionality of AcousticDataFrame.from_file() class method.
        """
        file_path = TEST_SONARDYNE_RAW
        sonardyne_file = file_schemas.SonardyneFile(location=file_path)
        acoustic_df_raw = sonardyne_to_acousticdf(sonardyne_file)
        acoustic_dataframe = obs_schemas.AcousticDataFrame.validate(acoustic_df_raw,lazy=True)
        assert acoustic_dataframe is not None, "AcousticDataFrame is not None"


class TestGNSSParsing:
    def test_kinfile(self):
        """
        Test the parsing functionality of PositionDataFrame.from_file() class method.
        """
        file_path = TEST_GNSS_PPP_POSITION
        kin_file = file_schemas.KinFile(location=file_path)
        gnss_df_raw = kin_to_gnssdf(kin_file)
        gnss_dataframe = obs_schemas.PositionDataFrame.validate(gnss_df_raw,lazy=True)
        assert gnss_dataframe is not None, "PositionDataFrame is not None"

    def test_gnss_validation(self):
        """
        Test the validation of PositionDataFrame.

        This method tests the validation of PositionDataFrame by passing in a fake dataframe
        with invalid data and a real dataframe with valid data. It asserts that the
        validation fails for the fake dataframe and passes for the real dataframe.
        """
        file_path = TEST_GNSS_PPP_POSITION

        fake_dataframe = pd.DataFrame({
            "time": [datetime(1880,1,1,tzinfo=timezone.utc),
                     datetime(3000,1,1,tzinfo=timezone.utc),
                     datetime(2024,1,1,tzinfo=timezone.utc)],
            "x": [0, -7000000, 0],
            "y": [0,-7000000, 0],
            "z": [0, -7000000, 0],
            "latitude": [100, -100, 0],
            "longitude": [0, 999, 0],
            "height": [0, -300, 0],
            "number_of_satellites": [130, 0, 0],
            "pdop": [0, 30, 0],
        })

        try:
            gnss_dataframe = obs_schemas.PositionDataFrame(fake_dataframe)
            assert False, "PositionDataFrame validation failed"
        except SchemaError as e:
            assert True, "PositionDataFrame validation failed"

class TestSiteConfigParsing:

    def test_masterfile(self):
        """
        Test the parsing functionality of masterfile_to_siteconfig() function.
        """
        masterfile = file_schemas.MasterFile(location=TEST_MASTERFILE)
        siteconfig:site_schemas.SiteConfig = masterfile_to_siteconfig(masterfile)
        assert siteconfig is not None, "SiteConfig is not None"
        assert siteconfig.position_llh.latitude == 54.3324
        assert siteconfig.position_llh.longitude == -158.4692
        assert siteconfig.position_llh.height == 10.3

        for transponder in siteconfig.transponders:
            assert transponder.id in ["5209", "5210", "5211", "5212"]
            assert transponder.position_llh.latitude >= 54
            assert transponder.position_llh.latitude <= 55
            assert transponder.position_llh.longitude >= -159
            assert transponder.position_llh.longitude <= -157
            assert transponder.position_llh.height >= -2200
            assert transponder.tat_offset >= 0.2

    def test_leverarms(self):

        leverarmfile = file_schemas.LeverArmFile(location=TEST_LEVERARM)
        atd_offset: site_schemas.ATDOffset = leverarmfile_to_atdoffset(leverarmfile)
        assert atd_offset is not None, "ATDOffset is not None"
        assert atd_offset.forward == 0.575
        assert atd_offset.rightward == 0.0
        assert atd_offset.downward == -0.844

class TestSeabirdParsing:

    def test_seabird(self):
        
        seabirdfile = file_schemas.SeaBirdFile(location=TEST_SEABIRD)
        sound_velocity_df_raw = seabird_to_soundvelocity(seabirdfile)
        sound_velocity_df_validated = obs_schemas.SoundVelocityDataFrame.validate(sound_velocity_df_raw,lazy=True)
        assert sound_velocity_df_validated is not None, "SoundVelocityDataFrame is not None"

class TestRinexParsing:

    def test_sonardyne(self):

        novatel_file = file_schemas.NovatelFile(location=TEST_NOVATEL_RAW)
        rinex_file = novatel_to_rinex(novatel_file,outdir=os.path.dirname(TEST_NOVATEL_RAW),site="IVB1")
        rinex_file.write(Path(TEST_NOVATEL_RAW).parent)
        assert rinex_file.location.exists(), f"File {rinex_file.location} not found"
        os.remove(rinex_file.location)


