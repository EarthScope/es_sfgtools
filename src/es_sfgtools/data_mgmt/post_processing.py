"""
This module defines the DataPostProcessor class, which is responsible for post-processing of data.
"""
from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
from datetime import datetime
import json
import shutil

from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.data_models.metadata.benchmark import Benchmark, Transponder
from es_sfgtools.data_models.metadata.campaign import Survey, Campaign
from es_sfgtools.data_models.observables import ShotDataFrame
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposInput,
    GPTransponder,
    GPATDOffset,
    GPPositionENU,
    GPPositionLLH,
    GarposFixed
)
from es_sfgtools.data_mgmt.directory_handler import (
    DirectoryHandler,
    SurveyDir,
    GARPOSSurveyDir,
    TileDBDir,
    CampaignDir,
    NetworkDir,
)
from es_sfgtools.modeling.garpos_tools.functions import (
    CoordTransformer,
    rectify_shotdata,
)
from es_sfgtools.modeling.garpos_tools.data_prep import (
    prepare_garpos_input_from_survey,
    filter_shotdata,
    prepare_shotdata_for_garpos,
    GP_Transponders_from_benchmarks,
    get_array_dpos_center,  
)
from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.tiledb_tools.tiledb_schemas import (
    TDBShotDataArray,
    TDBKinPositionArray,
    TDBIMUPositionArray,
)
from es_sfgtools.modeling.garpos_tools.shot_data_utils import (
    filter_ping_replies,
    filter_wg_distance_from_center,
    good_acoustic_diagnostics,
    ok_acoustic_diagnostics,
    difficult_acoustic_diagnostics,
    filter_pride_residuals,
    DEFAULT_FILTER_CONFIG,
)


class IntermediateDataProcessor:
    """
    A class to handle post-processing of data.
    """
    def __init__(self, site: Site, directory_handler: DirectoryHandler, network: Optional[str] = None, station: Optional[str] = None, campaign: Optional[str] = None):
        """
        Initializes the IntermediateDataProcessor.

        :param site: The site metadata.
        :type site: Site
        :param directory_handler: The directory handler.
        :type directory_handler: DirectoryHandler
        """
        self.site = site
        self.directory_handler = directory_handler
        self.currentCampaign: Campaign = None
        self.currentStation: str = None
        self.currentNetwork: str = None
        self.currentSurvey: Survey = None
        self.currentNetworkDir: NetworkDir = None
        self.currentCampaignDir: CampaignDir = None
        self.currentSurveyDir: SurveyDir = None

        self.coordTransformer = CoordTransformer(
            latitude=site.arrayCenter.latitude,
            longitude=site.arrayCenter.longitude,
            elevation=-float(site.localGeoidHeight),
        )
        if station is not None and network is None:
            raise ValueError("Network must be provided if station is provided.")
        if campaign is not None and (station is None or network is None):
            raise ValueError("Network and station must be provided if campaign is provided.")
        
        if network is not None:
            self.setNetwork(network)
        if station is not None:
            self.setStation(station)
        if campaign is not None:
            self.setCampaign(campaign)


    def setNetwork(self, network_id: str):
        """
        Sets the current network.

        :param network_id: The ID of the network to set.
        :type network_id: str
        :raises ValueError: If the network is not found in the site metadata.
        """
        self.currentNetwork = None
        self.currentNetworkDir = None

        self.currentStation = None
        self.currentStationDir = None

        self.currentCampaign = None
        self.currentCampaignDir = None

        self.currentSurvey = None
        self.currentSurveyDir = None

        # Set current network attributes
        for network_name in self.site.networks:
            if network_name == network_id:
                self.currentNetwork = network_name
                break
        if self.currentNetwork is None:
            raise ValueError(f"Network {network_id} not found in site metadata.")

        if (
            currentNetworkDir := self.directory_handler.networks.get(
                self.currentNetwork, None
            )
        ) is None:
            currentNetworkDir = self.directory_handler.add_network(
                name=self.currentNetwork
            )
        self.currentNetworkDir = currentNetworkDir

    def setStation(self, station_id: str):
        """
        Sets the current station.

        :param station_id: The ID of the station to set.
        :type station_id: str
        :raises ValueError: If the station is not found in the site metadata.
        """

        self.currentStation = None
        self.currentStationDir = None

        self.currentCampaign = None
        self.currentCampaignDir = None

        self.currentSurvey = None
        self.currentSurveyDir = None

        # Set current station attributes
        for station_name in self.site.names:
            if station_name == station_id:
                self.currentStation = station_name
                break
        if self.currentStation is None:
            raise ValueError(f"Station {station_id} not found in site metadata.")

        if (
            currentStationDir := self.currentNetworkDir.stations.get(
                self.currentStation, None
            )
        ) is None:
            currentStationDir = self.currentNetworkDir.add_station(
                name=self.currentStation
            )
        self.currentStationDir = currentStationDir

    def setCampaign(self, campaign_id: str):
        """
        Sets the current campaign.

        :param campaign_id: The ID of the campaign to set.
        :type campaign_id: str
        :raises ValueError: If the campaign is not found in the site metadata.
        """
        self.currentCampaign = None
        self.currentCampaignDir = None

        self.currentSurvey = None
        self.currentSurveyDir = None

        # Set current campaign attributes

        for campaign in self.site.campaigns:
            if campaign.name == campaign_id:
                self.currentCampaign = campaign
                break
        if self.currentCampaign is None:
            raise ValueError(f"Campaign {campaign_id} not found in site metadata.")

        if (
            currentCampaignDir := self.currentStationDir.campaigns.get(
                self.currentCampaign.name, None
            )
        ) is None:
            currentCampaignDir = self.currentStationDir.add_campaign(name=campaign_id)
        self.currentCampaignDir = currentCampaignDir

    def setSurvey(self, survey_id: str):
        """
        Sets the current survey.

        :param survey_id: The ID of the survey to set.
        :type survey_id: str
        :raises ValueError: If the survey is not found in the current campaign.
        """
        assert isinstance(survey_id,str), "survey_id must be a string"

        self.currentSurvey = None
        self.currentSurveyDir = None
        # Set current survey attributes
        for survey in self.currentCampaign.surveys:
            if survey.id == survey_id:
                self.currentSurvey = survey
                break
        if self.currentSurvey is None:
            raise ValueError(
                f"Survey {survey_id} not found in campaign {self.currentCampaign.name}."
            )

        if (
            currentSurveyDir := self.currentCampaignDir.surveys.get(survey_id, None)
        ) is None:
            currentSurveyDir = self.currentCampaignDir.add_survey(name=survey_id)
        self.currentSurveyDir = currentSurveyDir

    def parse_surveys(
        self,
        survey_id: Optional[str] = None,
        override: bool = False,
        write_intermediate: bool = False,
    ):
        """
        Parses the surveys from the current campaign and adds them to the directory structure.

        :param network: The network ID.
        :type network: str
        :param station: The station ID.
        :type station: str
        :param campaign: The campaign ID.
        :type campaign: str, optional
        :param override: Whether to override existing files.
        :type override: bool, optional
        :param write_intermediate: Whether to write intermediate files.
        :type write_intermediate: bool, optional
        """
        if self.currentNetwork is None or self.currentStation is None:
            raise ValueError("Network and station must be set before parsing surveys.")

        if self.currentCampaign is None:
            raise ValueError("Campaign must be set before parsing surveys.")

        tileDBDir = self.currentStationDir.tiledb_directory

        shotDataTDB = TDBShotDataArray(tileDBDir.shot_data)
            
        with open(
            self.currentCampaignDir.campaign_metadata,
            "w",
        ) as f:
            json.dump(self.currentCampaign.model_dump_json(), f, indent=4)

        surveys_to_process: List[Survey] = []
        for survey in self.currentCampaign.surveys:
            if survey_id is None or survey_id == survey.id:
                surveys_to_process.append(survey)
        if not surveys_to_process:
            raise ValueError(f"Survey {survey_id} not found in campaign {self.currentCampaign.name}.")

        for survey in surveys_to_process:
            self.setSurvey(survey_id=survey.id)

            # Prepare shotdata
            shotdata_file_name_unfiltered = (
                f"{survey.id}_{survey.type}_shotdata.csv".replace(" ","")
            )
            shotdata_file_dest = (
                self.currentSurveyDir.location / shotdata_file_name_unfiltered
            )
            self.currentSurveyDir.shotdata = shotdata_file_dest
            if (
                not shotdata_file_dest.exists()
                or shotdata_file_dest.stat().st_size == 0
                or override
            ):
                shot_data_queried = shotDataTDB.read_df(
                    start=survey.start,
                    end=survey.end,
                )
                if shot_data_queried.empty:
                    logger.logwarn(
                        f"No shot data found for survey {survey.id} from {survey.start} to {survey.end}, skipping survey."
                    )
                    continue
                else:
                    shot_data_queried.to_csv(shotdata_file_dest)
                    

            if write_intermediate:

                # Prepare PPP kinematic Position Data
                kinpositiondata_file_name = (
                    f"{survey.id}_{survey.type}_kinpositiondata.csv".replace(" ","")
                )
                kinpositiondata_file_dest = (
                    self.currentSurveyDir.location / kinpositiondata_file_name
                )

                if (
                    not kinpositiondata_file_dest.exists()
                    or kinpositiondata_file_dest.stat().st_size == 0
                    or override
                ):
                    kinPositionTDB = TDBKinPositionArray(
                        tileDBDir.kin_position_data
                    )
                    kinposition_data_queried = kinPositionTDB.read_df(
                        start=survey.start,
                        end=survey.end,
                    )
                    if kinposition_data_queried.empty:
                        logger.logwarn(
                            f"No kinposition data found for survey {survey.id} from {survey.start} to {survey.end}"
                        )

                    else:
                        kinposition_data_queried.to_csv(kinpositiondata_file_dest)
                        self.currentSurveyDir.kinpositiondata = (
                            kinpositiondata_file_dest
                        )

                # Prepare IMU Position Data
                imupositiondata_file_name = (
                    f"{survey.id}_{survey.type}_imupositiondata.csv".replace(" ","")
                )
                imupositiondata_file_dest = (
                    self.currentSurveyDir.location / imupositiondata_file_name
                )
                if (
                    not imupositiondata_file_dest.exists()
                    or imupositiondata_file_dest.stat().st_size == 0
                    or override
                ):
                    imuPositionTDB = TDBIMUPositionArray(
                        tileDBDir.imu_position_data
                    )
                    imuposition_data_queried = imuPositionTDB.read_df(
                        start=survey.start,
                        end=survey.end,
                    )
                    if imuposition_data_queried.empty:
                        logger.logwarn(
                            f"No imuposition data found for survey {survey.id} from {survey.start} to {survey.end}"
                        )
                    else:
                        imuposition_data_queried.to_csv(imupositiondata_file_dest)
                        self.currentSurveyDir.imupositiondata = (
                            imupositiondata_file_dest
                        )

            with open(
                self.currentSurveyDir.metadata,
                "w",
            ) as f:
                json.dump(survey.model_dump_json(), f, indent=4)

        self.directory_handler.save()

    def prepare_shotdata_garpos(
        self,
        campaign_id: Optional[str] = None,
        survey_id: Optional[str] = None,
        custom_filters: Optional[dict] = None,
        shotdata_filter_config: dict = DEFAULT_FILTER_CONFIG,
        overwrite: bool = False,
    ) -> None:
        """
        Prepares shotdata for GARPOS processing.

        :param campaign_id: The ID of the campaign.
        :type campaign_id: str
        :param survey_id: The ID of the survey.
        :type survey_id: str, optional
        :param custom_filters: Custom filters to apply.
        :type custom_filters: dict, optional
        :param shotdata_filter_config: The shotdata filter configuration.
        :type shotdata_filter_config: dict, optional
        :param overwrite: Whether to overwrite existing files.
        :type overwrite: bool, optional
        """

        if campaign_id is None:
            if self.currentCampaign is None:
                raise ValueError("Campaign must be set before preparing GARPOS shotdata.")
        else:
            # load the campaign
            self.setCampaign(campaign_id=campaign_id)

        surveys_to_process = []
        for survey in self.currentCampaign.surveys:
            if survey_id is None or survey.id == survey_id:
                surveys_to_process.append(survey)
        if not surveys_to_process:
            raise ValueError(f"Survey {survey_id} not found in campaign {campaign_id}.")

        for survey in surveys_to_process:
            self.setSurvey(survey_id=survey.id)
            logger.loginfo(f"Processing survey {survey.id}")

            self.prepare_single_garpos_survey(
                survey=survey,
                custom_filters=custom_filters,
                filter_config=shotdata_filter_config,
                overwrite=overwrite,
            )

    def prepare_single_garpos_survey(
        self,
        survey: Survey,
        custom_filters: dict = None,
        filter_config: dict = DEFAULT_FILTER_CONFIG,
        overwrite: bool = False,
    ):
        """
            Prepares a single survey for GARPOS processing.
            
            :param survey: The survey metadata.
            :type survey: Survey
            :param custom_filters: Custom filters to apply.
            :type custom_filters: dict, optional
            :param filter_config: The filter configuration.
            :type filter_config: dict, optional
            :param overwrite: Whether to overwrite existing files.
            :type overwrite: bool, optional
            """
        if not self.currentSurveyDir.shotdata.exists():
            raise FileNotFoundError(
                f"Shotdata file {self.currentSurveyDir.shotdata} does not exist. Please run parse_surveys first."
            )
        shotDataRaw = pd.read_csv(self.currentSurveyDir.shotdata)
        if shotDataRaw.empty:
            logger.logwarn(
                    f"No shot data found for survey {str(self.currentSurveyDir.shotdata)}, skipping shot data preparation."
                )
            return
        
        garposDir : GARPOSSurveyDir = self.currentSurveyDir.garpos
        garposDir.build()

        if not garposDir.default_settings.exists():
            GarposFixed()._to_datafile(garposDir.default_settings)

        file_name_filtered = self.currentSurveyDir.shotdata.parent / f"{self.currentSurveyDir.shotdata.stem}_filtered.csv"
        garposDir.shotdata_filtered = file_name_filtered
        
        if file_name_filtered.exists():
            shot_data_filtered = pd.read_csv(file_name_filtered)
        else:
            shot_data_filtered = pd.DataFrame()
        if shot_data_filtered.empty or overwrite:
            shot_data_filtered = filter_shotdata(
                survey_type=survey.type,
                site=self.site,
                shot_data=shotDataRaw,
                kinPostionTDBUri=self.currentStationDir.tiledb_directory.kin_position_data,
                start_time=survey.start,
                end_time=survey.end,
                custom_filters=custom_filters,
                filter_config=filter_config,
            )
            if shot_data_filtered.empty:
                logger.logwarn(
                        f"No shot data remaining after filtering for survey {survey.id}, skipping survey."
                    )
                return

            shot_data_filtered.to_csv(file_name_filtered)

        GPtransponders = GP_Transponders_from_benchmarks(
            coord_transformer=self.coordTransformer, survey=survey, site=self.site
        )
        array_dpos_center = get_array_dpos_center(self.coordTransformer, GPtransponders)

        shotdata_out_path = (
                garposDir.location / f"{file_name_filtered.stem}_rectified.csv"
            )
        garposDir.shotdata_rectified = shotdata_out_path

        if shotdata_out_path.exists():
            shot_data_rectified = pd.read_csv(shotdata_out_path)
        else:
            shot_data_rectified = pd.DataFrame()
        if shot_data_rectified.empty or overwrite:
            shot_data_rectified = prepare_shotdata_for_garpos(
                    coord_transformer=self.coordTransformer,
                    shodata_out_path=shotdata_out_path,
                    shot_data=shot_data_filtered,
                    GPtransponders=GPtransponders,
                )
            if shot_data_rectified.empty:
                logger.logwarn(
                        f"No shot data remaining after rectification for survey {survey.id}, skipping survey."
                    )
                return
            shot_data_rectified.to_csv(shotdata_out_path)

        # Copy the campaign svp file to the garpos directory if it doesn't exist
        if not garposDir.svp_file.exists():
            if self.currentCampaignDir.svp_file.exists():
                shutil.copy(self.currentCampaignDir.svp_file, garposDir.svp_file)
            else:
                logger.logwarn(
                        f"No sound speed profile file found for campaign {self.currentCampaign.name}, GARPOS processing may fail."
                    )
        obsfile_out_path = garposDir.default_obsfile
        if not obsfile_out_path.exists() or overwrite:
            garpos_input = prepare_garpos_input_from_survey(
                    shot_data_path=shotdata_out_path,
                    survey=survey,
                    site=self.site,
                    campaign=self.currentCampaign,
                    ss_path=garposDir.svp_file,
                    array_dpos_center=array_dpos_center,
                    num_of_shots=len(shot_data_rectified),
                    GPtransponders=GPtransponders,
                )
            garpos_input.to_datafile(garposDir.default_obsfile)

        self.directory_handler.save()
