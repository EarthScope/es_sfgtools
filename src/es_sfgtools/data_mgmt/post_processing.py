from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
from datetime import datetime
import json

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


class DataPostProcessor:
    def __init__(self, site: Site, directory_handler: DirectoryHandler):
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

    def setNetwork(self, network_id: str):
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
        network: str,
        station: str,
        override: bool = False,
        write_intermediate: bool = False,
    ):
        """
        Parses the surveys from the current campaign and adds them to the directory structure.

        Args:
            site (Site): The site object containing the campaign and survey information.
        """
        self.setNetwork(network_id=network)
        self.setStation(station_id=station)

        tileDBDir = self.currentStationDir.tiledb_directory

        shotDataTDB = TDBShotDataArray(tileDBDir.shot_data)

        for campaign in self.site.campaigns:
            self.setCampaign(campaign_id=campaign.name)

            with open(
                self.currentCampaignDir.campaign_metadata,
                "w",
            ) as f:
                json.dump(campaign.model_dump_json(), f, indent=4)

            for survey in campaign.surveys:
                self.setSurvey(survey_id=survey.id)

                # Prepare shotdata
                shotdata_file_name_unfiltered = (
                    f"{survey.id}_{survey.type}_shotdata.csv".strip()
                )
                shotdata_file_dest = (
                    self.currentSurveyDir.location / shotdata_file_name_unfiltered
                )

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
                        self.currentSurveyDir.shotdata = shotdata_file_dest

                if write_intermediate:

                    # Prepare PPP kinematic Position Data
                    kinpositiondata_file_name = (
                        f"{survey.id}_{survey.type}_kinpositiondata.csv".strip()
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
                        f"{survey.id}_{survey.type}_imupositiondata.csv".strip()
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
        campaign_id: str,
        survey_id: str = None,
        custom_filters: dict = None,
        shotdata_filter_config: dict = DEFAULT_FILTER_CONFIG,
        overwrite: bool = False,
    ) -> None:

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

            DataPostProcessor.prepare_single_survey(
                coordTransformer=self.coordTransformer,
                site=self.site,
                campaign=self.currentCampaign,
                survey=survey,
                campaignDir=self.currentCampaignDir,
                surveyDir=self.currentSurveyDir,
                tileDBDir=self.currentStationDir.tiledb_directory,
                custom_filters=custom_filters,
                filter_config=shotdata_filter_config,
                overwrite=overwrite,
            )

    @staticmethod
    def prepare_single_survey(
        coordTransformer: CoordTransformer,
        site: Site,
        campaign: Campaign,
        survey: Survey,
        campaignDir: CampaignDir,
        surveyDir: SurveyDir,
        tileDBDir: TileDBDir,
        custom_filters: dict = None,
        filter_config: dict = DEFAULT_FILTER_CONFIG,
        overwrite: bool = False,
    ):
        shotDataRaw = pd.read_csv(surveyDir.shotdata)
        if shotDataRaw.empty:
            logger.logwarn(
                f"No shot data found for survey {str(surveyDir.shotdata)}, skipping shot data preparation."
            )
            return

        file_name_filtered = surveyDir.shotdata.with_suffix("_filtered.csv")

        if file_name_filtered.exists():
            shot_data_filtered = pd.read_csv(file_name_filtered)

        if shot_data_filtered.empty or overwrite:
            shot_data_filtered = filter_shotdata(
                survey_type=survey.type,
                site=site,
                shot_data=shotDataRaw,
                kinPostionTDBUri=tileDBDir.kin_position_data,
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
            coord_transformer=coordTransformer, survey=survey, site=site
        )
        array_dpos_center = get_array_dpos_center(coordTransformer, GPtransponders)

        shotdata_out_path = (
            surveyDir.garpos / file_name_filtered.with_suffix("_rectified.csv").name
        )
        if shotdata_out_path.exists():
            shot_data_rectified = pd.read_csv(shotdata_out_path)
        if shot_data_rectified.empty or overwrite:
            shot_data_rectified = prepare_shotdata_for_garpos(
                coord_transformer=coordTransformer,
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

        obsfile_out_path = surveyDir.garpos.default_obsfile
        if not obsfile_out_path.exists() or overwrite:
            garpos_input = prepare_garpos_input_from_survey(
                shot_data_path=shotdata_out_path,
                survey=survey,
                site=site,
                campaign=campaign,
                ss_path=campaignDir.svp_file,
                array_dpos_center=array_dpos_center,
                num_of_shots=len(shot_data_rectified),
                GPtransponders=GPtransponders,
            )
        garpos_input.to_datafile(surveyDir.garpos.default_obsfile)
