"""
This module defines the DataPostProcessor class, which is responsible for post-processing of data.
"""
import json
import shutil
from typing import List, Optional

import pandas as pd

from es_sfgtools.data_mgmt.directorymgmt.handler import (
    CampaignDir,
    DirectoryHandler,
    GARPOSSurveyDir,
    NetworkDir,
    SurveyDir,
)
from es_sfgtools.data_models.metadata.campaign import Campaign, Survey,SurveyType
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.modeling.garpos_tools.data_prep import (
    GP_Transponders_from_benchmarks,
    get_array_dpos_center,
    prepare_garpos_input_from_survey,
    prepare_shotdata_for_garpos,
    apply_survey_config
)
from es_sfgtools.config.garpos_config import (
    DEFAULT_SITE_CONFIG,
)
from es_sfgtools.modeling.prefiltering import filter_shotdata

from es_sfgtools.modeling.garpos_tools.functions import (
    CoordTransformer,
)
from es_sfgtools.modeling.garpos_tools.schemas import GarposFixed

from es_sfgtools.tiledb_tools.tiledb_schemas import (
    TDBIMUPositionArray,
    TDBKinPositionArray,
    TDBShotDataArray,
)


class IntermediateDataProcessor:
    """
    A class to handle post-processing of data.
    """
    def __init__(self, site: Site, directory_handler: DirectoryHandler, network: Optional[str] = None, station: Optional[str] = None, campaign: Optional[str] = None):
        """Initializes the IntermediateDataProcessor.

        Parameters
        ----------
        site : Site
            The site metadata.
        directory_handler : DirectoryHandler
            The directory handler.
        network : Optional[str], optional
            The network ID, by default None.
        station : Optional[str], optional
            The station ID, by default None.
        campaign : Optional[str], optional
            The campaign ID, by default None.
        """
        self.site = site
        self.directory_handler = directory_handler
        self.current_campaign: Campaign = None
        self.current_station: str = None
        self.current_network: str = None
        self.current_survey: Survey = None
        self.current_network_dir: NetworkDir = None
        self.current_campaign_dir: CampaignDir = None
        self.current_survey_dir: SurveyDir = None

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
            self.set_network(network)
        if station is not None:
            self.set_station(station)
        if campaign is not None:
            self.set_campaign(campaign)

    def set_network(self, network_id: str):
        """Sets the current network.

        Parameters
        ----------
        network_id : str
            The ID of the network to set.

        Raises
        ------
        ValueError
            If the network is not found in the site metadata.
        """
        self.current_network = None
        self.current_network_dir = None

        self.current_station = None
        self.current_station_dir = None

        self.current_campaign = None
        self.current_campaign_dir = None

        self.current_survey = None
        self.current_survey_dir = None

        # Set current network attributes
        for network_name in self.site.networks:
            if network_name == network_id:
                self.current_network = network_name
                break
        if self.current_network is None:
            raise ValueError(f"Network {network_id} not found in site metadata.")

        if (
            current_network_dir := self.directory_handler.networks.get(
                self.current_network, None
            )
        ) is None:
            current_network_dir = self.directory_handler.add_network(
                name=self.current_network
            )
        self.current_network_dir = current_network_dir

    def set_station(self, station_id: str):
        """Sets the current station.

        Parameters
        ----------
        station_id : str
            The ID of the station to set.

        Raises
        ------
        ValueError
            If the station is not found in the site metadata.
        """

        self.current_station = None
        self.current_station_dir = None

        self.current_campaign = None
        self.current_campaign_dir = None

        self.current_survey = None
        self.current_survey_dir = None

        # Set current station attributes
        for station_name in self.site.names:
            if station_name == station_id:
                self.current_station = station_name
                break
        if self.current_station is None:
            raise ValueError(f"Station {station_id} not found in site metadata.")

        if (
            current_station_dir := self.current_network_dir.stations.get(
                self.current_station, None
            )
        ) is None:
            current_station_dir = self.current_network_dir.add_station(
                name=self.current_station
            )
        self.current_station_dir = current_station_dir

    def set_campaign(self, campaign_id: str):
        """Sets the current campaign.

        Parameters
        ----------
        campaign_id : str
            The ID of the campaign to set.

        Raises
        ------
        ValueError
            If the campaign is not found in the site metadata.
        """
        self.current_campaign = None
        self.current_campaign_dir = None

        self.current_survey = None
        self.current_survey_dir = None

        # Set current campaign attributes

        for campaign in self.site.campaigns:
            if campaign.name == campaign_id:
                self.current_campaign = campaign
                break
        if self.current_campaign is None:
            raise ValueError(f"Campaign {campaign_id} not found in site metadata.")

        if (
            current_campaign_dir := self.current_station_dir.campaigns.get(
                self.current_campaign.name, None
            )
        ) is None:
            current_campaign_dir = self.current_station_dir.add_campaign(name=campaign_id)
        self.current_campaign_dir = current_campaign_dir

    def set_survey(self, survey_id: str):
        """Sets the current survey.

        Parameters
        ----------
        survey_id : str
            The ID of the survey to set.

        Raises
        ------
        ValueError
            If the survey is not found in the current campaign.
        """
        assert isinstance(survey_id,str), "survey_id must be a string"

        self.current_survey = None
        self.current_survey_dir = None
        # Set current survey attributes
        for survey in self.current_campaign.surveys:
            if survey.id == survey_id:
                self.current_survey = survey
                break
        if self.current_survey is None:
            raise ValueError(
                f"Survey {survey_id} not found in campaign {self.current_campaign.name}."
            )

        if (
            current_survey_dir := self.current_campaign_dir.surveys.get(survey_id, None)
        ) is None:
            current_survey_dir = self.current_campaign_dir.add_survey(name=survey_id)
        self.current_survey_dir = current_survey_dir

    def parse_surveys(
        self,
        survey_id: Optional[str] = None,
        override: bool = False,
        write_intermediate: bool = False,
    ):
        """Parses the surveys from the current campaign and adds them to the directory structure.

        Parameters
        ----------
        survey_id : Optional[str], optional
            The ID of the survey to parse. If None, all surveys are parsed, by default None.
        override : bool, optional
            Whether to override existing files, by default False.
        write_intermediate : bool, optional
            Whether to write intermediate files, by default False.
        """
        if self.current_network is None or self.current_station is None:
            raise ValueError("Network and station must be set before parsing surveys.")

        if self.current_campaign is None:
            raise ValueError("Campaign must be set before parsing surveys.")

        tileDBDir = self.current_station_dir.tiledb_directory

        shotDataTDB = TDBShotDataArray(tileDBDir.shot_data)

        with open(
            self.current_campaign_dir.campaign_metadata,
            "w",
        ) as f:
            json_dict = json.loads(self.current_campaign.model_dump_json())
            json.dump(json_dict, f, indent=4)

        surveys_to_process: List[Survey] = []
        for survey in self.current_campaign.surveys:
            if survey_id is None or survey_id == survey.id:
                surveys_to_process.append(survey)
        if not surveys_to_process:
            raise ValueError(f"Survey {survey_id} not found in campaign {self.current_campaign.name}.")

        for survey in surveys_to_process:
            self.set_survey(survey_id=survey.id)

            # Prepare shotdata
            shotdata_file_name_unfiltered = (
                f"{survey.id}_{survey.type.value}_shotdata.csv".replace(" ","")
            )
            shotdata_file_dest = (
                self.current_survey_dir.location / shotdata_file_name_unfiltered
            )
            self.current_survey_dir.shotdata = shotdata_file_dest
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
                    f"{survey.id}_{survey.type.value}_kinpositiondata.csv".replace(" ","")
                )
                kinpositiondata_file_dest = (
                    self.current_survey_dir.location / kinpositiondata_file_name
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
                        self.current_survey_dir.kinpositiondata = (
                            kinpositiondata_file_dest
                        )

                # Prepare IMU Position Data
                imupositiondata_file_name = (
                    f"{survey.id}_{survey.type.value}_imupositiondata.csv".replace(" ","")
                )
                imupositiondata_file_dest = (
                    self.current_survey_dir.location / imupositiondata_file_name
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
                        self.current_survey_dir.imupositiondata = (
                            imupositiondata_file_dest
                        )

            with open(
                self.current_survey_dir.metadata,
                "w",
            ) as f:
                json.dump(survey.model_dump_json(), f, indent=4)

        self.directory_handler.save()

    def prepare_shotdata_garpos(
        self,
        campaign_id: Optional[str] = None,
        survey_id: Optional[str] = None,
        custom_filters: Optional[dict] = None,
        overwrite: bool = False,
    ) -> None:
        """Prepares shotdata for GARPOS processing.

        Parameters
        ----------
        campaign_id : Optional[str], optional
            The ID of the campaign, by default None.
        survey_id : Optional[str], optional
            The ID of the survey, by default None.
        custom_filters : Optional[dict], optional
            Custom filters to apply, by default None.
        overwrite : bool, optional
            Whether to overwrite existing files, by default False.
        """

        if campaign_id is None:
            if self.current_campaign is None:
                raise ValueError("Campaign must be set before preparing GARPOS shotdata.")
        else:
            # load the campaign
            self.set_campaign(campaign_id=campaign_id)

        surveys_to_process = []
        for survey in self.current_campaign.surveys:
            if survey_id is None or survey.id == survey_id:
                surveys_to_process.append(survey)
        if not surveys_to_process:
            raise ValueError(f"Survey {survey_id} not found in campaign {campaign_id}.")

        for survey in surveys_to_process:
            self.set_survey(survey_id=survey.id)
            logger.loginfo(f"Processing survey {survey.id}")

            self.prepare_single_garpos_survey(
                survey=survey,
                custom_filters=custom_filters,
                overwrite=overwrite
            )

    def prepare_single_garpos_survey(
        self,
        survey: Survey,
        custom_filters: dict = None,
        overwrite: bool = False,
    ):
        """Prepares a single survey for GARPOS processing.

        Parameters
        ----------
        survey : Survey
            The survey metadata.
        custom_filters : dict, optional
            Custom filters to apply, by default None.
        overwrite : bool, optional
            Whether to overwrite existing files, by default False.
        """
        if not self.current_survey_dir.shotdata.exists():
            raise FileNotFoundError(
                f"Shotdata file {self.current_survey_dir.shotdata} does not exist. Please run parse_surveys first."
            )
        shotDataRaw = pd.read_csv(self.current_survey_dir.shotdata)
        if shotDataRaw.empty:
            logger.logwarn(
                    f"No shot data found for survey {str(self.current_survey_dir.shotdata)}, skipping shot data preparation."
                )
            return

        garposDir : GARPOSSurveyDir = self.current_survey_dir.garpos
        garposDir.build()

        if not garposDir.default_settings.exists() or overwrite:
            GarposFixed()._to_datafile(garposDir.default_settings)
        

        file_name_filtered = self.current_survey_dir.shotdata.parent / f"{self.current_survey_dir.shotdata.stem}_filtered.csv"
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
                kinPostionTDBUri=self.current_station_dir.tiledb_directory.kin_position_data,
                start_time=survey.start,
                end_time=survey.end,
                custom_filters=custom_filters,
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
            if self.current_campaign_dir.svp_file.exists():
                shutil.copy(self.current_campaign_dir.svp_file, garposDir.svp_file)
            else:
                logger.logwarn(
                        f"No sound speed profile file found for campaign {self.current_campaign.name}, GARPOS processing may fail."
                    )
        obsfile_out_path = garposDir.default_obsfile
        if not obsfile_out_path.exists() or overwrite:
            garpos_input = prepare_garpos_input_from_survey(
                    shot_data_path=shotdata_out_path,
                    survey=survey,
                    site=self.site,
                    campaign=self.current_campaign,
                    ss_path=garposDir.svp_file,
                    array_dpos_center=array_dpos_center,
                    num_of_shots=len(shot_data_rectified),
                    GPtransponders=GPtransponders,
                )
            # Apply survey-type-specific configuration to garpos_input

            match survey.type:
                # TODO Get the right configs for survey patterns
                case _:
                    garpos_input_configured = apply_survey_config(DEFAULT_SITE_CONFIG, garpos_input)

            garpos_input_configured.to_datafile(garposDir.default_obsfile)

        self.directory_handler.save()