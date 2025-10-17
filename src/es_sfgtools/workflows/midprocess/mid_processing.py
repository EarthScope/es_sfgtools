"""
This module defines the DataPostProcessor class, which is responsible for post-processing of data.
"""
import json
import shutil
from typing import List, Optional

from es_sfgtools.data_mgmt.directorymgmt.schemas import StationDir
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
from es_sfgtools.prefiltering import filter_shotdata

from es_sfgtools.modeling.garpos_tools.functions import (
    CoordTransformer,
)
from es_sfgtools.modeling.garpos_tools.schemas import GarposFixed, GarposInput

from es_sfgtools.tiledb_tools.tiledb_schemas import (
    TDBIMUPositionArray,
    TDBKinPositionArray,
    TDBShotDataArray,
)
from es_sfgtools.config.loadconfigs import get_survey_filter_config,get_garpos_site_config,GarposSiteConfig,FilterConfig

from es_sfgtools.workflows.utils.protocols import WorkflowABC,validate_network_station_campaign
from es_sfgtools.utils.model_update import validate_and_merge_config

class IntermediateDataProcessor(WorkflowABC):
    """
    A class to handle post-processing of data.
    """
    mid_process_workflow: bool = True

    def __init__(self, station_metadata: Site, directory_handler: DirectoryHandler):
        """Initializes the IntermediateDataProcessor.

        Parameters
        ----------
        station_metadata : Site
            The station metadata.
        directory_handler : DirectoryHandler
            The directory handler.
        """
        super().__init__(station_metadata=station_metadata,directory_handler=directory_handler)

        self.coordTransformer = CoordTransformer(
            latitude=station_metadata.arrayCenter.latitude,
            longitude=station_metadata.arrayCenter.longitude,
            elevation=-float(station_metadata.localGeoidHeight),
        )

    @validate_network_station_campaign
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

        tileDBDir = self.current_station_dir.tiledb_directory

        shotDataTDB = TDBShotDataArray(tileDBDir.shot_data)

        with open(
            self.current_campaign_dir.campaign_metadata,
            "w",
        ) as f:
            json_dict = json.loads(self.current_campaign_metadata.model_dump_json())
            json.dump(json_dict, f, indent=4)

        surveys_to_process: List[Survey] = []
        for survey in self.current_campaign_metadata.surveys:
            if survey_id is None or survey_id == survey.id:
                surveys_to_process.append(survey)
        if not surveys_to_process:
            raise ValueError(f"Survey {survey_id} not found in campaign {self.current_campaign_metadata.name}.")

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
            if self.current_campaign_metadata is None:
                raise ValueError("Campaign must be set before preparing GARPOS shotdata.")
        else:
            # load the campaign
            self.set_campaign(campaign_id=campaign_id)

        surveys_to_process = []
        for survey in self.current_campaign_metadata.surveys:
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
        custom_filters: Optional[dict] = None,
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
            filter_config = get_survey_filter_config(
                survey_type=survey.type,
                station_metadata=self.current_station_metadata,
            )
            if custom_filters is not None:
                filter_config = validate_and_merge_config(
                    base_model=filter_config,
                    update_dict=custom_filters,
                )
            shot_data_filtered = filter_shotdata(
                survey_type=survey.type,
                site=self.current_station_metadata,
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
            coord_transformer=self.coordTransformer, survey=survey, site=self.current_station_metadata
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
                        f"No sound speed profile file found for campaign {self.current_campaign_metadata.name}, GARPOS processing may fail."
                    )
        obsfile_out_path = garposDir.default_obsfile
        if not obsfile_out_path.exists() or overwrite:
            garpos_input = prepare_garpos_input_from_survey(
                    shot_data_path=shotdata_out_path,
                    survey=survey,
                    site=self.current_station_metadata,
                    campaign=self.current_campaign_metadata,
                    ss_path=garposDir.svp_file,
                    array_dpos_center=array_dpos_center,
                    num_of_shots=len(shot_data_rectified),
                    GPtransponders=GPtransponders,
                )
            # Apply survey-type-specific configuration to garpos_input
            site_config_update: GarposSiteConfig = get_garpos_site_config(survey.type)
            garpos_input_configured: GarposInput = apply_survey_config(
                site_config_update, garpos_input
            )

            garpos_input_configured.to_datafile(garposDir.default_obsfile)

        self.directory_handler.save()
