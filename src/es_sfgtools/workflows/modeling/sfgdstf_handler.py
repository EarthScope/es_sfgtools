"""
SFGDSTFHandler class for processing and preparing shot data for modeling.
"""

from pathlib import Path
from typing import Optional, Union
import shutil
from datetime import datetime
import numpy as np

# Plotting imports
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.collections import LineCollection
from matplotlib.colors import Normalize
sns.set_theme(style="whitegrid")

from es_sfgtools.data_mgmt.directorymgmt import DirectoryHandler

from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.modeling.garpos_tools.functions import (
    _run_garpos,
    process_garpos_results,
)
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposFixed,
    GarposInput,
    InversionParams,
)
from es_sfgtools.modeling.sfgdstf_tools.data_prep import sfgdstf_to_garpos
from es_sfgtools.utils.model_update import validate_and_merge_config
from ..utils.protocols import WorkflowABC

class SFGDSTFHandler(WorkflowABC):
    """Handles the processing and preparation of shot data for modeling."""
    mid_process_workflow = True

    def __init__(
                 self,
                 directory_handler: DirectoryHandler,
                 station_metadata: Site):
        """Initializes the SFGDSTFHandler.

        Parameters
        ----------
        directory_handler : DirectoryHandler
            The directory handler.
        site : Site
            The site metadata.
        """

        super().__init__(station_metadata=station_metadata,directory_handler=directory_handler)

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
        super().set_network(network_id=network_id)

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

        super().set_station(station_id=station_id)
     
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
        super().set_campaign(campaign_id=campaign_id)

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

        super().set_survey(survey_id=survey_id)

    def run_modeling(
        self,
        survey_id: Optional[str] = None,
        run_id: str = "Test",
        iterations: int = 1,
        override: bool = False,
        custom_settings: Optional[Union[dict, "InversionParams"]] = None,
    ) -> None:
        """
        Runs the modeling pipeline.
        """
        logger.loginfo(f"Running modeling for survey {survey_id}. Run ID: {run_id}")

        # Get the sfgdstf data from tiledb
        sfgdstf_tdb = self.directory_handler.get_station(self.current_station_name).get_tiledb_array("sfgdstf_acoustic_data")
        sfgdstf_df = sfgdstf_tdb.read_df()

        # Get the survey
        survey = self.current_campaign_metadata.get_survey(survey_id)

        # Create a temporary directory for the garpos input files
        garpos_tmp_dir = self.directory_handler.get_station(self.current_station_name).get_campaign(self.current_campaign_name).get_survey(survey_id).garpos_dir / "tmp"
        garpos_tmp_dir.mkdir(exist_ok=True)

        shot_data_path = garpos_tmp_dir / "shot_data.csv"
        sound_speed_path = self.current_campaign_dir.svp_file

        # Convert the data to garpos format
        garpos_input = sfgdstf_to_garpos(
            sfgdstf_df=sfgdstf_df,
            site_metadata=self.current_station_metadata,
            survey_id=survey_id,
            shot_data_path=shot_data_path,
            sound_speed_path=sound_speed_path,
        )
        
        # Create the results directory
        results_dir = self.directory_handler.get_station(self.current_station_name).get_campaign(self.current_campaign_name).get_survey(survey_id).results_dir / f"run_{run_id}"
        results_dir.mkdir(exist_ok=True, parents=True)

        # Run garpos
        obsfile_path = garpos_tmp_dir / "garpos_input.ini"
        garpos_input.to_datafile(obsfile_path)

        for i in range(iterations):
            logger.loginfo(f"Iteration {i+1} of {iterations} for survey {survey_id}")

            obsfile_path = _run_garpos(
                custom_settings=custom_settings,
                obsfile_path=obsfile_path,
                results_dir=results_dir,
                run_id=f"{i}",
                override=override,
                garpos_fixed=GarposFixed(),
            )
            if iterations > 1 and i < iterations - 1:
                iterationInput = GarposInput.from_datafile(obsfile_path)
                delta_position = iterationInput.delta_center_position.get_position()
                iterationInput.array_center_enu.east += delta_position[0]
                iterationInput.array_center_enu.north += delta_position[1]
                iterationInput.array_center_enu.up += delta_position[2]
                # zero out delta position for next iteration
                iterationInput.delta_center_position = garpos_input.delta_center_position

                iterationInput.to_datafile(obsfile_path)

        results = GarposInput.from_datafile(obsfile_path)
        process_garpos_results(results)
