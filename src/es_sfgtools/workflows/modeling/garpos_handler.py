"""
GarposHandler class for processing and preparing shot data for the GARPOS model.
"""

from pathlib import Path
from typing import Optional
import shutil
from datetime import datetime,timezone, UTC
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

from es_sfgtools.data_mgmt.directorymgmt import DirectoryHandler, GARPOSSurveyDir

from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposFixed,
    GarposInput,
    InversionParams,
)

try:
    from garpos import drive_garpos
except ImportError:
    # Handle the case where garpos is not available
    pass
from es_sfgtools.modeling.garpos_tools.schemas import GarposInput, ObservationData
from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.modeling.garpos_tools.functions import process_garpos_results

from es_sfgtools.utils.model_update import validate_and_merge_config
from ..utils.protocols import WorkflowABC

colors = [
    "blue",
    "green",
    "red",
    "cyan",
    "magenta",
    "yellow",
    "black",
    "brown",
    "orange",
    "pink",
]

class GarposHandler(WorkflowABC):
    """Handles the processing and preparation of shot data for the GARPOS model.

    This class provides a high-level interface for running the GARPOS model,
    including data preparation, setting parameters, and processing results.

    Attributes
    ----------
    directory_handler : DirectoryHandler
        Handles the directory structure.
    site : Site
        The site metadata.
    network : str
        The network name.
    station : str
        The station name.
    campaign : Campaign
        The campaign metadata.
    current_survey : Survey
        The current survey being processed.
    coord_transformer : CoordTransformer
        Coordinate transformer for the site.
    garpos_fixed : GarposFixed
        Fixed parameters for the GARPOS model.
    sound_speed_path : Path
        Path to the sound speed profile file.

    """
    mid_process_workflow = True

    def __init__(
                 self,
                 directory_handler: DirectoryHandler,
                 station_metadata: Site):
        """Initializes the GarposHandler.

        Parameters
        ----------
        directory_handler : DirectoryHandler
            The directory handler.
        site : Site
            The site metadata.
        """

        super().__init__(station_metadata=station_metadata,directory_handler=directory_handler)

        self.garpos_fixed = GarposFixed()

        self.current_garpos_survey_dir: GARPOSSurveyDir = None

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


        self.current_garpos_survey_dir = None

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

        if self.current_survey_dir.shotdata is None or not self.current_survey_dir.shotdata.exists():
            raise ValueError(f"Shotdata for survey {survey_id} not found in directory handler. Please run intermediate data processing to create shotdata file.")

        try:
            if self.current_survey_dir.garpos.shotdata_rectified.exists():
                self.current_garpos_survey_dir = self.current_survey_dir.garpos
                logger.set_dir(self.current_garpos_survey_dir.log_directory)
                return
        except Exception:
            pass
        raise ValueError(f"Rectified shotdata for survey {survey_id} not found in directory handler. Please run intermediate data processing to create rectified shotdata file.")

    def set_inversion_params(self, parameters: dict | InversionParams):
        """Set inversion parameters for the model.

        This method updates the inversion parameters of the model using the
        key-value pairs provided in the `args` dictionary. Each key in the
        dictionary corresponds to an attribute of the `inversion_params`
        object, and the associated value is assigned to that attribute.

        Parameters
        ----------
        parameters : dict | InversionParams
            A dictionary containing key-value pairs to update the inversion
            parameters or an InversionParams object.
        """

        self.garpos_fixed.inversion_params = validate_and_merge_config(
            base_class=self.garpos_fixed.inversion_params,
            override_config=parameters
        )

    def _run_garpos(
        self,
        obsfile_path: Path,
        results_dir: Path,
        custom_settings: Optional[dict | InversionParams] = None,
        run_id: int | str = 0,
        override: bool = False) -> Path:
        """Runs the GARPOS model.

        Parameters
        ----------
        obsfile_path : Path
            The path to the observation file.
        results_dir : Path
            The path to the results directory.
        custom_settings : Optional[dict | InversionParams], optional
            Custom GARPOS settings to apply, by default None.
        run_id : int | str, optional
            The run ID, by default 0.
        override : bool, optional
            If True, override existing results, by default False.

        Returns
        -------
        Path
            The path to the results file.
        """

        garpos_fixed_params = self.garpos_fixed.model_copy()
        if custom_settings is not None:
            garpos_fixed_params.inversion_params = validate_and_merge_config(
                base_class=garpos_fixed_params.inversion_params,
                override_config=custom_settings
            )

        garpos_input = GarposInput.from_datafile(obsfile_path)
        results_suffix = f"{garpos_input.survey_id}_{run_id}"
        results_path = results_dir / f"{results_suffix}-res.dat"
        
        if results_path.exists() and not override:
            print(f"Results already exist for {str(results_path)}")
            return None
        logger.loginfo(
            f"Running GARPOS model for {garpos_input.site_name}, {garpos_input.survey_id}. Run ID: {run_id}"
        )
        input_path = results_dir / f"_{run_id}_observation.ini"
        fixed_path = results_dir / f"_{run_id}_settings.ini"
        garpos_fixed_params._to_datafile(fixed_path)
        garpos_input.to_datafile(input_path)

        rf = drive_garpos(
            str(input_path),
            str(fixed_path),
            str(results_dir) + "/",
            f"{garpos_input.survey_id}_{run_id}",
            13,
        )
        return rf 

    def _run_garpos_survey(
        self,
        survey_id: str,
        custom_settings: Optional[dict | InversionParams] = None,
        run_id: int | str = 0,
        iterations: int = 1,
        override: bool = False,
    ) -> None:
        """Run the GARPOS model for a specific survey.

        Parameters
        ----------
        survey_id : str
            The ID of the survey to run.
        custom_settings : dict, optional
            Custom GARPOS settings to apply, by default None.
        run_id : int | str, optional
            The run identifier, by default 0.
        iterations : int, optional
            The number of iterations to run, by default 1.
        override : bool, optional
            If True, override existing results, by default False.

        Raises
        ------
        ValueError
            If the observation file does not exist.
        """
        logger.loginfo(f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}")

        try:
            self.set_survey(survey_id=survey_id)
        except ValueError as e:
            logger.logwarn(f"Skipping survey {survey_id}: {e}")
            return

        results_dir_main = self.current_garpos_survey_dir.results_dir
        results_dir = results_dir_main / f"run_{run_id}"
        if results_dir.exists() and override:
            # Remove existing results directory if override is True
            try:
                shutil.rmtree(results_dir)
            except Exception as e:
                logger.logerr(f"Failed to remove existing results directory {results_dir}: {e}")
                
        results_dir.mkdir(parents=True, exist_ok=True)

        obsfile_path = self.current_garpos_survey_dir.default_obsfile

        if not obsfile_path.exists():
            raise ValueError(f"Observation file not found at {obsfile_path}")

        initialInput = GarposInput.from_datafile(obsfile_path)

        for i in range(iterations):
            logger.loginfo(f"Iteration {i+1} of {iterations} for survey {survey_id}")

            obsfile_path = self._run_garpos(
                custom_settings=custom_settings,
                obsfile_path=obsfile_path,
                results_dir=results_dir,
                run_id=f"{i}",
                override=override,
            )
            if iterations > 1 and i < iterations - 1:
                iterationInput = GarposInput.from_datafile(obsfile_path)
                delta_position = iterationInput.delta_center_position.get_position()
                iterationInput.array_center_enu.east += delta_position[0]
                iterationInput.array_center_enu.north += delta_position[1]
                iterationInput.array_center_enu.up += delta_position[2]
                # zero out delta position for next iteration
                iterationInput.delta_center_position = initialInput.delta_center_position

                iterationInput.to_datafile(obsfile_path)

            # Add array delta center position to the array center enu

        results = GarposInput.from_datafile(obsfile_path)
        process_garpos_results(results)

    def run_garpos(
        self,
        survey_id: Optional[str] = None,
        run_id: int | str = 0,
        iterations: int = 1,
        override: bool = False,
        custom_settings: Optional[dict | InversionParams] = None, 
    ) -> None:
        """Run the GARPOS model for a specific date or for all dates.

        Parameters
        ----------
        survey_id : str, optional
            The ID of the survey to run, by default None.
        run_id : int | str, optional
            The run identifier, by default 0.
        iterations : int, optional
            The number of iterations to run, by default 1.
        override : bool, optional
            If True, override existing results, by default False.
        custom_settings : dict | InversionParams, optional
            Custom GARPOS settings to apply, by default None.
        """

        logger.loginfo(f"Running GARPOS model. Run ID: {run_id}")
        surveys = [s.id for s in self.current_campaign_metadata.surveys] if survey_id is None else [survey_id]

        for survey_id in surveys:
            logger.loginfo(
                f"Running GARPOS model for survey {survey_id}. Run ID: {run_id}"
            )
            self._run_garpos_survey(
                survey_id=survey_id, run_id=run_id, override=override, iterations=iterations, custom_settings=dict(custom_settings).get("inversion_params") if custom_settings else None
            )


    def plot_shotdata_replies_per_transponder(
        self,
        savefig: bool = False,
        showfig: bool = True,
    ) -> None:
        """Plots the time series results for a given survey.

        Parameters
        ----------
        survey_id : str, optional
            ID of the survey to plot results for, by default None.
        savefig : bool, optional
            If True, save the figure, by default False.
            If True, save the figure, by default False.
        showfig : bool, optional
            If True, display the figure, by default True.
        """
        self._plot_shotdata_replies_per_transponder(
            savefig=savefig,
            showfig=showfig,
        )

    def _plot_shotdata_replies_per_transponder(
            self,
            savefig: bool,
            showfig: bool,
    ) -> None:
        """
        Plots the shotdata replies for a given survey and transponder.

        Parameters
        ----------
        survey_id : str
            The ID of the survey to plot.
        survey_type : str
            The type of the survey to plot.
        savefig : bool
            If True, save the figure, by default False.
        showfig : bool
            If True, display the figure, by default True.
        """
        metadata_surveys = []
        for campaign in self.current_station_metadata.campaigns:
            if campaign.name == self.current_campaign_name:
                metadata_surveys = campaign.surveys

        metadata_time_windows = {}
        shotdata_time_windows = {}
        shotdata_dfs = {}
        shotdata_filtered_dfs = {}
        for survey_name in sorted(self.current_campaign_dir.surveys):
            if survey_name in [survey.id for survey in metadata_surveys]:
                for survey in metadata_surveys:
                    if survey.id == survey_name:
                        metadata_start = survey.start.replace(tzinfo=UTC)
                        metadata_end = survey.end.replace(tzinfo=UTC)
                        metadata_time_windows[survey_name] = (metadata_start, metadata_end)
                    continue
                try:
                    shotdata_filepath = self.current_campaign_dir.surveys[survey_name].shotdata
                    shotdata_df = pd.read_csv(shotdata_filepath, sep=",", header=0, index_col=0)
                    shotdata_dfs[survey_name] = shotdata_df
                    #use utc
                    start = datetime.fromtimestamp(shotdata_df['pingTime'].iloc[0], tz=timezone.utc)
                    end = datetime.fromtimestamp(shotdata_df['pingTime'].iloc[-1], tz=timezone.utc)
                    shotdata_time_windows[survey_name] = (start, end)

                    shotdata_filtered_filepath = self.current_campaign_dir.surveys[survey_name].shotdata_filtered
                    shotdata_filtered_df = pd.read_csv(shotdata_filtered_filepath, sep=",", header=0, index_col=0)
                    shotdata_filtered_dfs[survey_name] = shotdata_filtered_df
                
                except Exception as e:
                    print(e)

        fig, axs = plt.subplots(3, 1, figsize=(20, 15), sharex=False)
        colors = ['blue', 'orange', 'red', 'green', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan']
        for i, (survey_name, shotdata_df) in enumerate(shotdata_dfs.items()):
            try:
                unique_ids = shotdata_df["transponderID"].unique()
                for j, transponder_id in enumerate(unique_ids):
                    df = shotdata_df[shotdata_df["transponderID"] == transponder_id]
                    filtered_df = shotdata_filtered_dfs[survey_name][shotdata_filtered_dfs[survey_name]["transponderID"] == transponder_id]
                    # # Resample the data to 10 minute intervals and count replies
                    df = df.set_index(pd.to_datetime(df['pingTime'], unit='s'))
                    filtered_df = filtered_df.set_index(pd.to_datetime(filtered_df['pingTime'], unit='s'))
                    replies_per_bin = df['pingTime'].resample('10min').count()
                    filtered_replies_per_bin = filtered_df['pingTime'].resample('10min').count()
                    axs[j].scatter(replies_per_bin.index, replies_per_bin.values/40*100, label=f"{survey_name} - {transponder_id}", s=10, color='black')
                    axs[j].scatter(filtered_replies_per_bin.index, filtered_replies_per_bin.values/40*100, label=f"{survey_name} - {transponder_id} (Filtered)", s=10, color=colors[(i) % len(colors)])
                    total_pings = shotdata_df['pingTime'].nunique()
                    total_filtered_pings = shotdata_filtered_df['pingTime'].nunique()
                    survey_midpoint = metadata_time_windows[survey_name][0] + (metadata_time_windows[survey_name][1] - metadata_time_windows[survey_name][0]) / 2
                    axs[j].text(
                        survey_midpoint, 110,
                        f"{survey_name}\n{next((survey.type.value for survey in metadata_surveys if survey.id == survey_name), 'Unknown')}\ntotal pings: {total_pings}\ntotal replies: {replies_per_bin.sum()}\nfiltered replies: {filtered_replies_per_bin.sum()}\nfiltered reply %: {filtered_replies_per_bin.sum() / total_pings * 100:.2f}%", fontsize=12, ha='center'
                    )
                    axs[j].set_xlabel("Time")
                    axs[j].set_ylabel("% Expected replies per 10 min bin")
                    axs[j].set_ylim(0, 150)
                    axs[j].axvspan(xmin=metadata_time_windows[survey_name][0], xmax=metadata_time_windows[survey_name][1], color=colors[(i) % len(colors)], linestyle='--', linewidth=1, alpha=0.1)
            except Exception as e:
                logger.logwarn(f"Error processing {survey_name}")
        fig.suptitle(f"Shotdata Reply Percentages for {self.current_station_name} {self.current_campaign_name}")
        axs[0].set_title(f"{self.current_station_name} Transponder 5209")
        axs[1].set_title(f"{self.current_station_name} Transponder 5210")
        axs[2].set_title(f"{self.current_station_name} Transponder 5211")
        fig.tight_layout()
        if showfig:
            plt.show()
        
        
        fig_path =  f"{self.current_campaign_dir.location}/{self.current_station_name}_{self.current_campaign_name}_shotdata_replies.png"
        if savefig:
            logger.loginfo(f"Saving figure to {fig_path}")
            plt.savefig(
                fig_path,
                dpi=300,
                bbox_inches="tight",
                pad_inches=0.1,
            )
        
    def plot_residuals_per_transponder_before_and_after(
        self,
        survey_id: str,
        run_id: int | str = 0,
        savefig: bool = False,
        showfig: bool = True,
    ):
        surveys_to_process = []
        for survey in self.current_campaign_metadata.surveys:
            if survey.id == survey_id or survey_id is None:
                surveys_to_process.append((survey.id,survey.type.value))

        for survey_id, survey_type in surveys_to_process:
            try:
                self.set_survey(survey_id)
                self._plot_residuals_per_transponder_before_and_after(
                    survey_id=survey_id,
                    run_id=run_id,
                    savefig=savefig,
                    showfig=showfig,
                )
            except Exception as e:
                logger.logwarn(f"Skipping plotting for survey {survey_id}: {e}")
                continue
        

    def _plot_residuals_per_transponder_before_and_after(
        self,
        survey_id: str,
        run_id: int | str = 0,
        savefig: bool = False,
        showfig: bool = True,
    ):
        """Plots the residuals on 3 subplots for a given survey.

        Args:
            survey_id (str): The ID of the survey to plot results for.
            run_id (int | str, optional): The run ID of the survey results to plot. Defaults to 0.
            savefig (bool, optional): If True, save the figure, by default False.
            showfig (bool, optional): If True, display the figure, by default True.
        """
        results_dir: Path = self.current_garpos_survey_dir.results_dir
        run_dir = results_dir / f"run_{run_id}"
        if not run_dir.exists():
            raise FileNotFoundError(f"Run directory {run_dir} does not exist.")

        # Get *-res.dat files
        data_files = list(run_dir.glob("*-res.dat"))
        if not data_files:
            raise FileNotFoundError(f"No .dat files found in run directory {run_dir}.")

        """
            sort by iteration number if multiple files found

            >>> data_files = [NTH1.2025_A_1126_0-res.dat,NTH1.2025_A_1126_1-res.dat,NTH1.2025_A_1126_2-res.dat]
            >>> sorted_data_files = sorted(data_files, key=lambda x: int(x.stem.split("_")[-1].split("-")[0]))
            >>> sorted_data_files
            [NTH1.2025_A_1126_0-res.dat,NTH1.2025_A_1126_1-res.dat,NTH1.2025_A_1126_2-res.dat]

            """
        data_files = sorted(
            data_files, key=lambda x: int(x.stem.split("_")[-1].split("-")[0])
        )
        data_file = data_files[-1]
        logger.loginfo(f"Using data file {data_file} for plotting.")

        garpos_results = GarposInput.from_datafile(data_file)
        
        array_enu = garpos_results.array_center_enu
        array_dpos = garpos_results.delta_center_position
        if array_enu is None or array_dpos is None:
            raise ValueError("Array center or delta position not found in GARPOS results.")

        array_final_position = array_dpos.model_copy()
        array_final_position.east += array_enu.east
        array_final_position.north += array_enu.north
        array_final_position.up += array_enu.up

        results_df_raw = pd.read_csv(garpos_results.shot_data)
        results_df_raw = ObservationData.validate(results_df_raw, lazy=True)
        results_df_raw["time"] = results_df_raw.ST.apply(
            lambda x: datetime.fromtimestamp(x, timezone.utc)
        )
        #df_filter_1 = results_df_raw["ResiRange"].abs() < res_filter
        df_filter_2 = results_df_raw["flag"] == False
        #results_df = results_df_raw[df_filter_1 & df_filter_2]
        results_df = results_df_raw[df_filter_2]
        #logger.loginfo(results_df_raw.columns)
        unique_ids = results_df_raw["MT"].unique()
        #make a plot with 3 subplots showing ResiRange vs time for each unique_id
        fig, axs = plt.subplots(3, 1, figsize=(20, 8), sharex=True)
        fig.suptitle(f"Residuals for {self.current_station_name} {survey_id} (Run {run_id})")
        for i, unique_id in enumerate(unique_ids):
            transponder_df_raw = results_df_raw[results_df_raw["MT"] == unique_id].sort_values("time")
            transponder_df = results_df[results_df["MT"] == unique_id].sort_values("time")
            axs[i].scatter(transponder_df_raw["time"], transponder_df_raw[f"ResiRange"], s=1, label=f"{unique_id}_raw {transponder_df_raw['time'].count()}", color="blue")
            percent_remaining = round(transponder_df['time'].count() / transponder_df_raw['time'].count() * 100, 1)
            axs[i].scatter(transponder_df["time"], transponder_df[f"ResiRange"], s=1, label=f"{unique_id}_unflagged {transponder_df['time'].count()} ({percent_remaining} %)", color="orange")
            axs[i].set_ylabel("Residual (m)")
            axs[i].legend(loc="upper right")
            axs[i].grid()
        axs[-1].set_xlabel("Time")
        plt.xticks(rotation=45)
        # add gridlines
        for ax in axs:
            ax.grid()
        plt.tight_layout()
        fig_path =  f"{self.current_garpos_survey_dir.results_dir}/{self.current_station_name}_{survey_id}_flagged_residuals.png"
        if savefig:
            logger.loginfo(f"Saving figure to {fig_path}")
            plt.savefig(
                fig_path,
                dpi=300,
                bbox_inches="tight",
                pad_inches=0.1,
            )
        if showfig:
            plt.show()
    
    def plot_remaining_residuals_per_transponder(
        self,
        survey_id: str,
        run_id: int | str = 0,
        subplots: bool = True,
        savefig: bool = False,
        showfig: bool = True,
    ) -> None:
        """Plots the remaining residuals for each transponder.

        Args:
            survey_id (str): The ID of the survey to plot results for.
            run_id (int | str, optional): The run ID of the survey results to plot. Defaults to 0.
            savefig (bool, optional): If True, save the figure. Defaults to False.
            showfig (bool, optional): If True, display the figure. Defaults to True.
        """
        surveys_to_process = []
        for survey in self.current_campaign_metadata.surveys:
            if survey.id == survey_id or survey_id is None:
                surveys_to_process.append((survey.id,survey.type.value))

        for survey_id, survey_type in surveys_to_process:
            try:
                self.set_survey(survey_id)
                self._plot_remaining_residuals_per_transponder(
                    survey_id=survey_id,
                    run_id=run_id,
                    subplots=subplots,
                    savefig=savefig,
                    showfig=showfig,
                )
            except Exception as e:
                logger.logwarn(f"Skipping plotting for survey {survey_id}: {e}")
                continue
        
        
        

    def _plot_remaining_residuals_per_transponder(
        self,
        survey_id: str,
        run_id: int | str = 0,
        subplots: bool = True,
        savefig: bool = False,
        showfig: bool = True,
    ):
        """Plots the residuals on 3 subplots for a given survey.

        Args:
            survey_id (str): The ID of the survey to plot results for.
            survey_type (str, optional): The type of the survey. Defaults to None.
            run_id (int | str, optional): The run ID of the survey results to plot. Defaults to 0.
            res_filter (float, optional): The residual filter value to filter outrageous values (m). Defaults to 10.
            savefig (bool, optional): If True, save the figure, by default False.
            showfig (bool, optional): If True, display the figure, by default True.
        """
        results_dir: Path = self.current_garpos_survey_dir.results_dir
        run_dir = results_dir / f"run_{run_id}"
        if not run_dir.exists():
            raise FileNotFoundError(f"Run directory {run_dir} does not exist.")

        # Get *-res.dat files
        data_files = list(run_dir.glob("*-res.dat"))
        if not data_files:
            raise FileNotFoundError(f"No .dat files found in run directory {run_dir}.")

        """
            sort by iteration number if multiple files found

            >>> data_files = [NTH1.2025_A_1126_0-res.dat,NTH1.2025_A_1126_1-res.dat,NTH1.2025_A_1126_2-res.dat]
            >>> sorted_data_files = sorted(data_files, key=lambda x: int(x.stem.split("_")[-1].split("-")[0]))
            >>> sorted_data_files
            [NTH1.2025_A_1126_0-res.dat,NTH1.2025_A_1126_1-res.dat,NTH1.2025_A_1126_2-res.dat]

            """
        data_files = sorted(
            data_files, key=lambda x: int(x.stem.split("_")[-1].split("-")[0])
        )
        data_file = data_files[-1]
        logger.loginfo(f"Using data file {data_file} for plotting.")

        garpos_results = GarposInput.from_datafile(data_file)
        
        array_enu = garpos_results.array_center_enu
        array_dpos = garpos_results.delta_center_position
        if array_enu is None or array_dpos is None:
            raise ValueError("Array center or delta position not found in GARPOS results.")

        array_final_position = array_dpos.model_copy()
        array_final_position.east += array_enu.east
        array_final_position.north += array_enu.north
        array_final_position.up += array_enu.up

        results_df_raw = pd.read_csv(garpos_results.shot_data)
        results_df_raw = ObservationData.validate(results_df_raw, lazy=True)
        results_df_raw["time"] = results_df_raw.ST.apply(
            lambda x: datetime.fromtimestamp(x, timezone.utc)
        )
        #df_filter_1 = results_df_raw["ResiRange"].abs() < res_filter
        df_filter_2 = results_df_raw["flag"] == False
        #results_df = results_df_raw[df_filter_1 & df_filter_2]
        results_df = results_df_raw[df_filter_2]
        #logger.loginfo(results_df_raw.columns)
        unique_ids = results_df_raw["MT"].unique()
        colors = ["green", "orange", "blue"] 
        if subplots:
            #make a plot with 3 subplots showing ResiRange vs time for each unique_id
            fig, axs = plt.subplots(3, 1, figsize=(20, 8), sharex=True)
            fig.suptitle(f"Residuals for {self.current_station_name} {survey_id} (Run {run_id})")
            for i, unique_id in enumerate(unique_ids):
                transponder_df = results_df[results_df["MT"] == unique_id].sort_values("time")
                axs[i].scatter(transponder_df["time"], transponder_df[f"ResiRange"], s=1,label=f"{unique_id}_unflagged {transponder_df['time'].count()}", color=colors[i])
                axs[i].set_ylabel("Residual (m)")
                axs[i].legend(loc="upper right")
                axs[i].grid()
            axs[-1].set_xlabel("Time")
            plt.xticks(rotation=45)
            # add gridlines
            for ax in axs:
                ax.grid()
        else:
            fig, ax = plt.subplots(figsize=(20, 8))
            for i, unique_id in enumerate(unique_ids):
                transponder_df = results_df[results_df["MT"] == unique_id].sort_values("time")
                ax.scatter(transponder_df["time"], transponder_df[f"ResiRange"], s=1,label=f"{unique_id}_unflagged {transponder_df['time'].count()}", color=colors[i])
            ax.set_ylabel("Residual (m)")
            ax.legend()
            ax.grid()
            ax.set_xlabel("Time")
            plt.xticks(rotation=45)
            # add gridlines
            ax.grid()
        plt.tight_layout()
        fig_path =  f"{self.current_garpos_survey_dir.results_dir}/{self.current_station_name}_{survey_id}_garpos_residuals.png"
        if savefig:
            logger.loginfo(f"Saving figure to {fig_path}")
            plt.savefig(
                fig_path,
                dpi=300,
                bbox_inches="tight",
                pad_inches=0.1,
            )
        if showfig:
            plt.show()

    def plot_ts_results(
        self,
        survey_id: str = None,
        run_id: int | str = 0,
        res_filter: float = 10,
        savefig: bool = False,
        showfig: bool = True,
    ) -> None:
        """Plots the time series results for a given survey.

        Parameters
        ----------
        survey_id : str, optional
            ID of the survey to plot results for, by default None.
        run_id : int or str, optional
            The run ID of the survey results to plot, by default 0.
        res_filter : float, optional
            The residual filter value to filter outrageous values (m), by
            default 10.
        savefig : bool, optional
            If True, save the figure, by default False.
        showfig : bool, optional
            If True, display the figure, by default True.
        """
        surveys_to_process = []
        for survey in self.current_campaign_metadata.surveys:
            if survey.id == survey_id or survey_id is None:
                surveys_to_process.append((survey.id,survey.type.value))

        for survey_id, survey_type in surveys_to_process:
            try:
                self.set_survey(survey_id)
                self._plot_ts_results(
                    survey_id=survey_id,
                    survey_type=survey_type,
                    run_id=run_id,
                    res_filter=res_filter,
                    savefig=savefig,
                    showfig=showfig,
                )
            except Exception as e:
                logger.logwarn(f"Skipping plotting for survey {survey_id}: {e}")
                continue
    
    def _plot_ts_results(
        self,
        survey_id: str,
        survey_type: str = None,
        run_id: int | str = 0,
        res_filter: float = 10,
        savefig: bool = False,
        showfig: bool = True,
    ) -> None:
        """
        Plots the time series results for a given survey.

        Parameters
        ----------
        survey_id : str
            The ID of the survey to plot results for.
        survey_type : str, optional
            Optional survey type to include in the title.
        run_id : int or str, default 0
            The GARPOS run ID to plot results for.
        res_filter : float, default 10
            The residual filter value to apply.
        savefig : bool, default False
            Whether to save the figure as a PNG file.
        showfig : bool, default True
            Whether to display the figure.

        """

        # Clear previous plots
        #plt.clf()
        
        results_dir: Path = self.current_garpos_survey_dir.results_dir
        run_dir = results_dir / f"run_{run_id}"
        if not run_dir.exists():
            raise FileNotFoundError(f"Run directory {run_dir} does not exist.")

        # Get *-res.dat files
        data_files = list(run_dir.glob("*-res.dat"))
        if not data_files:
            raise FileNotFoundError(f"No .dat files found in run directory {run_dir}.")

        """
            sort by iteration number if multiple files found

            >>> data_files = [NTH1.2025_A_1126_0-res.dat,NTH1.2025_A_1126_1-res.dat,NTH1.2025_A_1126_2-res.dat]
            >>> sorted_data_files = sorted(data_files, key=lambda x: int(x.stem.split("_")[-1].split("-")[0]))
            >>> sorted_data_files
            [NTH1.2025_A_1126_0-res.dat,NTH1.2025_A_1126_1-res.dat,NTH1.2025_A_1126_2-res.dat]

            """
        data_files = sorted(
            data_files, key=lambda x: int(x.stem.split("_")[-1].split("-")[0])
        )
        data_file = data_files[-1]
        logger.loginfo(f"Using data file {data_file} for plotting.")

        garpos_results = GarposInput.from_datafile(data_file)

        """
            Get the array center position and delta position.
            Add the delta position to the array center position to get the final position.
            
            """
        array_enu = garpos_results.array_center_enu
        array_dpos = garpos_results.delta_center_position
        if array_enu is None or array_dpos is None:
            raise ValueError("Array center or delta position not found in GARPOS results.")

        array_final_position = array_dpos.model_copy()
        array_final_position.east += array_enu.east
        array_final_position.north += array_enu.north
        array_final_position.up += array_enu.up

        results_df_raw = pd.read_csv(garpos_results.shot_data)
        results_df_raw = ObservationData.validate(results_df_raw, lazy=True)
        results_df_raw["time"] = results_df_raw.ST.apply(
            lambda x: datetime.fromtimestamp(x, timezone.utc)
        )
        df_filter_1 = results_df_raw["ResiRange"].abs() < res_filter
        df_filter_2 = results_df_raw["flag"].eq(False)
        results_df = results_df_raw[df_filter_1 & df_filter_2]
        # Use raw IDs so we allocate plot space for every transponder present,
        # even if a transponder has no points after filtering.
        unique_ids = results_df_raw["MT"].unique()

        # Build a plot plan so we don't create empty (extra) subplots.
        # Always include the unfiltered plot when raw data exists; include the
        # filtered plot only when there are points after filtering.
        plot_plan: list[tuple[str, str]] = []
        for unique_id in unique_ids:
            df_raw_transponder = results_df_raw[results_df_raw["MT"] == unique_id]
            if not df_raw_transponder.empty:
                plot_plan.append((unique_id, "unfiltered"))
            df_filtered_transponder = results_df[results_df["MT"] == unique_id]
            if not df_filtered_transponder.empty:
                plot_plan.append((unique_id, "filtered"))

        # Number of time-series subplot rows (each entry in plot_plan is one row)
        total_rows = len(plot_plan)

        # Dynamic figure sizing:
        # - ~1 inch per time-series subplot row.
        # - fixed extra inches for map/box/hist panels, spacer, and top text.
        # Slightly > 1 inch per plot row to leave room for titles.
        ts_row_height_in = 1.2
        extra_height_in = 8.0
        spacer_rows = 2  # gap between last time-series x ticks and lower panels
        lower_panel_rows = 6  # box (3) + hist (3), map shares these rows on the right
        min_extra_rows = spacer_rows + lower_panel_rows
        extra_rows = max(int(np.ceil(extra_height_in / ts_row_height_in)), min_extra_rows)
        total_height = (total_rows + extra_rows) * ts_row_height_in

        plt.figure(figsize=(20, total_height))
        title = f"{self.current_campaign_dir.location.parent.stem}"
        if survey_type is not None:
            title += f" {survey_type}"
        title += f" Survey {survey_id} Results"
        plt.suptitle(title, x=0.6, y=0.96, fontsize=16)  # Move title higher up
        # GridSpec: with the figure height above, 1 row ~= 1 inch.
        gs = gridspec.GridSpec(total_rows + extra_rows, 16, hspace=1.35, wspace=0.35)
        
        # Adjust subplot parameters to add more space at the top
        plt.subplots_adjust(top=0.90, left=0.04, right=0.99, bottom=0.06)

        dpos_std = array_dpos.get_std_dev()
        dpos = array_dpos.get_position()
        figure_text = f"Array Final Position: East {array_final_position.east:.4f} m, North {array_final_position.north:.4f} m, Up {array_final_position.up:.4f} m\n"
        figure_text += f" Sig East {dpos_std[0]:.2f} m  Sig North {dpos_std[1]:.2f} m  Sig Up {dpos_std[2]:.2f} m \n"
        figure_text += f"Array Delta Position :  East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"
        for id, transponder in enumerate(garpos_results.transponders):
            try:
                dpos = transponder.position_enu.get_position()
                figure_text += f"TSP {transponder.id} : East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"
            except ValueError:
                figure_text += f"TSP {transponder.id} : No results found\n"

        print(figure_text)

        lower_start = total_rows + spacer_rows

        """
            Plot the waveglider track and transponder positions
            """
        # Make the ENU track plot larger: more columns and a bit more height.
        ax3 = plt.subplot(gs[lower_start:(lower_start + 6), 9:])
        ax3.set_aspect("equal", "box")
        ax3.set_xlabel("East (m)")
        ax3.set_ylabel("North (m)", labelpad=-1)
        colormap_times = results_df_raw.ST.to_numpy()
        colormap_times_scaled = (colormap_times - colormap_times.min()) / 3600
        norm = Normalize(
            vmin=0,
            vmax=(colormap_times.max() - colormap_times.min()) / 3600,
        )
        sc = ax3.scatter(
            results_df_raw["ant_e0"],
            results_df_raw["ant_n0"],
            c=colormap_times_scaled,
            cmap="viridis",
            label="Vessel",
            norm=norm,
            alpha=0.25,
        )
        ax3.scatter(0, 0, label="Origin", color="magenta", s=100)

        """
        Plot the time series of residuals - separate plot for each transponder
        """
        
        # Color mapping per transponder ID
        id_colors = {uid: colors[idx % len(colors)] for idx, uid in enumerate(unique_ids)}

        # Plot separate unfiltered/filtered plots based on plot_plan
        shared_ax = None
        last_ts_ax = None
        for row_idx, (unique_id, kind) in enumerate(plot_plan):
            if shared_ax is None:
                ax_ts = plt.subplot(gs[row_idx : row_idx + 1, 1:14])
                shared_ax = ax_ts
            else:
                ax_ts = plt.subplot(gs[row_idx : row_idx + 1, 1:14], sharex=shared_ax)

            if kind == "unfiltered":
                df_ts = results_df_raw[results_df_raw["MT"] == unique_id].sort_values("time")
                title_ts = f"Transponder {unique_id} - Unfiltered Data"
                label_ts = f"{unique_id} Unfiltered"
            else:
                df_ts = results_df[results_df["MT"] == unique_id].sort_values("time")
                title_ts = (
                    f"Transponder {unique_id} - Filtered Data (|residuals| < {res_filter}m, flag=False)"
                )
                label_ts = f"{unique_id} Filtered"

            ax_ts.plot(
                df_ts["time"],
                df_ts["ResiRange"].abs(),
                label=label_ts,
                color=id_colors.get(unique_id, "black"),
                linewidth=1,
                alpha=0.85,
            )
            ax_ts.set_title(title_ts, fontsize=11, pad=6)
            ax_ts.legend(loc="upper right")
            ax_ts.grid(True, alpha=0.3)

            # Hide datetime ticks on all but the bottom-most time-series plot
            if row_idx < (len(plot_plan) - 1):
                ax_ts.tick_params(
                    axis="x",
                    which="both",
                    bottom=False,
                    top=False,
                    labelbottom=False,
                    labeltop=False,
                )
            else:
                ax_ts.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H"))
                ax_ts.xaxis.set_major_locator(mdates.HourLocator(interval=6))
                ax_ts.set_xlabel("Time - Month / Day / Hour")
                plt.setp(ax_ts.xaxis.get_majorticklabels(), rotation=45, ha="right")

        
        # Create a y-label subplot on the left side
        ax_ylabel = plt.subplot(gs[:total_rows, 0])
        ax_ylabel.text(0.5, 0.5, "Range-Residuals (m)", rotation=90, va='center', ha='center', 
                      fontsize=14, weight='bold', transform=ax_ylabel.transAxes)
        ax_ylabel.axis('off')  # Hide the axes

        for transponder in garpos_results.transponders:
            try:
                idx = unique_ids.tolist().index(transponder.id)
                ax3.scatter(
                    transponder.position_enu.east,
                    transponder.position_enu.north,
                    label=f"{transponder.id}",
                    color=colors[idx % len(colors)],
                    s=100,
                )
            except ValueError as e:
                logger.logwarn(
                    f"Transponder {transponder.id} not found in results, skipping plotting. {e}"
                )
        cbar = plt.colorbar(sc, label="Time (hr)", norm=norm)
        ax3.legend()

        """
            Plot the residual range boxplot and histogram
            """
        ax2 = plt.subplot(gs[lower_start:(lower_start + 3), :9])
        resiRange = results_df_raw["ResiRange"].abs()
        
        resiRange_np = resiRange.to_numpy()
        resiRange_filter = np.abs(resiRange_np) < 50
        resiRange = resiRange[resiRange_filter]
        max_value = resiRange.max()
        flier_props = dict(marker=".", markerfacecolor="r", markersize=5, alpha=0.25)
        ax2.boxplot(resiRange.to_numpy(), vert=False, flierprops=flier_props)
        # keep axis plot limit slightly larger than max value for visibility
        ax2.set_xlim(0, max_value * 1.1)
        median = resiRange.median()
        q1 = resiRange.quantile(0.25)
        q3 = resiRange.quantile(0.75)
        ax2.text(
            0.5,
            1.2,
            f"Median: {median:.2f} , IQR 1: {q1:.2f}, IQR 3: {q3:.2f}",
            fontsize=10,
            verticalalignment="center",
            horizontalalignment="center",
        )
        ax2.set_xlabel("Residual Range (m)", labelpad=-1)
        ax2.yaxis.set_visible(False)
        ax2.set_title("Box Plot of Residual Range Values")
        bins = np.arange(0, res_filter, 0.05)
        counts, bins = np.histogram(resiRange_np, bins=bins, density=True)
        ax4 = plt.subplot(gs[(lower_start + 3):(lower_start + 6), :9])
        ax4.sharex(ax2)
        ax4.hist(bins[:-1], bins, weights=counts, edgecolor="black")
        ax4.axvline(median, color="blue", linestyle="-", label=f"Median: {median:.3f}")
        ax4.set_xlabel("Residual Range (m)", labelpad=-1)
        ax4.set_ylabel("Frequency")
        ax4.set_title(f"Histogram of Residual Range Values, within {res_filter:.1f} meters")
        ax4.legend()
        # add figure text
        plt.gcf().text(0.02, 0.98, figure_text, fontsize=9, ha="left", va="top")

        # Avoid tight_layout() here; it tends to compress the GridSpec time-series
        # area when there are only a few transponders.
        
        if showfig:
            plt.show()
        fig_path = run_dir / f"_{run_id}_results.png"

        if savefig:
            logger.loginfo(f"Saving figure to {fig_path}")
            plt.savefig(
                fig_path,
                dpi=300,
                bbox_inches="tight",
                pad_inches=0.1,
            )
        
