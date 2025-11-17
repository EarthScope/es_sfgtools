"""
GarposHandler class for processing and preparing shot data for the GARPOS model.
"""

from pathlib import Path
from typing import Optional
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
        results_path = results_dir / f"_{run_id}_results.json"

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
            shutil.rmtree(results_dir)
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
            lambda x: datetime.fromtimestamp(x)
        )
        df_filter = results_df_raw["ResiRange"].abs() < res_filter
        results_df = results_df_raw[df_filter]
        unique_ids = results_df["MT"].unique()

        plt.figure(figsize=(16, 9))
        title = f"{self.current_campaign_dir.location.parent.stem}"
        if survey_type is not None:
            title += f" {survey_type}"
        title += f" Survey {survey_id} Results"
        plt.suptitle(title, x=0.6, y=0.95, fontsize=14)  # Move to left with left alignment
        gs = gridspec.GridSpec(13, 16)

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

        """
            Plot the waveglider track and transponder positions
            """
        ax3 = plt.subplot(gs[6:, 8:])
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
            Plot the time series of residuals
            """
        ax1 = plt.subplot(gs[1:5, :])
        points = (
            pd.DataFrame(
                {"x": mdates.date2num(results_df["time"]), "y": results_df["ResiRange"]}
            )
            .sort_values("x")
            .to_numpy()
        )
        segments = np.concatenate(
            [points[:-1, np.newaxis, :], points[1:, np.newaxis, :]], axis=1
        )
        lc = LineCollection(segments, cmap="viridis", norm=norm, linewidth=5, zorder=10)
        lc.set_array(colormap_times_scaled)

        for i, unique_id in enumerate(unique_ids):
            df = results_df[results_df["MT"] == unique_id].sort_values("time")
            ax1.plot(
                df["time"],
                df["ResiRange"],
                label=f"{unique_id}",
                color=colors[i],
                linewidth=1,
                zorder=i,
                alpha=0.75,
            )
        # ax1.add_collection(lc)
        ax1.set_xlabel("Time - Month / Day / Hour")
        ax1.set_ylabel("Residuals - Range (M)", labelpad=-1)
        ax1.xaxis.set_label_position("top")
        ax1.xaxis.set_ticks_position("top")
        ax1.legend()

        for transponder in garpos_results.transponders:
            try:
                idx = unique_ids.tolist().index(transponder.id)
                ax3.scatter(
                    transponder.position_enu.east,
                    transponder.position_enu.north,
                    label=f"{transponder.id}",
                    color=colors[idx],
                    s=100,
                )
            except ValueError:
                logger.logwarn(
                    f"Transponder {transponder.id} not found in results, skipping plotting."
                )
        cbar = plt.colorbar(sc, label="Time (hr)", norm=norm)
        ax3.legend()

        """
            Plot the residual range boxplot and histogram
            """
        ax2 = plt.subplot(gs[6:9, :7])
        resiRange = results_df_raw["ResiRange"]
        resiRange_np = resiRange.to_numpy()
        resiRange_filter = np.abs(resiRange_np) < 50
        resiRange = resiRange[resiRange_filter]
        flier_props = dict(marker=".", markerfacecolor="r", markersize=5, alpha=0.25)
        ax2.boxplot(resiRange.to_numpy(), vert=False, flierprops=flier_props)
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
        bins = np.arange(-res_filter, res_filter, 0.05)
        counts, bins = np.histogram(resiRange_np, bins=bins, density=True)
        ax4 = plt.subplot(gs[10:, :7])
        ax4.sharex(ax2)
        ax4.hist(bins[:-1], bins, weights=counts, edgecolor="black")
        ax4.axvline(median, color="blue", linestyle="-", label=f"Median: {median:.3f}")
        ax4.set_xlabel("Residual Range (m)", labelpad=-1)
        ax4.set_ylabel("Frequency")
        ax4.set_title(f"Histogram of Residual Range Values, within {res_filter:.1f} meters")
        ax4.legend()
        # add figure text
        plt.gcf().text(0.2, 0.85, figure_text, fontsize=11, ha="center")
        if showfig:
            plt.show()
        if savefig:
            plt.savefig(
                run_dir / f"_{run_id}_results.png",
                dpi=300,
                bbox_inches="tight",
                pad_inches=0.1,
            )
