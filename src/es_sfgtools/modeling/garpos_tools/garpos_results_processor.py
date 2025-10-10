"""
This module contains the GarposResultsProcessor class, which is responsible for processing and analyzing GARPOS results.
"""

import json
from datetime import datetime
from pathlib import Path
import numpy as np
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.collections import LineCollection
from matplotlib.colors import Normalize

from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.modeling.garpos_tools.schemas import GarposInput, ObservationData
from es_sfgtools.data_mgmt.directory_handler import SurveyDir,GARPOSSurveyDir,CampaignDir

sns.set_theme()

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

RESULTS_DIR_NAME = "results"

class GarposResultsProcessor:
    """
    A class to process and analyze GARPOS results.
    """

    def __init__(self, campaign_dir: CampaignDir):
        """
        Initializes the GarposResultsProcessor.

        Args:
            survey_dir (SurveyDir): The survey directory.
        """
        self.campaign_dir = campaign_dir

    def plot_ts_results(
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
        
        :param survey_id: The ID of the survey to plot results for.
        :type survey_id: str
        :param survey_type: Optional survey type to include in the title.
        :type survey_type: str, optional
        :param run_id: The GARPOS run ID to plot results for. Defaults to 0.
        :type run_id: int | str, optional
        :param res_filter: The residual filter value to apply. Defaults to 10.
        :type res_filter: float, optional
        :param savefig: Whether to save the figure as a PNG file. Defaults to False.
        :type savefig: bool, optional
        :param showfig: Whether to display the figure. Defaults to True.
        :type showfig: bool, optional

        """
        if (survey_dir := self.campaign_dir.surveys.get(survey_id)) is None:
            raise ValueError(f"Survey ID {survey_id} not found in campaign directory.")

        if (garpos_dir := survey_dir.garpos) is None:
            raise ValueError(f"GARPOS directory not found for survey ID {survey_id}.")

        results_dir: Path = garpos_dir.results_dir
        if not results_dir.exists():
            raise FileNotFoundError(f"Results directory {results_dir} does not exist.")

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
        results_df_raw["time"] = results_df_raw.ST.apply(lambda x: datetime.fromtimestamp(x))
        df_filter = results_df_raw["ResiRange"].abs() < res_filter
        results_df = results_df_raw[df_filter]
        unique_ids = results_df["MT"].unique()

        plt.figure(figsize=(16, 9))
        title = f"{self.campaign_dir.location.parent.stem}"
        if survey_type is not None:
            title += f" {survey_type}"
        title += f" Survey {survey_id} Results"
        plt.suptitle(title,x=.6,y=.95,fontsize=14)  # Move to left with left alignment
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
            pd.DataFrame({"x": mdates.date2num(results_df["time"])
                        , "y": results_df["ResiRange"]})
            .sort_values("x")
            .to_numpy()
        )
        segments = np.concatenate([points[:-1, np.newaxis, :], points[1:, np.newaxis, :]], axis=1)
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
        #ax1.add_collection(lc)
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
