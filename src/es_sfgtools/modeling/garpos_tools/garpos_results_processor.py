"""
This module contains the GarposResultsProcessor class, which is responsible for processing and analyzing GARPOS results.
"""

import json
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.collections import LineCollection
from matplotlib.colors import Normalize

from es_sfgtools.logging import GarposLogger as logger
from es_sfgtools.modeling.garpos_tools.schemas import GarposInput, ObservationData

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

    def __init__(self, working_dir: Path):
        """
        Initializes the GarposResultsProcessor.

        Args:
            working_dir (Path): The working directory.
        """
        self.working_dir = working_dir

    def plot_ts_results(
        self,
        survey_id: str,
        run_id: int | str = 0,
        res_filter: float = 10,
        savefig: bool = False,
    ) -> None:
        """
        Plots the time series results for a given survey.
        Args:
            survey_id (str): ID of the survey to plot results for.
            run_id (int or str, optional): The run ID of the survey results to plot. Default is 0.
            res_filter (float, optional): The residual filter value to filter outrageous values (m). Default is 10.
            savefig (bool, optional): If True, save the figure. Default is False.

        Returns:
            None
        """
        results_dir = self.working_dir / survey_id / RESULTS_DIR_NAME
        results_path = results_dir / f"_{run_id}_results.json"
        with open(results_path, "r") as f:
            results = json.load(f)
            garpos_results = GarposInput(**results)
            arrayinfo = garpos_results.delta_center_position

        results_df_raw = pd.read_csv(garpos_results.shot_data)
        results_df_raw = ObservationData.validate(results_df_raw, lazy=True)
        results_df_raw["time"] = results_df_raw.ST.apply(lambda x: datetime.fromtimestamp(x))
        df_filter = results_df_raw["ResiRange"].abs() < res_filter
        results_df = results_df_raw[df_filter]
        unique_ids = results_df["MT"].unique()

        plt.figure(figsize=(16, 9))
        plt.suptitle(f"Survey {survey_id} Results")
        gs = gridspec.GridSpec(13, 16)
        figure_text = "Delta Center Position\n"
        dpos = arrayinfo.get_position()
        figure_text += f"Array :  East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"
        for id, transponder in enumerate(garpos_results.transponders):
            try:
                dpos = transponder.delta_center_position.get_position()
                figure_text += f"TSP {transponder.id} : East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"
            except ValueError:
                figure_text += f"TSP {transponder.id} : No results found\n"

        print(figure_text)
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
        ax1.add_collection(lc)
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
        plt.show()
        if savefig:
            plt.savefig(
                results_dir / f"_{run_id}_results.png",
                dpi=300,
                bbox_inches="tight",
                pad_inches=0.1,
            )
