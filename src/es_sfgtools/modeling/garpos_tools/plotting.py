import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List
import datetime
import seaborn as sns
import json
from collections import defaultdict
from matplotlib.colors import Normalize
from matplotlib.ticker import FuncFormatter

sns.set_theme()

from es_sfgtools.modeling.garpos_tools.model_io import PositionENU, Transponder


class DOYResult:
    def __init__(self, year: int, doy: int, df_path: Path, results_path: Path):
        self.year = year
        self.doy = doy
        self.date = datetime.datetime(year, 1, 1) + datetime.timedelta(days=doy - 1)
        self.df = pd.read_csv(df_path)
        self.df["time"] = [
            self.date + datetime.timedelta(seconds=x) for x in self.df["ST"].tolist()
        ]
        self.unique_ids = self.df["MT"].unique().tolist()
        self.transponders = {}
        with open(results_path) as f:
            self.results = json.load(f)
            self.arrayinfo = PositionENU.model_validate(
                self.results["delta_center_position"]
            )
            for transponder in self.results["transponders"]:
                _transponder_ = Transponder.model_validate(transponder)
                self.transponders[_transponder_.id] = _transponder_


class DOYPlotter:
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

    def __init__(self, results: List[DOYResult]):
        self.results = sorted(results, key=lambda x: x.date)
        self.unique_ids = []
        for result in self.results:
            self.unique_ids += result.unique_ids
        self.unique_ids = list(set(self.unique_ids))

        self.transponder_ts = {}

        for result in self.results:
            start_date = result.df.time.min().date()
            for id, transponder in result.transponders.items():
                if id not in self.transponder_ts:
                    self.transponder_ts[id] = {}
                self.transponder_ts[id][start_date] = transponder

        self.df_merged_main = pd.concat([result.df for result in self.results])
        df_filter_1 = self.df_merged_main["ResiRange"].abs() < 2
        df_filter_2 = self.df_merged_main["ResiTT"].abs() < 2
        self.df_merged_main = self.df_merged_main[df_filter_1 & df_filter_2]

        self.df_merged = self.df_merged_main.copy()

    def set_df_merged_date(self,start:datetime.datetime,end:datetime.datetime):
        self.df_merged = self.df_merged_main[
            (self.df_merged_main["time"] >= start) & (self.df_merged_main["time"] <= end)
        ]

    def _plot_residuals_range(self):
        ax = self.ax[0].twinx()

        ax.set_xlabel("Time - Month / Day / Hour")
        ax.set_ylabel("Residuals - Range (M)")

        for i, unique_id in enumerate(self.unique_ids):
            df = self.df_merged[self.df_merged["MT"] == unique_id]
            ax.scatter(
                df["time"],
                df["ResiRange"],
                label=f"{unique_id}",
                color=self.colors[i],
                alpha=0.2,
                s=0.125,
            )
        # self.ax[1] = ax

    def _plot_residuals_time(self):
        ax = self.ax[0]
        ax.set_ylabel("Residuals - Time (s)")

        for i, unique_id in enumerate(self.unique_ids):
            df = self.df_merged[self.df_merged["MT"] == unique_id]

            ax.scatter(
                df["time"],
                df["ResiTT"],
                label=f"{unique_id}",
                color=self.colors[i],
                alpha=0.2,
                s=0.125,
            )

    def _plot_transponder_offsets(self):
        ax = self.ax[-1]
        ax.set_ylabel("Transponder  (m)")
        for i, id in enumerate(self.unique_ids):
            transponder_times = self.transponder_ts[id]
            times = list(transponder_times.keys())
            dpos = [x.delta_center_position for x in transponder_times.values()]
            d_east = [x.east for x in dpos]
            d_north = [x.north for x in dpos]
            d_up = [x.up for x in dpos]
            ax.plot(times, d_east, label=f"{id} - East", color=self.colors[i])
            ax.plot(times, d_north, label=f"{id} - North", color=self.colors[i + 3])
            ax.plot(times, d_up, label=f"{id} - Up", color=self.colors[i + 6])

    def plot(self):
        self.fig, self.ax = plt.subplots(ncols=1, nrows=2, sharex=True)
        self.fig.suptitle(
            f"Residuals for {self.df_merged['time'].min().date()} to {self.df_merged['time'].max().date()}"
        )

        self._plot_residuals_range()
        self._plot_residuals_time()
        # self._plot_transponder_offsets()
        self.ax[1].legend(bbox_to_anchor=(1, 1), frameon=False)
        # self.ax[2].legend( bbox_to_anchor=(1, 1), frameon=False)

        plt.show()
        plt.tight_layout()

    def make_survey_image(self,start_date:datetime.datetime=None,end_date:datetime.datetime=None,survey_type="survey",survey_name="survey",filepath="survey_image.png"):
        '''
        Generate a plot of antenna positions along east/north axis with transponder positions

        1. plot transponder east, north position as markers
        2. plot antenna east, north position as line plot (from self.df_merged)

        '''
        self.set_df_merged_date(start_date,end_date)
        title = f"{survey_name} {survey_type} from {start_date} to {end_date}"
        fig, ax = plt.subplots(figsize=(16,10))
        fig.suptitle(title)
        ax.set_xlabel("East (m)")
        ax.set_ylabel("North (m)")
        ax.scatter(0,0,label="Origin",color="magenta",s=100)
        for result in self.results:
            for idx,(id, transponder) in enumerate(result.transponders.items()):
                ax.scatter(transponder.position_enu.east, transponder.position_enu.north, label=f"{id}", color=self.colors[idx],s=100)
            break
        colormap_times = self.df_merged["time"].apply(lambda x:x.timestamp()).to_numpy()
        colormap_times_scaled = (colormap_times - colormap_times.min())/3600

        norm = Normalize(
            vmin=0,
            vmax=(colormap_times.max() - colormap_times.min()) / 3600,
        )

        sc = ax.scatter(
            self.df_merged["ant_e0"],
            self.df_merged["ant_n0"],
            c=colormap_times_scaled,
            cmap="viridis",
            label="Antenna Position",
            norm=norm,
            alpha=0.25
        )
        norm = Normalize(
            vmin=0,
            vmax=(colormap_times.max() - colormap_times.min())/3600,
        )
        cbar = plt.colorbar(sc,label="Time (hr)",norm=norm)
        ax.legend()
        plt.savefig(filepath)
        plt.show()
        self.df_merged = self.df_merged_main.copy()

    def make_ts_plots(self,start_date:datetime.datetime=None,end_date:datetime.datetime=None,filepath="ts_plot.png"):
        self.set_df_merged_date(start_date,end_date)
        plt.clf()
        fig, ax = plt.subplots(nrows=2,figsize=(16,5))

        fig.suptitle(f"Residuals from {start_date} to {end_date}")

        ax[0].set_xlabel("Time - Month / Day / Hour")
        ax[0].set_ylabel("Residuals - Range (M)")

        for i, unique_id in enumerate(self.unique_ids):
            df = self.df_merged[self.df_merged["MT"] == unique_id]
            ax[0].plot(
                df["time"],
                df["ResiRange"],
                label=f"{unique_id}",
                color=self.colors[i],
                
             
                
            )
        ax[0].legend()
        ax[1].clear()
        ax[1].set_facecolor('white')
        ax[1].axis('off')  # Turn off the axis

        figure_text = "Delta Center Position\n"
        for result in self.results:
            if result.date.date() == start_date.date():
                current_result = result
                break
        dpos = current_result.arrayinfo.get_position()
        figure_text += f"Array :  East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"
        for id, transponder in current_result.transponders.items():
            dpos = transponder.delta_center_position.get_position()
            figure_text += f"Transponder {id} : East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"

        ax[1].text(0.5, 0.5, figure_text, horizontalalignment='center',verticalalignment='center', transform=ax[1].transAxes,fontsize=12)

        plt.savefig(filepath)
        plt.show()
        self.df_merged = self.df_merged_main.copy()

if __name__ == "__main__":
    gp_results_path = Path(
        "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/GARPOS/results"
    )
    # list all directories in the results path
    results_dirs = [x for x in gp_results_path.iterdir() if x.is_dir()]

    doy_results = []
    # dir names are YYYY_DOY
    for dir in results_dirs:
        year, doy = dir.stem.split("_")
        year = int(year)
        doy = int(doy)
        df_path = dir / "_0_results_df.csv"
        results_path = dir / "_0_results.json"
        if not df_path.exists():
            continue
        doy_result = DOYResult(year, doy, df_path, results_path)
        doy_results.append(doy_result)
    # df_path = Path(
    #     "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/GARPOS/results/2024_266"
    # ) / "_0_results_df.csv"

    # year = 2024
    # doy = 266
    # doy_result = DOYResult(year,doy,df_path)
    plotter = DOYPlotter(doy_results)
    """
      "start": "2024-09-22T17:30:00",
                    "end": "2024-09-23T00:35:00"
    """
    start = datetime.datetime(2024, 9, 22, 17, 30)
    end = datetime.datetime(2024, 9, 23, 0, 35)
  
    plotter.make_survey_image(start_date=start,end_date=end,survey_type="Cascadia NCC1",survey_name="CircleDrive")
    plotter.make_ts_plots(start_date=start,end_date=end)
