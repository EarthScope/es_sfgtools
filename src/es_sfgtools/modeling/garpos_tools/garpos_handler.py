import pandera as pa
from pandera.typing import DataFrame
from pathlib import Path
from typing import List, Tuple, Union
import pandas as pd
from configparser import ConfigParser
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np
import pymap3d as pm
import julian
from scipy.stats import hmean as harmonic_mean
from scipy.stats import norm as normal_dist
import json
from matplotlib.colors import Normalize
from matplotlib.collections import LineCollection
import matplotlib.dates as mdates
import seaborn as sns

sns.set_theme()
import shutil
import matplotlib.gridspec as gridspec

from es_sfgtools.processing.assets.observables import (
    ShotDataFrame
)

from es_sfgtools.modeling.garpos_tools.schemas import (
    GarposFixed,
    InversionParams,
    ObservationData,
    GarposInput,
    GPTransponder,
    GPATDOffset,
    GPPositionENU,
    GPPositionLLH
)
from es_sfgtools.modeling.garpos_tools.functions import CoordTransformer,process_garpos_results,rectify_shotdata

from es_sfgtools.utils.metadata.site import Site as MetaSite
from es_sfgtools.utils.metadata.vessel import Vessel as MetaVessel
from es_sfgtools.utils.loggers import GarposLogger as logger

from ...processing.assets.tiledb_temp import TDBShotDataArray

from garpos import drive_garpos


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

def avg_transponder_position(
    transponders: List[GPTransponder],
) -> Tuple[GPPositionENU, GPPositionLLH]:
    """
    Calculate the average position of the transponders

    Args:
        transponders: List of transponders

    Returns:
        Tuple[PositionENU, PositionLLH]: Average position in ENU and LLH
    """
    pos_array_llh = []
    pos_array_enu = []
    for transponder in transponders:
        pos_array_llh.append(
            [
                transponder.position_llh.latitude,
                transponder.position_llh.longitude,
                transponder.position_llh.height,
            ]
        )
        pos_array_enu.append(transponder.position_enu.get_position())
    avg_pos_llh = np.mean(pos_array_llh, axis=0).tolist()
    avg_pos_enu = np.mean(pos_array_enu, axis=0).tolist()

    min_pos_llh = np.min(pos_array_llh, axis=0).tolist()

    out_pos_llh = GPPositionLLH(
        latitude=avg_pos_llh[0], longitude=avg_pos_llh[1], height=avg_pos_llh[2]
    )
    out_pos_enu = GPPositionENU(
        east=avg_pos_enu[0], north=avg_pos_enu[1], up=avg_pos_enu[2]
    )

    return out_pos_enu, out_pos_llh


class GarposHandler:
    """
    GarposHandler is a class that handles the processing and preparation of shot data for the GARPOS model.
    It includes methods for rectifying shot data, preparing shot data files, setting inversion parameters,
    generating observation parameter files, generating data files with fixed parameters, and running the GARPOS model.

    Note:
        Respective instances of this class are intended to be for individual stations only.

    Attributes:
        LIB_DIRECTORY (str): Directory path for the RayTrace library.
        LIB_RAYTRACE (str): Path to the RayTrace library.
        shotdata (TDBShotDataArray): Array containing shot data.
        site_config (SiteConfig): Configuration for the site.
        working_dir (Path): Working directory path.
        shotdata_dir (Path): Directory path for shot data.
        results_dir (Path): Directory path for results.
        inversion_params (InversionParams): Parameters for the inversion process.
        dates (list): List of unique dates from the shot data.
        coord_transformer (CoordTransformer): Coordinate transformer for converting coordinates.

    Methods:
        __init__(self, shotdata: TDBShotDataArray, site_config: SiteConfig, working_dir: Path):
            Initializes the GarposHandler with shot data, site configuration, and working directory.
        _rectify_shotdata(self, shot_data: pd.DataFrame) -> pd.DataFrame:
        prep_shotdata(self, overwrite: bool = False):
            Prepares and saves shot data for each date in the object's date list.
        set_inversion_params(self, parameters: dict | InversionParams):
            Sets inversion parameters for the model.
        _input_to_datafile(self, shot_data: Path, path: Path, n_shot: int) -> None:
        _garposfixed_to_datafile(self, inversion_params: InversionParams, path: Path) -> None:
        _run_garpos(self, date: datetime, run_id: int | str = 0) -> GarposResults:
            Runs the GARPOS model for a given date and run ID.
        run_garpos(self, date_index: int = None, run_id: int | str = 0) -> None:
            Runs the GARPOS model for a specific date or for all dates.
    """

    def __init__(self, 
                 shotdata: TDBShotDataArray, 
                 working_dir: Path,
                 site_path:Path,
                 sound_speed_path:Path,
                 vessel_path:Path):
        """
        Initializes the class with shot data, site configuration, and working directory.
        Args:
            shotdata (TDBShotDataArray): The shot data array.
            site_config (SiteConfig): The site configuration.
            working_dir (Path): The working directory path.
        """
        self.garpos_fixed = GarposFixed()
        
        self.shotdata = shotdata
        self.working_dir = working_dir
        self.shotdata_dir = working_dir / "shotdata"
        self.shotdata_dir.mkdir(exist_ok=True, parents=True)
        self.results_dir = working_dir / "results"
        self.results_dir.mkdir(exist_ok=True, parents=True)

        self.current_campaign = None
        self.current_survey = None
        self.coord_transformer = None
        self.set_site_data(site_path=site_path,sound_speed_path=sound_speed_path,vessel_path=vessel_path)
        self.garpos_fixed._to_datafile(path=self.working_dir/"default_settings.ini")

    def set_site_data(
        self, site_path: Path | str, sound_speed_path: Path | str, vessel_path: Path|str
    ):
        self.site = MetaSite.from_json(site_path)
        self.vessel_meta = MetaVessel.from_json(vessel_path)
        # copy sound speed data to the working directory
        new_ss_path = self.working_dir / sound_speed_path.name
        shutil.copy(src=sound_speed_path,dst=new_ss_path)
        self.sound_speed_path = new_ss_path

    def set_campaign(self, name: str):
        for campaign in self.site.campaigns:
            if campaign.name == name:
                self.current_campaign = campaign
                self.current_campaign_dir = self.working_dir / self.current_campaign.name
                self.current_campaign_dir.mkdir(exist_ok=True)
                self.coord_transformer = CoordTransformer(
                    latitude=self.site.arrayCenter["latitude"],
                    longitude=self.site.arrayCenter["longitude"],
                    elevation=-float(self.site.localGeoidHeight) # use negatiive value to account for garpos error "ys is shallower than layer"
                )
                self.current_survey = None

                return
        raise ValueError(
            f"campaign {name} not found among: {[x.name for x in self.site.campaigns]}"
        )

    def set_survey(self, name: str):
        for survey in self.current_campaign.surveys:
            if survey.survey_id == name:
                self.current_survey = survey
                return
        raise ValueError(
            f"Survey {name} not found among: {[x.survey_id for x in self.current_campaign.surveys]}"
        )

    def get_obsfile_path(self,campaign_name:str,survey_id:str) -> Path:
        obs_path = self.working_dir / campaign_name / survey_id / "observaton.ini"
        # obs_path.parent.mkdir(exist_ok=True,parents=True)
        return obs_path

    def prep_shotdata(self, overwrite: bool = False):
        for campaign in self.site.campaigns:
            self.set_campaign(campaign.name)
            for survey in self.current_campaign.surveys:
                obsfile_path = self.get_obsfile_path(
                    campaign_name=self.current_campaign.name, survey_id=survey.survey_id
                )
                if obsfile_path.exists() and not overwrite:
                    continue
                shot_data_queried: pd.DataFrame = self.shotdata.read_df(
                        start=survey.start, end=survey.end
                    )
                if shot_data_queried.empty:
                    print(
                        f"No shot data found for survey {survey.survey_id}"
                    )
                    continue
                survey_dir = self.current_campaign_dir / survey.survey_id
                survey_dir.mkdir(exist_ok=True)

                benchmarks = []
                for benchmark in self.site.benchmarks:
                    if benchmark.name in survey.benchmarkIDs:
                        benchmarks.append(benchmark)
                GPtransponders = []
                for benchmark in benchmarks:
                    # Find correct transponder, default to first
                    current_transponder = benchmark.transponders[-1]
                    # for transponder in benchmark.transponders:
                    #     if survey.start > transponder.start:
                    #         current_transponder = transponder

                    gp_transponder = GPTransponder(
                        position_llh=GPPositionLLH(
                            latitude=benchmark.aPrioriLocation.latitude,
                            longitude=benchmark.aPrioriLocation.longitude,
                            height=float(benchmark.aPrioriLocation.elevation)
                        ),
                        tat_offset=current_transponder.tat, 
                        id = current_transponder.address,
                        name= benchmark.benchmarkID
                    )
                    # Rectify to local coordinates
                    gp_transponder_enu:Tuple = self.coord_transformer.LLH2ENU(
                        lat=gp_transponder.position_llh.latitude,
                        lon=gp_transponder.position_llh.longitude,
                        hgt=gp_transponder.position_llh.height
                    )
                    gp_transponder_enu = GPPositionENU(
                        east = gp_transponder_enu[0],
                        north=gp_transponder_enu[1],
                        up=gp_transponder_enu[2]
                    )
                    gp_transponder.position_enu = gp_transponder_enu
                    GPtransponders.append(gp_transponder)
                if len(GPtransponders) == 0:
                    print(f"No transponders found for survey {survey.id}")
                    continue

                # Get average transponder position
                _,array_center_llh = avg_transponder_position(GPtransponders)
                array_dpos_center = self.coord_transformer.LLH2ENU(
                    lat=array_center_llh.latitude,
                    lon=array_center_llh.longitude,
                    hgt=array_center_llh.height
                )

                survey_type = survey.type.replace(" ", "")
                start_doy = survey.start.timetuple().tm_yday
                end_doy = survey.end.timetuple().tm_yday

                shot_data_path = (
                    survey_dir
                    / f"{survey.survey_id}_{survey_type}.csv"
                )

                logger.loginfo("Preparing shot data")

                shot_data_rectified = rectify_shotdata(self.coord_transformer,shot_data_queried)
                transponder_ids = [x.id for x in GPtransponders]
                try:
                    shot_data_rectified = ShotDataFrame.validate(
                        shot_data_rectified, lazy=True
                    )
                    # Only use shotdata for transponders in the survey
                    shot_data_rectified = shot_data_rectified[
                        shot_data_rectified.MT.isin([x.id for x in GPtransponders])
                    ]

                    shot_data_rectified.MT = shot_data_rectified.MT.apply(
                        lambda x: "M" + str(x) if str(x)[0].isdigit() else str(x)
                    )

                    shot_data_rectified.to_csv(str(shot_data_path))
                except Exception as e:
                    logger.logerr(
                        f"Shot data for {str(year)}_{str(doy)} failed validation. Error: {e}"
                    )
                    raise ValueError(
                        f"Shot data for {survey.id} {survey_type} {start_doy} {end_doy} failed validation."
                    ) from e 

                logger.loginfo(f"Shot data prepared and saved to {str(shot_data_path)}")
                # get soundspeed relative path
                rel_depth = len(shot_data_path.relative_to(self.sound_speed_path.parent).parts) -1
                ss_path = "../"*rel_depth + self.sound_speed_path.name 
          
                garpos_input = GarposInput(
                    site_name=self.site.names[0],
                    campaign_id=self.current_campaign.name ,
                    survey_id=survey.survey_id ,
                    site_center_llh=GPPositionLLH(
                        latitude=self.site.arrayCenter["latitude"],
                        longitude=self.site.arrayCenter["longitude"],
                        height=float(self.site.localGeoidHeight)
                    ),
                    array_center_enu=GPPositionENU(
                        east=array_dpos_center[0],
                        north=array_dpos_center[1],
                        up=array_dpos_center[2]
                    ),
                    transponders=GPtransponders,
                    atd_offset=GPATDOffset(
                        forward=float(self.vessel_meta.atd_offsets[0].x),
                        rightward=float(self.vessel_meta.atd_offsets[0].y),
                        downward=float(self.vessel_meta.atd_offsets[0].z),
                    ),
                    start_date=survey.start,
                    end_date=survey.end,
                    shot_data="./"+shot_data_path.name,
                    delta_center_position=self.garpos_fixed.inversion_params.delta_center_position,
                    sound_speed_data=ss_path,
                    n_shot=len(shot_data_rectified)
                )
                garpos_input.to_datafile(obsfile_path)
                with open(survey_dir/"survey_meta.json",'w') as file:
                    json.dump(survey.to_dict(),file)

    def set_inversion_params(self, parameters: dict | InversionParams):
        """
        Set inversion parameters for the model.
        This method updates the inversion parameters of the model using the key-value pairs
        provided in the `args` dictionary. Each key in the dictionary corresponds to an attribute
        of the `inversion_params` object, and the associated value is assigned to that attribute.

        Args:
            parameters (dict | InversionParams): A dictionary containing key-value pairs to update the inversion parameters or an InversionParams object.

        """

        if isinstance(parameters, InversionParams):
            self.garpos_fixed.inversion_params = parameters
        else:
            for key, value in parameters.items():
                setattr(self.garpos_fixed.inversion_params, key, value)

    def _run_garpos(
        self,
        obs_file_path:Path,
        results_dir:Path,
        run_id: int | str = 0,
        override: bool = False,
    ) -> None:
        """
        Run the GARPOS model for a given date and run ID.

        Args:
            date (datetime): The date for which to run the GARPOS model.
            run_id (int | str, optional): The run identifier. Defaults to 0.

        Returns:
            GarposResults: The results of the GARPOS model run.

        Raises:
            AssertionError: If the shot data file does not exist for the given date.

        This method performs the following steps:
        1. Extracts the year and day of year (DOY) from the given date.
        2. Constructs the path to the shot data file and checks its existence.
        3. Reads the shot data from the CSV file.
        4. Creates a results directory for the given year and DOY.
        5. Prepares input and settings files for the GARPOS model.
        6. Runs the GARPOS model using the prepared input and settings files.
        7. Processes the GARPOS model results.
        8. Saves the processed results to a JSON file.
        9. Saves the results DataFrame to a CSV file.
        """

        garpos_input = GarposInput.from_datafile(obs_file_path)
        results_path = results_dir / f"_{run_id}_results.json"
        results_df_path: Path = results_dir / f"_{run_id}_results_df.csv"

        if results_path.exists() and not override:
            print(f"Results already exist for {str(results_path)}")
            return

        logger.loginfo(
            f"Running GARPOS model for {garpos_input.campaign_id}, {garpos_input.survey_id}. Run ID: {run_id}"
        )

        results_dir.mkdir(exist_ok=True, parents=True)
        garpos_input.shot_data = results_dir.parent / garpos_input.shot_data.name
        garpos_input.sound_speed_data = obs_file_path.parent.parent.parent / garpos_input.sound_speed_data.name
        input_path = results_dir / f"_{run_id}_observation.ini"
        fixed_path = results_dir / f"_{run_id}_settings.ini"
        garpos_input.to_datafile(input_path)
        self.garpos_fixed._to_datafile(fixed_path)

        print(f"Running GARPOS for {garpos_input.campaign_id}, {garpos_input.survey_id}")
        rf = drive_garpos(
            str(input_path),
            str(fixed_path),
            str(results_dir) + "/",
            garpos_input.campaign_id+"_"+garpos_input.survey_id + f"_{run_id}",
            13,
        )

        results = GarposInput.from_datafile(rf)
        proc_results, results_df = process_garpos_results(results)

        results_df.to_csv(results_df_path, index=False)
        proc_results.shot_data = results_df_path
        with open(results_path, "w") as f:
            json.dump(proc_results.model_dump(), f, indent=4)

    def _run_garpos_survey(
        self, survey_id: str, run_id: int | str = 0, override: bool = False
    ) -> None:
        
        self.set_survey(name=survey_id)
        results_dir = self.current_campaign_dir / survey_id / "results"
        results_dir.mkdir(exist_ok=True, parents=True)
        obsfile_path = self.get_obsfile_path(campaign_name=self.current_campaign.name,survey_id=survey_id)
        if not obsfile_path.exists():
            raise ValueError("Obsfile Not Found")
        
        self._run_garpos(obs_file_path=obsfile_path,results_dir=results_dir,run_id=run_id,override=override)

    def run_garpos(
        self, campaign_id:str,survey_id: str = None, run_id: int | str = 0, override: bool = False
    ) -> None:



        """
        Run the GARPOS model for a specific date or for all dates.
        Args:
            date_index (int, optional): The index of the date in the self.dates list to run the model for.
                                        If None, the model will be run for all dates. Defaults to None.
            run_id (int or str, optional): An identifier for the run. Defaults to 0.
        Returns:
            None
        """
        if campaign_id != self.current_campaign.name:
            self.set_campaign(campaign_id)

        if survey_id is None:
            for survey in self.current_campaign.surveys:
                self._run_garpos_survey(survey.survey_id, run_id, override=override)

        logger.loginfo(f"Running GARPOS model for date(s) provided. Run ID: {run_id}")
        if survey_id is None:
            for survey in self.current_campaign.surveys:
                self._run_garpos_survey(survey.survey_id, run_id, override=override)
        else:
            self._run_garpos_survey(survey_id, run_id, override=override)

    def plot_ts_results(
        self, campaign_name:str = None, survey_id: str= None, run_id: int | str = 0, res_filter: float = 10
    ) -> None:
        """
        Plots the time series results for a given survey.
        Parameters:
        -----------
        survey_id : str
            The ID of the survey to plot results for.
        run_id : int or str, optional
            The run ID of the survey results to plot. Default is 0.
        res_filter : float, optional
            The residual filter value to filter outrageous values (m). Default is 10.
        Returns:
        --------
        None

        Notes:
        ------
        - The function reads survey results from a JSON file and a CSV file.
        - It filters the results based on the residual range.
        - It generates multiple plots including scatter plots, line plots, box plots, and histograms.
        - The plots include information about the delta center position and transponder positions.
        """

        if campaign_name is not None:
            self.set_campaign(campaign_name)
        self.set_survey(survey_id)
        obsfile_path = self.get_obsfile_path(self.current_campaign.name,self.current_survey.survey_id)

        results_dir = obsfile_path.parent / "results"
        results_path = results_dir / f"_{run_id}_results.json"
        with open(results_path, "r") as f:
            results = json.load(f)
            garpos_results = GarposInput(**results)
            arrayinfo = garpos_results.delta_center_position

        results_df_raw = pd.read_csv(garpos_results.shot_data)
        results_df_raw = ShotDataFrame.validate(results_df_raw, lazy=True)
        results_df_raw["time"] = results_df_raw.ST.apply(
            lambda x: datetime.fromtimestamp(x)
        )
        df_filter = results_df_raw["ResiRange"].abs() < res_filter
        results_df = results_df_raw[df_filter]
        unique_ids = results_df["MT"].unique()

        plt.figsize = (32, 32)
        plt.suptitle(f"Survey {survey_id} Results")
        gs = gridspec.GridSpec(13, 16)
        figure_text = "Delta Center Position\n"
        dpos = arrayinfo.get_position()
        figure_text += f"Array :  East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"
        for id, transponder in enumerate(garpos_results.transponders):
            dpos = transponder.delta_center_position.get_position()
            figure_text += f"TSP {transponder.id} : East {dpos[0]:.3f} m, North {dpos[1]:.3f} m, Up {dpos[2]:.3f} m \n"

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
        points = np.array(
            [
                mdates.date2num(results_df["time"]),
                np.zeros(len(results_df["time"])),
            ]
        ).T.reshape(-1, 1, 2)
        segments = np.concatenate([points[:-1], points[1:]], axis=1)
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
            idx = unique_ids.tolist().index(transponder.id)
            ax3.scatter(
                transponder.position_enu.east,
                transponder.position_enu.north,
                label=f"{transponder.id}",
                color=colors[idx],
                s=100,
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
        # Get the 1st and 2nd interquartile range
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
        # Place ax2 x ticks on top
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
        ax4.set_title(
            f"Histogram of Residual Range Values, within {res_filter:.1f} meters"
        )
        ax4.legend()
        plt.show()
