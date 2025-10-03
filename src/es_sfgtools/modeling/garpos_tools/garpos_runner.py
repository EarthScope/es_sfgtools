"""
This module contains the GarposRunner class, which is responsible for running the GARPOS model.
"""

from pathlib import Path
import json
import pandas as pd
from scipy.stats import hmean as harmonic_mean

from es_sfgtools.modeling.garpos_tools.schemas import GarposInput, GarposFixed
from es_sfgtools.logging import GarposLogger as logger

try:
    from garpos import drive_garpos
except ImportError:
    # Handle the case where garpos is not available
    pass

class GarposRunner:
    """
    A class to run the GARPOS model.
    """

    def __init__(self, garpos_fixed: GarposFixed, sound_speed_path: Path):
        """
        Initializes the GarposRunner.

        Args:
            garpos_fixed (GarposFixed): The fixed parameters for the GARPOS model.
            sound_speed_path (Path): The path to the sound speed profile file.
        """
        self.garpos_fixed = garpos_fixed
        self.sound_speed_path = sound_speed_path

    def run(
        self,
        obs_file_path: Path,
        results_dir: Path,
        run_id: int | str = 0,
        override: bool = False,
    ) -> Path:
        """
        Run the GARPOS model for a given observation file.

        Args:
            obs_file_path (Path): The path to the observation file.
            results_dir (Path): The directory to store the results.
            run_id (int | str, optional): The run identifier. Defaults to 0.
            override (bool, optional): If True, override existing results. Defaults to False.

        Returns:
            Path: The path to the GARPOS model results.
        """
        garpos_input = GarposInput.from_datafile(obs_file_path)
        results_path = results_dir / f"_{run_id}_results.json"
        results_df_path: Path = results_dir / f"_{run_id}_results_df.csv"

        if results_path.exists() and not override:
            print(f"Results already exist for {str(results_path)}")
            return None

        logger.loginfo(
            f"Running GARPOS model for {garpos_input.site_name}, {garpos_input.survey_id}. Run ID: {run_id}"
        )

        results_dir.mkdir(exist_ok=True, parents=True)
        garpos_input.sound_speed_data = self.sound_speed_path
        input_path = results_dir / f"_{run_id}_observation.ini"
        fixed_path = results_dir / f"_{run_id}_settings.ini"
        self.garpos_fixed._to_datafile(fixed_path)
        garpos_input.to_datafile(input_path)

        rf = drive_garpos(
            str(input_path),
            str(fixed_path),
            str(results_dir) + "/",
            f"{garpos_input.survey_id}_{run_id}",
            13,
        )

        results = GarposInput.from_datafile(rf)

        svp_df = pd.read_csv(results.sound_speed_data)
        results_df = pd.read_csv(results.shot_data, skiprows=1)
        speed_mean = harmonic_mean(svp_df.speed.values)
        range_residuals = results_df.ResiTT.values * speed_mean / 2
        results_df["ResiRange"] = range_residuals

        proc_results = results

        results_df.to_csv(results_df_path, index=False)
        proc_results.shot_data = results_df_path
        with open(results_path, "w") as f:
            json.dump(proc_results.model_dump(), f, indent=4)

        return rf
