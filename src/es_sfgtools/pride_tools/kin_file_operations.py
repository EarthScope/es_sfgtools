# External imports
from datetime import datetime
from typing import List, Optional, Union
import os
import pandas as pd
from pydantic import BaseModel, Field, model_validator, ValidationError
import julian
import matplotlib.pyplot as plt
from pathlib import Path

# Local imports
from ..logging import PRIDELogger as logger


PRIDE_PPP_LOG_INDEX = {
    0: "modified_julian_date",
    1: "second_of_day",
    2: "east",
    3: "north",
    4: "up",
    5: "latitude",
    6: "longitude",
    7: "height",
    8: "number_of_satellites",  # 8 is minimum number of satellites for accurate positioning
    9: "pdop",
}

class PridePPP(BaseModel):
    """
    Data class for PPP GNSS kinematic position output
    Docs: https://github.com/PrideLab/PRIDE-PPPAR
    """

    modified_julian_date: float = Field(ge=0)
    second_of_day: float = Field(ge=0, le=86400)
    east: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF X coordinate
    north: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF Y coordinate
    up: float = Field(
        ge=-6378100,
        le=6378100,
    )  # ECEF Z coordinate
    latitude: float = Field(ge=-90, le=90)  # WGS84 latitude
    longitude: float = Field(ge=0, le=360)  # WGS84 longitude
    height: float = Field(ge=-101, le=100)  # WGS84 height (m)
    number_of_satellites: int = Field(
        default=1, ge=0, le=125
    )  # Average Number of available satellites
    pdop: float = Field(default=0, ge=0, le=1000)  # Position Dilution of Precision
    time: Optional[datetime] = None

    class Config:
        coerce = True

    @model_validator(mode="before")
    def validate_time(cls, values):
        values["pdop"] = float(values.get("pdop", 0.0))
        return values

    @model_validator(mode="after")
    def populate_time(cls, values):
        """Convert from modified julian date and seconds of day to standard datetime format"""
        values = values
        julian_date = (
            values.modified_julian_date + (values.second_of_day / 86400) + 2400000.5
        )
        t = julian.from_jd(julian_date, fmt="jd")
        values.time = t

        return values

    @classmethod
    def from_kin_file(cls, data: List[str]) -> Union["PridePPP", ValidationError]:
        """
        Read kinematic position file and return a DataFrame
        """
        try:
            data_dict = {}
            if "*" in data:
                data.remove("*")

            if len(data) < 10:
                data.insert(-1, 1)  # account for missing number of satellites

            for i, item in enumerate(data):
                field = PRIDE_PPP_LOG_INDEX[i]
                data_dict[field] = item
            return cls(**data_dict)
        except ValidationError as e:
            raise Exception(f"Error parsing into PridePPP {e}")


# read res and caculate wrms
def get_wrms_from_res(res_path):
    with open(res_path, "r") as res_file:
        timestamps = []
        data = []
        wrms = 0
        sumOfSquares = 0
        sumOfWeights = 0
        line = res_file.readline()  # first line is header and we can throw away
        while True:
            if line == "":  # break at EOF
                break
            line_data = line.split()
            if line_data[0] == "TIM":  # for a given epoch
                sumOfSquares = 0
                sumOfWeights = 0
                # parse date fields and make a timestamp
                seconds = float(line_data[6])
                SS = int(seconds)
                f = str(seconds - SS).split(".")[-1]
                isodate = f"{line_data[1]}-{line_data[2].zfill(2)}-{line_data[3].zfill(2)}T{line_data[4].zfill(2)}:{line_data[5].zfill(2)}:{str(SS).zfill(2)}.{str(f).zfill(6)}"
                timestamp = datetime.fromisoformat(isodate)
                timestamps.append(timestamp)
                # loop through SV data for that epoch, stop at next timestamp
                line = res_file.readline()
                line_data = line.split()
                while not line.startswith("TIM"):
                    phase_residual = float(line_data[1])
                    phase_weight = float(line_data[3].replace("D", "E"))
                    sumOfSquares += phase_residual**2 * phase_weight
                    sumOfWeights += phase_weight
                    line = res_file.readline()
                    if line == "":
                        break
                    line_data = line.split()
                wrms = (sumOfSquares / sumOfWeights) ** 0.5 * 1000  # in mm
                data.append(wrms)
            else:
                line = res_file.readline()
    wrms_df = pd.DataFrame({"date": timestamps, "wrms": data})
    return wrms_df


def plot_kin_results_wrms(kin_df, title=None, save_as=None):
    size = 3
    bad_nsat = kin_df[kin_df["Nsat"] <= 4]
    bad_pdop = kin_df[kin_df["PDOP"] >= 5]
    fig, axs = plt.subplots(
        6,
        1,
        figsize=(10, 10),
        sharex=True,
    )
    axs[0].scatter(kin_df.index, kin_df["Latitude"], s=size)
    axs[0].set_ylabel("Latitude")
    axs[1].scatter(kin_df.index, kin_df["Longitude"], s=size)
    axs[1].set_ylabel("Longitude")
    axs[2].scatter(kin_df.index, kin_df["Height"], s=size)
    axs[2].set_ylabel("Height")
    axs[3].scatter(kin_df.index, kin_df["Nsat"], s=size)
    axs[3].scatter(bad_nsat.index, bad_nsat["Nsat"], s=size * 2, color="red")
    axs[3].set_ylabel("Nsat")
    axs[4].scatter(kin_df.index, kin_df["PDOP"], s=size)
    axs[4].scatter(bad_pdop.index, bad_pdop["PDOP"], s=size * 2, color="red")
    axs[4].set_ylabel("log PDOP")
    axs[4].set_yscale("log")
    axs[4].set_ylim(1, 100)
    axs[5].scatter(kin_df.index, kin_df["wrms"], s=size)
    axs[5].set_ylabel("wrms mm")
    axs[0].ticklabel_format(axis="y", useOffset=False, style="plain")
    axs[1].ticklabel_format(axis="y", useOffset=False, style="plain")
    for ax in axs:
        ax.grid(True, c="lightgrey", zorder=0, lw=1, ls=":")
    plt.xticks(rotation=70)
    fig.suptitle(f"PRIDE-PPPAR results for {os.path.basename(title)}")
    fig.tight_layout()
    if save_as is not None:
        plt.savefig(save_as)


def kin_to_gnssdf(source: str|Path) -> Union[pd.DataFrame, None]:
    """
    Create an GNSSDataFrame from a kin file from PRIDE-PPP

    Parameters:
        file_path (str): The path to the kin file

    Returns:
        dataframe (GNSSDataFrame): An instance of the class.
    """
  
    with open(source, "r") as file:
        lines = file.readlines()

    end_header_index = next(
        (i for i, line in enumerate(lines) if line.strip() == "END OF HEADER"), None
    )

    # Read data from lines after the end of the header
    data = []
    if end_header_index is None:
        logger.logerr(f"GNSS: No header found in FILE {str(source)}")
        return None
    for idx, line in enumerate(lines[end_header_index + 2 :]):
        split_line = line.strip().split()
        selected_columns = split_line[:9] + [
            split_line[-1]
        ]  # Ignore varying satellite numbers
        try:
            ppp: Union[PridePPP, ValidationError] = PridePPP.from_kin_file(
                selected_columns
            )
            data.append(ppp)
        except:
            pass

    # Check if data is empty
    if not data:
        logger.logerr(f"GNSS: No data found in FILE {source.local_path}")
        return None

    # TODO convert lat/long to ecef
    dataframe = pd.DataFrame([dict(pride_ppp) for pride_ppp in data])
    # dataframe.drop(columns=["modified_julian_date", "second_of_day"], inplace=True)

    logger.loginfo(
        f"GNSS Parser: {dataframe.shape[0]} shots from FILE {source.local_path}"
    )
    dataframe["time"] = dataframe["time"].dt.tz_localize("UTC")
    return dataframe.drop(columns=["modified_julian_date", "second_of_day"])


def read_kin_data(kin_path):
    with open(kin_path, "r") as kin_file:
        for i, line in enumerate(kin_file):
            if "END OF HEADER" in line:
                end_of_header = i + 1
                break
    cols = [
        "Mjd",
        "Sod",
        "*",
        "X",
        "Y",
        "Z",
        "Latitude",
        "Longitude",
        "Height",
        "Nsat",
        "G",
        "R",
        "E",
        "C2",
        "C3",
        "J",
        "PDOP",
    ]
    colspecs = [
        (0, 6),
        (6, 16),
        (16, 18),
        (18, 32),
        (32, 46),
        (46, 60),
        (60, 77),
        (77, 94),
        (94, 108),
        (108, 114),
        (114, 117),
        (117, 120),
        (120, 123),
        (123, 126),
        (126, 129),
        (129, 132),
        (132, 140),
    ]
    kin_df = pd.read_fwf(
        kin_path,
        header=end_of_header,
        colspecs=colspecs,
        names=cols,
        on_bad_lines="skip",
    )
    # kin_df = pd.read_csv(kin_path, sep="\s+", names=cols, header=end_of_header, on_bad_lines='skip')
    kin_df.set_index(
        pd.to_datetime(kin_df["Mjd"] + 2400000.5, unit="D", origin="julian")
        + pd.to_timedelta(kin_df["Sod"], unit="sec"),
        inplace=True,
    )
    return kin_df
