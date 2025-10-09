import os
from enum import Enum
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field


# define system enum to use
class Constellations(str, Enum):
    GPS = "G"
    GLONASS = "R"
    GLAILEO = "E"
    BDS = "C"
    BDS_TWO = "2"
    BDS_THREE = "3"
    QZSS = "J"

    @classmethod
    def print_options(cls):
        print("System options are:")
        for option in cls:
            print(f"{option.value} for {option.name}")


class Tides(str, Enum):
    SOLID = "S"
    OCEAN = "O"
    POLAR = "P"

    @classmethod
    def print_options(cls):
        print("Tide options are:")
        for option in cls:
            print(f"{option.value} for {option.name}")


class PrideCLIConfig(BaseModel):
    """
    PrideCLIConfig is a configuration class for setting up and generating commands to run the pdp3 GNSS processing tool.
    Attributes:
        system (str): The GNSS system(s) to use. Default is "GREC23J" which is “GPS/GLONASS/Galileo/BDS/BDS-2/BDS-3/QZSS”.
        frequency (list): The GNSS frequencies to use. Default is ["G12", "R12", "E15", "C26", "J12"]. Refer to Table 5-4 in PRIDE-PPP-AR v.3.0 manual for more options.
        loose_edit (bool): Disable strict editing mode, which should be used when high dynamic data quality is poor. Default is True.
        cutoff_elevation (int): The elevation cutoff angle in degrees (0-60 degrees). Default is 7.
        interval (float): Processing interval, values range from 0.02s to 30s. If this item is not specified and the configuration file is specified, the processing interval in the configuration file will be read, otherwise, the sampling rate of the observation file is used by default.
        high_ion (bool): Use 2nd ionospheric delay model with CODE's GIM product. When this option is not entered, no higher-order ionospheric correction is performed. Default is False.
        tides (str): Enter one or more of "S" "O" "P", e.g SO for solid, ocean, and polar tides. Default is "SOP", which uses all tides.
        local_pdp3_path (str): The path to the local pdp3 binary. Default is None.
    Methods:
        generate_pdp_command(site: str, local_file_path: str) -> List[str]:
            Generate the command to run pdp3 with the given parameters.
                site (str): The site identifier for the GNSS data.
                local_file_path (str): The local file path to the GNSS data.
            Returns:
                List[str]: The command to run pdp3 with the specified configuration.
    """

    sample_frequency: float = 1
    system: str = "GREC23J"
    frequency: list = ["G12", "R12", "E15", "C26", "J12"]
    loose_edit: bool = True
    cutoff_elevation: int = 7
    interval: Optional[float] = None
    high_ion: Optional[bool] = None
    tides: str = "SOP"
    local_pdp3_path: Optional[str] = Field(
        None,
        title="Local Path to pdp3 Binary",
        description="Path to the local pdp3 binary. If not provided, the system PATH will be used.",
    )
    override: bool = False
    override_products_download: bool = Field(
        False, title="Flag to Override Existing Products Download"
    )
    pride_configfile_path: Optional[Path] = Field(
        None,
        title="Path to Pride Config File",
        description="Path to the Pride config file. If not provided, the default config will be used.",
    )

    def __post_init__(self):
        # Check if system is valid
        system = (
            self.system.upper()
        )  # Default to GREC23J which is “GPS/GLONASS/Galileo/BDS/BDS-2/BDS-3/QZSS”
        for char in system:
            if char not in Constellations._value2member_map_:
                Constellations.print_options()
                raise ValueError(f"Invalid constelation character: {char}")

        # If entered, check if tide characters are valid
        tides = self.tides.upper()
        for char in tides:
            if char not in Tides._value2member_map_:
                Tides.print_options()
                raise ValueError(f"Invalid tide character: {char}")

    def generate_pdp_command(
        self, site: str, local_file_path: str
    ) -> List[str]:
        """
        Generate the command to run pdp3 with the given parameters
        """

        if self.local_pdp3_path:
            if "pdp3" in self.local_pdp3_path:
                command = [self.local_pdp3_path]
            else:
                command = [os.path.join(self.local_pdp3_path, "pdp3")]
        else:
            command = ["pdp3"]

        command.extend(["-m", "K"])

        command.extend(["-i", str(self.sample_frequency)])

        if self.system != "GREC23J":
            command.extend(["--system", self.system])

        if self.frequency != ["G12", "R12", "E15", "C26", "J12"]:
            command.extend(["--frequency", " ".join(self.frequency)])

        if self.loose_edit:
            command.append("--loose-edit")

        if self.cutoff_elevation != 7:
            command.extend(["--cutoff-elev", str(self.cutoff_elevation)])

        if self.interval:
            command.extend(["--interval", str(self.interval)])

        if self.high_ion:
            command.append("--high-ion")

        if self.tides != "SOP":
            command.extend(["--tide-off", self.tides])

        command.extend(["--site", site])

        if self.pride_configfile_path:
            command.extend(["--config", str(self.pride_configfile_path)])

        command.append(str(local_file_path))

        return command
