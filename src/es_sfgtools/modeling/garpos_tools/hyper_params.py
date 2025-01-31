"""
Author: Franklyn Dunbar
Date: 2024-05-01
Email: franklyn.dunbar@earthscope.org, franklyn.dunbar@umontana.edu
"""

# External Imports
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator
from es_sfgtools.utils.loggers import GarposLogger as logger


class InversionType(Enum):
    positions = 0  # solve only positions
    gammas = 1  # solve only gammas (sound speed variation)
    both = 2  # solve both positions and gammas


class InversionParams(BaseModel):
    spline_degree: int = Field(default=3)
    log_lambda: List[float] = Field(
        default=[-2], description="Smoothness paramter for backgroun perturbation"
    )
    log_gradlambda: float = Field(
        default=-1, description="Smoothness paramter for spatial gradient"
    )
    mu_t: List[float] = Field(
        default=[0.0],
        description="Correlation length of data for transmit time [minute]",
    )
    mu_mt: List[float] = Field(
        default=[0.5],
        description="Data correlation coefficient b/w the different transponders",
    )

    knotint0: int = Field(
        default=5,
        description="Typical Knot interval (in min.) for gamma's component (a0, a1, a2)",
    )
    knotint1: int = Field(
        default=0,
        description="Typical Knot interval (in min.) for gamma's component (a0, a1, a2)",
    )
    knotint2: int = Field(
        default=0,
        description="Typical Knot interval (in min.) for gamma's component (a0, a1, a2)",
    )
    rejectcriteria: float = Field(
        default=2, description="Criteria for the rejection of data (+/- rsig * Sigma)"
    )
    inversiontype: InversionType = Field(
        default=InversionType(value=0), description="Inversion type"
    )
    positionalOffset: Optional[List[float]] = Field(
        default=[0.0, 0.0, 0.0], description="Positional offset for the inversion"
    )
    traveltimescale: float = Field(
        default=1.0e-5,
        description="Typical measurement error for travel time (= 1.e-4 sec is recommended in 10 kHz carrier)",
    )
    maxloop: int = Field(default=100, description="Maximum loop for iteration")
    convcriteria: float = Field(
        default=5.0e-3, description="Convergence criteria for model parameters"
    )
    deltap: float = Field(
        default=1.0e-6, description="Infinitesimal values to make Jacobian matrix"
    )
    deltab: float = Field(
        default=1.0e-6, description="Infinitesimal values to make Jacobian matrix"
    )
    delta_center_position_east_sigma: float = Field(
        default=0.0, description="Standard deviation of the ECEF East position"
    )
    delta_center_position_north_sigma: float = Field(
        default=0.0, description="Standard deviation of the ECEF North position"
    )
    delta_center_position_up_sigma: float = Field(
        default=0.0, description="Standard deviation of the height above ellipsoid"
    )

    class Config:
        coerce = True

    @model_validator(mode="after")
    def validate(cls, values):
        match values.inversiontype:
            case InversionType.gammas:
                if any([x <= 0 for x in values.positionalOffset]):
                    logger.logerr(
                        "positionalOffset is required for InversionType.positions"
                    )
            case [InversionType.positions, InversionType.both]:
                if any([x > 0 for x in values.positionalOffset]):
                    values.positionalOffset = [0.0, 0.0, 0.0]
                    logger.logerr(
                        "positionalOffset is not required for InversionType.gammas"
                    )

        return values


class InversionLoop(BaseModel):
    iteration: int
    rms_tt: float  # ms
    used_shot_percentage: float
    reject: int
    max_dx: float
    hgt: float
    inv_type: InversionType


class InversionResults(BaseModel):
    ABIC: float
    misfit: float
    inv_type: InversionType
    lambda_0_squared: float
    grad_lambda_squared: float
    mu_t: float  # [s]
    mu_mt: float
    delta_center_position: List[float]
    loop_data: List[InversionLoop]

    @classmethod
    def from_dat_file(cls, file_path: str) -> "InversionResults":
        """
        Parse the inversion results from a .dat file

        Args:
            file_path (str): Path to the .dat file

        Returns:
            InversionResults (obj): Inversion results
        """

        logger.loginfo(f"Reading inversion results from {file_path}")
        with open(file_path, "r") as f:
            lines = f.readlines()
            # Extract data from the file
            loop_data = []
            for line in lines:
                if line.startswith("#Inversion-type"):
                    parsed_line = line.split()
                    iteration = int(parsed_line[4].replace(",", ""))
                    inv_type = InversionType(int(parsed_line[1]))
                    rms_tt = float(parsed_line[7])
                    used_shot_percentage = float(parsed_line[11].replace("%,", ""))
                    reject = int(parsed_line[14].replace(",", ""))
                    max_dx = float(parsed_line[17].replace(",", ""))
                    hgt = float(parsed_line[20])
                    loop_data.append(
                        InversionLoop(
                            iteration=iteration,
                            rms_tt=rms_tt,
                            used_shot_percentage=used_shot_percentage,
                            reject=reject,
                            max_dx=max_dx,
                            hgt=hgt,
                            inv_type=inv_type,
                        )
                    )
                delta_center_position = [0, 0, 0, 0, 0, 0]
                if line.startswith("dcentpos"):
                    parsed_line = line.split()
                    delta_center_position = [float(x) for x in parsed_line[2]]
                if line.startswith("#  ABIC"):
                    parsed_line = line.split()
                    ABIC = float(parsed_line[3])
                    misfit = float(parsed_line[6])
                if line.startswith("# lambda_0^2"):
                    parsed_line = line.split()
                    lambda_0_squared = float(parsed_line[3])
                if line.startswith("# lambda_g^2"):
                    parsed_line = line.split()
                    grad_lambda_squared = float(parsed_line[3])
                if line.startswith("# mu_t"):
                    parsed_line = line.split()
                    mu_t = float(parsed_line[3])
                if line.startswith("# mu_MT"):
                    parsed_line = line.split()
                    mu_mt = float(parsed_line[3])

            logger.loginfo(f"Finished reading inversion results from {file_path}")
            return cls(
                delta_center_position=delta_center_position,
                ABIC=ABIC,
                misfit=misfit,
                inv_type=inv_type,
                lambda_0_squared=lambda_0_squared,
                grad_lambda_squared=grad_lambda_squared,
                mu_t=mu_t,
                mu_mt=mu_mt,
                loop_data=loop_data,
            )
