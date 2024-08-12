"""
Author: Franklyn Dunbar
Date: 2024-05-01
Email: franklyn.dunbar@earthscope.org, franklyn.dunbar@umontana.edu
"""

from typing import List, Optional
from pydantic import BaseModel,field_validator
import numpy as np
import pandera as pa
from pandera.typing import Series, DataFrame

# from ..utils.schema_utils import llh2xyz
class Point(BaseModel):
    value: Optional[float] = 0.0
    sigma: Optional[float] = 0.0


class PositionENU(BaseModel):
    east: Point = Point()
    north: Point = Point()
    up: Point = Point()
    cov_nu: Optional[float] = 0.0
    cov_ue: Optional[float] = 0.0
    cov_en: Optional[float] = 0.0

    def get_position(self) -> List[float]:
        return [self.east.value, self.north.value, self.up.value]

    def get_std_dev(self) -> List[float]:
        return [self.east.sigma, self.north.sigma, self.up.sigma]

    def get_covariance(self) -> np.ndarray:
        cov_mat = np.diag([self.east.sigma**2, self.north.sigma**2, self.up.sigma**2])
        cov_mat[0, 1] = cov_mat[1, 0] = self.cov_en**2
        cov_mat[0, 2] = cov_mat[2, 0] = self.cov_ue**2
        cov_mat[1, 2] = cov_mat[2, 1] = self.cov_nu**2
        return cov_mat

    def __add__(self, other: "PositionENU") -> "PositionENU":
        return PositionENU(
            east=Point(value=self.east.value + other.east.value, sigma=self.east.sigma),
            north=Point(
                value=self.north.value + other.north.value, sigma=self.north.sigma
            ),
            up=Point(value=self.up.value + other.up.value, sigma=self.up.sigma),
        )

    def __sub__(self, other: "PositionENU") -> "PositionENU":
        return PositionENU(
            east=Point(value=self.east.value - other.east.value, sigma=self.east.sigma),
            north=Point(
                value=self.north.value - other.north.value, sigma=self.north.sigma
            ),
            up=Point(value=self.up.value - other.up.value, sigma=self.up.sigma),
        )

    @classmethod
    def from_list(
        cls, values: List[float], var: Optional[List[float]] = [0.0, 0.0, 0.0]
    ) -> "PositionENU":
        return cls(
            east=Point(value=values[0], sigma=var[0]),
            north=Point(value=values[1], sigma=var[1]),
            up=Point(value=values[2], sigma=var[2]),
        )


class PositionLLH(BaseModel):
    latitude: float
    longitude: float
    height: float

    # def to_ecef(self, var: list = [0, 0, 0]) -> List[float]:
    #     X, Y, Z = llh2xyz(self.latitude, self.longitude, self.height)

    #     pos_enu = PositionENU(
    #         east=Point(value=X, sigma=var[0]),
    #         north=Point(value=Y, sigma=var[1]),
    #         up=Point(value=Z, sigma=var[2]),
    #     )
    #     return pos_enu


class Transponder(BaseModel):
    id: str
    position_enu: Optional[PositionENU] = None
    position_llh: Optional[PositionLLH] = None
    delta_position_enu: Optional[PositionENU] = None
    tat_offset: Optional[float] = 0.0 # Time of arrival offset [s]

    @field_validator("id")
    def validate_id(cls, value):
        if value[0].isdigit():
            return "M" + value
        return value


class ATDOffset(BaseModel):
    forward: Point
    rightward: Point
    downward: Point
    cov_rd: Optional[float] = 0.0
    cov_df: Optional[float] = 0.0
    cov_fr: Optional[float] = 0.0

    def get_offset(self) -> List[float]:
        return [self.forward.value, self.rightward.value, self.downward.value]

    def get_std_dev(self) -> List[float]:
        return [self.forward.sigma, self.rightward.sigma, self.downward.sigma]

    def get_covariance(self) -> np.ndarray:
        cov_mat = np.diag(
            [self.forward.sigma**2, self.rightward.sigma**2, self.downward.sigma**2]
        )
        cov_mat[0, 1] = cov_mat[1, 0] = self.cov_fr**2
        cov_mat[0, 2] = cov_mat[2, 0] = self.cov_df**2
        cov_mat[1, 2] = cov_mat[2, 1] = self.cov_rd**2
        return cov_mat

    @classmethod
    def from_file(cls, file_path: str):
        """
        Read the ATD offset from a "lever_arms" file
        format is [rightward,forward,downward] [m]


        0.0 +0.575 -0.844

        """
        with open(file_path, "r") as f:
            line = f.readlines()[0]
            values = [float(x) for x in line.split()]
            forward = Point(value=values[1])
            rightward = Point(value=values[0])
            downward = Point(value=values[2])

        return cls(forward=forward, rightward=rightward, downward=downward)


class SoundVelocityProfile(pa.DataFrameModel):

    depth: Series[float] = pa.Field(
        ge=0, le=10000, description="Depth of the speed [m]", coerce=True
    )
    speed: Series[float] = pa.Field(
        ge=0, le=3800, description="Spee of sound [m/s]", coerce=True
    )
