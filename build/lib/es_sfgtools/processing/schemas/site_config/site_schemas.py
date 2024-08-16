from pydantic import BaseModel
from typing import Optional,List
import numpy as np

class PositionLLH(BaseModel):
    latitude: float
    longitude: float
    height: float

class PositionENU(BaseModel):
    east: Optional[float] = 0
    north: Optional[float] = 0
    up: Optional[float] = 0
    east_sigma: Optional[float] = 0
    north_sigma: Optional[float] = 0
    up_sigma: Optional[float] = 0
    cov_nu: Optional[float] = 0
    cov_ue: Optional[float] = 0
    cov_en: Optional[float] = 0

    def get_position(self) -> List[float]:
        return [self.east, self.north, self.up]

    def get_std_dev(self) -> List[float]:
        return [self.east_sigma, self.north_sigma, self.up_sigma]

    def get_covariance(self) -> np.ndarray:
        cov_mat = np.diag([self.east.sigma**2, self.north.sigma**2, self.up.sigma**2])
        cov_mat[0, 1] = cov_mat[1, 0] = self.cov_en**2
        cov_mat[0, 2] = cov_mat[2, 0] = self.cov_ue**2
        cov_mat[1, 2] = cov_mat[2, 1] = self.cov_nu**2
        return cov_mat


class Transponder(BaseModel):
    position_llh: Optional[PositionLLH] = None
    position_enu: Optional[PositionENU] = None
    tat_offset: Optional[float] = None
    name: Optional[str] = None
    id: Optional[str] = None
    campaign_id: Optional[str] = None
    site_id: Optional[str] = None
    delta_center_position: Optional[PositionENU] = None

class SiteConfig(BaseModel):
    position_llh: PositionLLH
    transponders: Optional[List[Transponder]]
    name: Optional[str] = None
    id: Optional[str] = None
    campaign_id: Optional[str] = None

class ATDOffset(BaseModel):
    forward: float
    rightward: float
    downward: float
    def get_offset(self) -> List[float]:
        return [self.forward, self.rightward, self.downward]

