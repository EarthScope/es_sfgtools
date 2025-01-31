from pydantic import BaseModel
from typing import Optional,List,Union
from pathlib import Path
import numpy as np
import datetime
import logging
import yaml

logger = logging.getLogger(__name__)

class PositionLLH(BaseModel):
    latitude: float
    longitude: float
    height: Optional[float] = 0

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
        cov_mat = np.diag([self.east_sigma**2, self.north_sigma**2, self.up_sigma**2])
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
    delta_center_position: Optional[PositionENU] = None


class ATDOffset(BaseModel):
    forward: float
    rightward: float
    downward: float
    def get_offset(self) -> List[float]:
        return [self.forward, self.rightward, self.downward]


class SiteConfig(BaseModel):
    name: Optional[str] = None
    campaign: Optional[str] = None
    date: Optional[datetime.datetime] = None
    position_llh: PositionLLH
    transponders: Optional[List[Transponder]]
    sound_speed_data: Optional[str] = None
    atd_offset: Optional[ATDOffset] = None
    delta_center_position: Optional[PositionENU] = PositionENU()

    @classmethod
    def from_config(cls, config_file: Union[str, Path]) -> "SiteConfig":
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        name = config["site_id"]
        campaign = config["campaign"]
        date = config["time_origin"]
        array_center = config["array_center"]
        height = -1*config["solver"]["geoid_undulation"]
        position_llh = PositionLLH(latitude=array_center["lat"], longitude=array_center["lon"], height=height)
        transponder_set = []
        for transponder in config["transponders"]:
            transponder_position = PositionLLH(
                latitude=transponder["lat"],
                longitude=transponder["lon"],
                height=transponder["height"]
            )
            tat_offset = transponder["sv_mean"]
            name = transponder["pxp_id"]
            match name.split("-")[1]:
                case "1":
                    id = "5209"
                case "2":
                    id = "5210"
                case "3":
                    id = "5211"
                case "4":
                    id = "5212"
            transponder_set.append(Transponder(
                position_llh=transponder_position,
                tat_offset=tat_offset,
                id=id,
                name=name
            ))
        atd_offset_dict = config["posfilter"]["atd_offsets"]
        atd_offset = ATDOffset(
            forward=atd_offset_dict["forward"],
            rightward=atd_offset_dict["rightward"],
            downward=atd_offset_dict["downward"],
        )

        return SiteConfig(
            name=name,
            campaign=campaign,
            date=date,
            position_llh=position_llh,
            transponders=transponder_set,
            atd_offset=atd_offset
        )



