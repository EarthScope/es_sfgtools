from pydantic import BaseModel, Field,model_validator,model_serializer,AliasChoices,field_validator
from typing import Optional,List,Union,Dict
from pathlib import Path
import numpy as np
import datetime
import logging
import yaml

logger = logging.getLogger(__name__)

class GPPositionLLH(BaseModel):
    latitude: float
    longitude: float
    height: Optional[float] = 0

class GPPositionENU(BaseModel):
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


class GPTransponder(BaseModel):
    position_llh: Optional[GPPositionLLH] = None
    position_enu: Optional[GPPositionENU] = None
    tat_offset: Optional[float] = None
    name: Optional[str] = None
    id: Optional[str] = Field(None, alias=AliasChoices("id","address"))
    delta_center_position: Optional[GPPositionENU] = None
    class Config:
        allow_population_by_field_name = True


class GPATDOffset(BaseModel):
    forward: float
    rightward: float
    downward: float
    def get_offset(self) -> List[float]:
        return [self.forward, self.rightward, self.downward]


class GPSiteConfig(BaseModel):
    campaign: Optional[str] = None
    date: Optional[datetime.datetime] = None
    position_llh: GPPositionLLH
    transponders: Optional[List[GPTransponder]]
    sound_speed_data: Optional[str] = None
    atd_offset: Optional[GPATDOffset] = None
    delta_center_position: Optional[GPPositionENU] = GPPositionENU()

    @classmethod
    def from_config(cls, config_file: Union[str, Path]) -> "GPSiteConfig":
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        name = config["site_id"]
        campaign = config["campaign"]
        date = config["time_origin"]
        array_center = config["array_center"]
        height = -1*config["solver"]["geoid_undulation"]
        position_llh = GPPositionLLH(latitude=array_center["lat"], longitude=array_center["lon"], height=height)
        transponder_set = []
        for transponder in config["transponders"]:
            transponder_position = GPPositionLLH(
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
            transponder_set.append(GPTransponder(
                position_llh=transponder_position,
                tat_offset=tat_offset,
                id=id,
                name=name
            ))
        atd_offset_dict = config["posfilter"]["atd_offsets"]
        atd_offset = GPATDOffset(
            forward=atd_offset_dict["forward"],
            rightward=atd_offset_dict["rightward"],
            downward=atd_offset_dict["downward"],
        )

        return GPSiteConfig(
            name=name,
            campaign=campaign,
            date=date,
            position_llh=position_llh,
            transponders=transponder_set,
            atd_offset=atd_offset
        )


class Benchmark(BaseModel):
    name: str
    benchmarkID: str = None
    dropPointLocation: GPPositionLLH = None
    aPrioriLocation: GPPositionLLH = None
    transponders: List[GPTransponder] = []
    start: datetime.datetime = None

class Vessel(BaseModel):
    name: str
    type: str
    serialNumber: str = None
    start : datetime.datetime = None
    atd_offset: GPATDOffset = None

class Survey(BaseModel):
    id: str = None
    type: str = None 
    vesselName: str = None
    benchmarkIDs: List[str] = []
    start: datetime.datetime
    end: datetime.datetime
    shot_data_path: str|Path = None

    @model_serializer
    def to_dict(self):
        dict_ = self.__dict__.copy()
        if isinstance(self.start,datetime.datetime):
            dict_["start"] = self.start.isoformat() 
        if isinstance(self.end,datetime.datetime):
            dict_["end"] = self.end.isoformat()
        dict_["shot_data_path"] = str(self.shot_data_path)
        return dict_

    @field_validator("start","end",mode='before')
    def validate_times(cls,v):
        
        if isinstance(v,str):
            return datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S")
        return v

    class Config:
        allow_arbitraty_types = True
    # @model_validator(mode='after')
    # def load_times(self):
    #     self.start = datetime.datetime.fromisoformat(self.start)
    #     self.end = datetime.datetime.fromisoformat(self.end)
    #     return self


class Campaign(BaseModel):
    name : str = "Campaign"
    type: str  = "Campaign Type"
    launchVesselName: str = None
    recoveryVesselName: str = None
    principalInvestigator: str = None   
    cruiseName: str = None
    surveys:Union[List[Survey] | Dict[str,Survey]] = []

    @model_validator(mode='after')
    def validate_surveys(self):
        if isinstance(self.surveys,list):
            self.surveys = {survey.id:survey for survey in self.surveys}
        return self
    @field_validator("surveys")
    def validate_surveys_pre(cls,v):
        if isinstance(v,list):
            return [survey for survey in v]
        return v
    
class Site(BaseModel):
    name: str = "Site"
    networks: str = "Networks"
    timeOrigin: datetime.datetime = None
    arrayCenter: GPPositionLLH = {}
    localGeoidHeight: float = 0
    benchmarks: List[Benchmark] = []
    campaigns: List[Campaign] = []
    surveyVessels: List[Vessel] = []

    @classmethod
    def from_json(cls,path:Path) -> "Site":
        with open(path,"r") as f:
            site = yaml.safe_load(f)
        name = site["names"][0]
        networks = site["networks"][0]
        timeOrigin = datetime.datetime.fromisoformat(site["timeOrigin"])
        arrayCenter = site["arrayCenter"]
        localGeoidHeight = site["localGeoidHeight"]
        benchmarks = []
        for benchmark_dict in site["benchmarks"]:
            benchmark = Benchmark(**benchmark_dict)
            benchmarks.append(benchmark)
        campaigns = []
        for campaign_dict in site["campaigns"]:
            campaign = Campaign(**campaign_dict)
            campaigns.append(campaign)
        surveyVessels = []
        for vessel_dict in site["surveyVessels"]:
            vessel = Vessel(**vessel_dict)
            surveyVessels.append(vessel)
        return Site(
            name=name,
            networks=networks,
            timeOrigin=timeOrigin,
            localGeoidHeight=localGeoidHeight,
            benchmarks=benchmarks,
            campaigns=campaigns,
            surveyVessels=surveyVessels
        )

