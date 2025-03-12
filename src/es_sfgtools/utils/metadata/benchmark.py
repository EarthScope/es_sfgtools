from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

from es_sfgtools.utils.metadata.utils import AttributeUpdater, Location,  parse_datetime, check_dates

class Transponder(AttributeUpdater, BaseModel):
    # Required
    address: str
    tat: float
    start: datetime

    # Optional
    end: Optional[datetime] = Field(default=None)
    uid: Optional[str] = Field(default=None)
    model: Optional[str] = Field(default=None)
    serialNumber: Optional[str] = Field(default=None)
    batteryCapacity: Optional[str] = Field(default=None)
    notes: Optional[str] = Field(default=None)
    batteryVoltage: Optional[List[Any]] = Field(default=None)
    extraSensors: Optional[List[Any]] = Field(default=None)


class Benchmark(AttributeUpdater, BaseModel):
    # Required
    name: str
    benchmarkID: str
    dropPointLocation: Location
    aPrioriLocation: Optional[Location] = Field(default=None)
    start: datetime
    end: Optional[datetime] = Field(default=None)
    transponders: List[Transponder] = Field(default_factory=list)

    _parse_datetime = field_validator('start', 'end', mode='before')(parse_datetime)
    _check_dates = field_validator('end')(check_dates)
