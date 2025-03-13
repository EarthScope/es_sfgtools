from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

from es_sfgtools.utils.metadata.utils import AttributeUpdater, check_optional_fields_for_empty_strings, parse_datetime, check_dates, if_zero_than_none


class ExtraSensors(AttributeUpdater, BaseModel):
    # Required
    type: str = Field(..., description="The type of the extra sensor")
    serialNumber: str = Field(..., description="The serial number of the extra sensor")
    model: str = Field(..., description="The model of the extra sensor")

class BatteryVoltage(AttributeUpdater, BaseModel):
    # Required
    date: datetime = Field(..., description="The date of the battery voltage reading",
                           ge=datetime(1901, 1, 1))
    voltage: float = Field(..., description="The battery voltage reading",
                           ge=0,
                           le=20)

    _parse_datetime = field_validator('date', mode='before')(parse_datetime)


class Location(AttributeUpdater, BaseModel):
    latitude: Optional[float] = Field(default=None, 
                                      description="The latitude of the location.",
                                      ge=-90, 
                                      le=90)
    longitude: Optional[float] = Field(default=None, 
                                       description="The longitude of the location.",
                                       ge=-180,
                                       le=180)
    elevation: Optional[float] = Field(default=None, 
                                       description="The elevation of the location.")
    
    _if_zero_than_none = field_validator('latitude', 'longitude', 'elevation')(if_zero_than_none)


class Transponder(AttributeUpdater, BaseModel):
    # Required
    address: str = Field(..., description="The address of the transponder")
    tat: float = Field(..., description="TAS in ms",
                       gt=0)
    start: datetime = Field(..., description="The start date of the transponder",
                            gt=datetime(1901, 1, 1))

    # Optional
    end: Optional[datetime] = Field(default=None, 
                                    description="The end date of the transponder (if removed)",
                                    gt=start) # TODO Check if this is works (maybe don't need to check if end is after start)
    uid: Optional[str] = Field(default=None, description="The UID of the transponder")
    model: Optional[str] = Field(default=None, description="The model of the transponder")
    serialNumber: Optional[str] = Field(default=None, description="The serial number of the transponder")
    batteryCapacity: Optional[str] = Field(default=None, description="The battery capacity of the transponder, e.g 4 Ah")
    notes: Optional[str] = Field(default=None, description="Additional notes about the transponder or deployment")
    batteryVoltage: Optional[List[BatteryVoltage]] = Field(default_factory=list, description="The battery voltage of the transponder, including date and voltage")
    extraSensors: Optional[List[ExtraSensors]] = Field(default_factory=list, description="Extra sensors attached to the transponder")

    _check_for_empty_strings = field_validator('uid', 'model', 'serialNumber', 'batteryCapacity', 'notes')(check_optional_fields_for_empty_strings)

class Benchmark(AttributeUpdater, BaseModel):
    # Required
    name: str = Field(..., description="The name of the benchmark")
    benchmarkID: str = Field(..., description="The benchmark ID")
    aPrioriLocation: Location = Field(default=None, description="The a priori location of the benchmark")
    start: datetime = Field(..., description="The start date of the benchmark",
                            gt=datetime(1901, 1, 1))

    # Optional
    end: Optional[datetime] = Field(default=None, description="The end date of the benchmark",
                                    gt=start)
    dropPointLocation: Optional[Location] = Field(default=None, description="The drop point location of the benchmark")
    transponders: Optional[List[Transponder]] = Field(default_factory=list, description="The transponders attached to the benchmark")

    _parse_datetime = field_validator('start', 'end', mode='before')(parse_datetime)
    _check_dates = field_validator('end')(check_dates)

