from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

from .utils import (
    AttributeUpdater,
    Location,
    check_fields_for_empty_strings,
    parse_datetime,
    check_dates,
)


class ExtraSensors(AttributeUpdater, BaseModel):
    # Required
    type: str = Field(..., description="The type of the extra sensor")
    serialNumber: str = Field(..., description="The serial number of the extra sensor")
    model: str = Field(..., description="The model of the extra sensor")


class BatteryVoltage(AttributeUpdater, BaseModel):
    # Required
    date: datetime = Field(
        ...,
        description="The date of the battery voltage reading",
        ge=datetime(1901, 1, 1),
    )
    voltage: float = Field(..., description="The battery voltage reading", ge=0, le=20)

    _parse_datetime = field_validator("date", mode="before")(parse_datetime)


class TAT(AttributeUpdater, BaseModel):
    # Required
    value: float = Field(..., description="Turn around time (TAT) in ms", ge=0, le=1000)

    # Optional
    timeIntervals: Optional[List[Dict[str, Union[str, datetime, None]]]] = Field(
        default_factory=list,
        description="List of time intervals with start and end times for TAT",
    )

    @field_validator("timeIntervals", mode="before")
    def validate_time_intervals(cls, time_intervals):
        for interval in time_intervals:
            start = interval.get("start")
            end = interval.get("end")

            # Parse start and end times if they exist
            if start:
                interval["start"] = parse_datetime(cls, start)
            if end:
                interval["end"] = parse_datetime(cls, end)

            # Validate that start is before end
            if start and end and start >= end:
                raise ValueError(
                    "'end' time must be after 'start' time in each interval"
                )

        return time_intervals


class Transponder(AttributeUpdater, BaseModel):
    # Required
    address: str = Field(..., description="The address of the transponder")
    tat: List[TAT] = Field(
        ..., description="The turn around time (TAT) of the transponder"
    )
    start: datetime = Field(
        ..., description="The start date of the transponder", gt=datetime(1901, 1, 1)
    )

    # Optional
    end: Optional[datetime] = Field(
        default=None,
        description="The end date of the transponder (if removed)",
        gt=datetime(1901, 1, 1),
    )  # TODO Check if this is works (maybe don't need to check if end is after start)
    uid: Optional[str] = Field(default=None, description="The UID of the transponder")
    model: Optional[str] = Field(
        default=None, description="The model of the transponder"
    )
    serialNumber: Optional[str] = Field(
        default=None, description="The serial number of the transponder"
    )
    batteryCapacity: Optional[str] = Field(
        default=None, description="The battery capacity of the transponder, e.g 4 Ah"
    )
    notes: Optional[str] = Field(
        default=None, description="Additional notes about the transponder or deployment"
    )
    batteryVoltage: Optional[List[BatteryVoltage]] = Field(
        default_factory=list,
        description="The battery voltage of the transponder, including date and voltage",
    )
    extraSensors: Optional[List[ExtraSensors]] = Field(
        default_factory=list, description="Extra sensors attached to the transponder"
    )

    _check_for_empty_strings = field_validator(
        "uid", "model", "serialNumber", "batteryCapacity", "notes"
    )(check_fields_for_empty_strings)
    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end")(check_dates)

    def get_tat_by_datetime(self, dt: datetime) -> Optional[TAT]:

        # If there is only 1 TAT available, return that TAT
        if len(self.tat) == 1:
            return self.tat[0].value

        # If there are multiple TATs, check if the datetime is within the time intervals
        for tat in self.tat:
            for interval in tat.timeIntervals:
                if interval["start"] <= dt <= interval["end"]:
                    return tat.value

        return None


class Benchmark(AttributeUpdater, BaseModel):
    # Required
    name: str = Field(..., description="The name of the benchmark")
    benchmarkID: Optional[str] = Field("", description="The benchmark ID")
    aPrioriLocation: Optional[Location] = Field(
       None, description="The a priori location of the benchmark"
    )
    start: Optional[datetime] = Field(
        None, description="The start date of the benchmark", gt=datetime(1901, 1, 1)
    )

    # Optional
    end: Optional[datetime] = Field(
        default=None,
        description="The end date of the benchmark",
        gt=datetime(1901, 1, 1),
    )
    dropPointLocation: Optional[Location] = Field(
        default=None, description="The drop point location of the benchmark"
    )
    transponders: Optional[List[Transponder]] = Field(
        default_factory=list, description="The transponders attached to the benchmark"
    )

    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end")(check_dates)

    def get_transponder_by_datetime(self,  dt: datetime) -> Optional[Transponder]:
        # If there is only 1 transponder available, return that transponder
        if len(self.transponders) == 1:
            return self.transponders[0]

        # If there are multiple transponders, check if the datetime is within the start and end dates
        for transponder in self.transponders:
            if transponder.start <= dt:
                if transponder.end is None or transponder.end >= dt:
                    return transponder
        return None