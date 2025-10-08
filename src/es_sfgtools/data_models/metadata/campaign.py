from datetime import datetime
from typing import List, Optional
from .utils import (
    AttributeUpdater,
    check_dates,
    check_fields_for_empty_strings,
    parse_datetime,
)
from .vessel import Vessel
from pydantic import BaseModel, Field, field_validator,model_serializer


def campaign_checks(campaign_year, campaign_interval, vessel_code):
    """Check the campaign year, interval and vessel code

    Args:
        campaign_year (str): The campaign year.
        campaign_interval (str): The campaign interval.
        vessel_code (str): The vessel code.

    Returns:
        str: The campaign name.
        str: The vessel code.
    """
    if not campaign_year.isnumeric() or not len(campaign_year) == 4:
        raise ValueError("Campaign year must be a 4 digit year")
    if not campaign_interval.isalpha() or not len(campaign_interval) == 1:
        raise ValueError("Campaign interval must be a single letter")
    if not len(vessel_code) == 4:
        raise ValueError("Vessel code must be a 4 digit/letter code")

    campaign_name = (
        campaign_year + "_" + campaign_interval.upper() + "_" + vessel_code.upper()
    )

    print("Campaign name: " + campaign_name)
    return campaign_name, vessel_code.upper()


class Survey(AttributeUpdater, BaseModel):
    # Required
    id: str = Field(
        ..., description="The unique ID of the survey"
    )  # Todo generate this
    type: str = Field(
        ..., description="The type of the survey (e.g. circle | fixed point | mixed)"
    )
    benchmarkIDs: List[str] = Field(
        ..., description="Benchmark IDs associated with the survey"
    )
    start: datetime = Field(
        ..., description="The start date & time of the survey", gt=datetime(1901, 1, 1)
    )
    end: datetime = Field(
        ..., description="The end date & time of the survey", gt=datetime(1901, 1, 1)
    )

    # Optional
    notes: Optional[str] = Field(
        default=None, description="Any additional notes about the survey"
    )
    commands: Optional[str] = Field(default=None, description="Log of commands")

    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end", mode="after")(check_dates)
    _check_for_empty_strings = field_validator("notes", "commands")(
        check_fields_for_empty_strings
    )


class Campaign(AttributeUpdater, BaseModel):
    # Required
    name: str = Field(
        ..., description="The name of the campaign in the format YYYY_A_VVVV"
    )
    type: str = Field(
        ..., description="The type of the campaign (deploy | measure | etc)"
    )
    vesselCode: str = Field(
        ...,
        description="The 4 digit vessel code, associated with a vessel metadata file",
    )
    start: datetime = Field(
        ...,
        description="The start date & time of the campaign",
        gt=datetime(1901, 1, 1),
    )
    end: datetime = Field(
        ..., description="The end date & time of the campaign", gt=datetime(1901, 1, 1)
    )

    # Optional
    vessel: Optional[Vessel] = Field(default=None, description="Instatiate Vessel object",)
    principalInvestigator: Optional[str] = Field(default=None)
    launchVesselName: Optional[str] = Field(default=None)
    recoveryVesselName: Optional[str] = Field(default=None)
    cruiseName: Optional[str] = Field(default=None)
    technicianName: Optional[str] = Field(default=None)
    technicianContact: Optional[str] = Field(default=None)
    surveys: List[Survey] = Field(default_factory=list)

    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end", mode="after")(check_dates)
    _check_for_empty_strings = field_validator(
        "principalInvestigator",
        "launchVesselName",
        "recoveryVesselName",
        "cruiseName",
        "technicianName",
        "technicianContact",
    )(check_fields_for_empty_strings)

    def check_survey_times(self):
        """Check that survey times do not overlap with each other"""
        # TODO test this

        # Sort surveys by start time
        sorted_surveys = sorted(self.surveys, key=lambda survey: survey.start)

        # Check for overlapping times
        for i in range(len(sorted_surveys) - 1):
            current_survey = sorted_surveys[i]
            next_survey = sorted_surveys[i + 1]

            if current_survey.end > next_survey.start:
                raise ValueError(
                    f"Survey times overlap: {current_survey.id} ends at {current_survey.end} and {next_survey.id} starts at {next_survey.start}"
                )

        print("No overlapping survey times found.")

    # @model_serializer
    # def _serialize(self):
    #     to_omit = ["vessel"]
    #     return {k:v for k,v in self if k not in to_omit}

    def get_survey_by_datetime(self, dt: datetime) -> Survey:
        """Return the survey that contains the given datetime"""
        for survey in self.surveys:
            if survey.start <= dt <= survey.end:
                return survey

        raise ValueError(f"No survey found for datetime {dt}")

    class Config:
        arbitrary_types_allowed = True
