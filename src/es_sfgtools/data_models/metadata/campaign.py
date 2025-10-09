from datetime import datetime
from typing import List, Optional,Union
from enum import Enum
from pydantic import BaseModel, Field, field_validator,field_serializer,PrivateAttr
from regex import match

from .utils import (
    AttributeUpdater,
    check_dates,
    check_fields_for_empty_strings,
    parse_datetime,
)
from .vessel import Vessel


def campaign_checks(campaign_year, campaign_interval, vessel_code):
    """
    Checks the campaign year, interval, and vessel code for validity.

    :param campaign_year: The campaign year (e.g., "2023").
    :type campaign_year: str
    :param campaign_interval: The campaign interval (e.g., "A").
    :type campaign_interval: str
    :param vessel_code: The 4-character vessel code.
    :type vessel_code: str
    :return: A tuple containing the formatted campaign name and the uppercase vessel code.
    :rtype: Tuple[str, str]
    :raises ValueError: If the campaign year, interval, or vessel code are invalid.
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

class SurveyType(str, Enum):
    CIRCLE = "circledrive"
    FIXED_POINT = "fixedpoint"
    MIXED = "mixed"
    CENTER = "center"
    GARPOS = "garpos"
    MOVEAROUND = "movearound"
    OTHER = "other"

def classify_survey_type(survey_type: str) -> SurveyType:
    """
    Classifies the survey type based on the provided string.

    :param survey_type: The survey type as a string.
    :type survey_type: str
    :return: The classified SurveyType.
    :rtype: SurveyType
    :raises ValueError: If the survey type is not recognized.
    """
    survey_type = survey_type.lower()
    if match(r"^(circle|circledrive|circle drive)$", survey_type):
        return SurveyType.CIRCLE
    elif match(r"^(fixed point|fixedpoint)$", survey_type):
        return SurveyType.FIXED_POINT
    elif match(r"^(mixed)$", survey_type):
        return SurveyType.MIXED
    elif match(r"^(center)$", survey_type):
        return SurveyType.CENTER
    elif match(r"^(garpos)$", survey_type):
        return SurveyType.GARPOS
    elif match(r"^(movearound|move around)$", survey_type):
        return SurveyType.MOVEAROUND
    else:
        return SurveyType.OTHER

class Survey(AttributeUpdater, BaseModel):
    """
    Represents a single survey within a campaign.
    """
    # Required
    id: str = Field(
        ..., description="The unique ID of the survey"
    )  
    type: Union[str,SurveyType] = Field(
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
    _type_input : Optional[str] = PrivateAttr(default=None)

    """
    Gracefully convert survey type from string to SurveyType enum and vice versa for serialization.
    """
    @field_validator("type", mode="before")
    def _validate_survey_type(type:Union[str,SurveyType]) -> SurveyType:
        if isinstance(type, str):
            type = classify_survey_type(type)
        elif not isinstance(type, SurveyType):
            raise ValueError("Survey type must be a string or SurveyType enum")
        return type
   
    @field_serializer("type")
    def _serialize_survey_type(self,type:SurveyType,_info) -> str:
        if self._type_input is None:
            return type.value

        return self._type_input

class Campaign(AttributeUpdater, BaseModel):
    """
    Represents a campaign, which is a collection of surveys.
    """
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
        """
        Checks that survey times within the campaign do not overlap with each other.

        :raises ValueError: If any survey times overlap.
        """
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
        """
        Returns the survey that encompasses the given datetime.

        :param dt: The datetime to check against surveys.
        :type dt: datetime
        :return: The Survey object that contains the given datetime.
        :rtype: Survey
        :raises ValueError: If no survey is found for the given datetime.
        """
        for survey in self.surveys:
            if survey.start <= dt <= survey.end:
                return survey

        raise ValueError(f"No survey found for datetime {dt}")

    class Config:
        arbitrary_types_allowed = True
