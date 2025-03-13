from datetime import datetime
from typing import Any, Dict, List, Optional

from es_sfgtools.utils.metadata.utils import AttributeUpdater, check_dates, check_optional_fields_for_empty_strings, parse_datetime
from pydantic import BaseModel, Field, field_validator


def campaign_checks(campaign_year, campaign_interval, vessel_code):
    """ Check the campaign year, interval and vessel code 
    
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
    
    campaign_name = campaign_year + "_" + campaign_interval.upper() + "_" + vessel_code.upper()
    
    print("Campaign name: " + campaign_name)
    return campaign_name,  vessel_code.upper()


class Survey(AttributeUpdater, BaseModel):
    # Required
    id: str = Field(..., description="The unique ID of the survey") # Todo generate this
    type: str = Field(..., description="The type of the survey (e.g. circle | fixed point | mixed)")
    benchmarkIDs: List[str] = Field(..., description="The unique IDs of the benchmarks associated with the survey")
    start: datetime = Field(..., description="The start date of the survey",
                            gt=datetime(1901, 1, 1))
    end: datetime = Field(..., description="The end date of the survey",
                          gt=datetime(1901, 1, 1))

    # Optional
    notes: Optional[str] = Field(default=None)
    commands: Optional[str] = Field(default=None)

    _parse_datetime = field_validator('start', 'end', mode='before')(parse_datetime)
    _check_dates = field_validator('end', mode='after')(check_dates)
    _check_for_empty_strings = field_validator('notes', 'commands')(check_optional_fields_for_empty_strings)


class Campaign(AttributeUpdater, BaseModel):
    # Required
    name: str = Field(..., description="The name of the campaign in the format YYYY_A_VVVV")
    type: str = Field(..., description="The type of the campaign (deploy | measure | etc)")
    vesselCode: str = Field(..., description="The 4 digit vessel code, associated with a vessel metadata file")
    start: datetime = Field(..., description="The start date of the campaign",
                            gt=datetime(1901, 1, 1))
    end: datetime = Field(..., description="The end date of the campaign",
                          gt=datetime(1901, 1, 1))

    # Optional
    principalInvestigator: Optional[str] = Field(default=None)
    launchVesselName: Optional[str] = Field(default=None)
    recoveryVesselName: Optional[str] = Field(default=None)
    cruiseName: Optional[str] = Field(default=None)
    technicianName: Optional[str] = Field(default=None)
    technicianContact: Optional[str] = Field(default=None)
    surveys: List[Survey] = Field(default_factory=list)

    _parse_datetime = field_validator('start', 'end', mode='before')(parse_datetime)
    _check_dates = field_validator('end', mode='after')(check_dates)
    _check_for_empty_strings = field_validator('principalInvestigator', 'launchVesselName', 'recoveryVesselName', 
                                               'cruiseName', 'technicianName', 'technicianContact')(check_optional_fields_for_empty_strings)
