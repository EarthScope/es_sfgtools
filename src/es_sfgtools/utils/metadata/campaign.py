from datetime import datetime
from typing import Any, Dict, List, Optional

from es_sfgtools.utils.metadata.utils import AttributeUpdater, check_dates, parse_datetime
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
    id: str
    type: str
    benchmarkIDs: List[str]
    start: datetime

    # Optional
    end: Optional[datetime] = Field(default=None)
    notes: Optional[str] = Field(default=None)
    commands: Optional[str] = Field(default=None)

    _parse_datetime = field_validator('start', 'end', mode='before')(parse_datetime)
    _check_dates = field_validator('end', mode='after')(check_dates)


class Campaign(AttributeUpdater, BaseModel):
    # Required
    name: str
    type: str
    vesselCode: str
    start: datetime
    end: datetime

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
