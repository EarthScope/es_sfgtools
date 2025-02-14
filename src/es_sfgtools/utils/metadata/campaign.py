from datetime import datetime
from typing import Any, Dict, List

from es_sfgtools.utils.metadata.utils import AttributeUpdater, convert_to_datetime

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


class Survey(AttributeUpdater):
    def __init__(self, survey_id: str= None, additional_data: Dict[str, Any] = None, existing_survey: Dict[str, Any] = None):
        if existing_survey:
            self.import_existing_survey(existing_survey)
            return
        
        self.survey_id = survey_id
        self.type: str = ""
        self.benchmarkIDs: List[str] = []
        self.start: datetime = None
        self.end: datetime = None
        self.notes: str = ""
        self.commands: str = ""

        if additional_data:
            self.update_attributes(additional_data)

    def import_existing_survey(self, existing_survey: Dict[str, Any]):
        """
        Import an existing survey from a dictionary.

        Args:
            existing_survey (Dict[str, Any]): A dictionary containing the existing survey data.
        """
        self.survey_id = existing_survey.get("id", "")
        self.type = existing_survey.get("type", "")
        self.benchmarkIDs = existing_survey.get("benchmarkIDs", [])
        start_time = existing_survey.get("start", "")
        if start_time:
            self.start = convert_to_datetime(start_time)
        else:
            self.start = None

        end_time = existing_survey.get("end", "")
        if end_time:
            self.end = convert_to_datetime(end_time)
        else:
            self.end = None

        self.notes = existing_survey.get("notes", "")
        self.commands = existing_survey.get("commands", "")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Survey instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Survey instance.
        """
        return {
            "id": self.survey_id,
            "type": self.type if self.type else "",
            "benchmarkIDs": self.benchmarkIDs if self.benchmarkIDs else [],
            "start": self.start.strftime('%Y-%m-%dT%H:%M:%S') if self.start else "",
            "end": self.end.strftime('%Y-%m-%dT%H:%M:%S') if self.end else "",
            "notes": self.notes if self.notes else "",
            "commands": self.commands if self.commands else ""
        }


class Campaign(AttributeUpdater):

    def __init__(self, name: str = None, additional_data: Dict[str, Any] = None, existing_campaign: dict = None):
        self.vesselCode: str = ""
        self.type: str = ""
        self.principalInvestigator: str = ""
        self.launchVesselName: str = ""
        self.recoveryVesselName: str = ""
        self.cruiseName: str = ""
        self.technicianName: str = ""
        self.technicianContact: str = ""
        self.start: datetime = None
        self.end: datetime = None
        self.surveys: List[Survey] = []
        
        if existing_campaign:
            self.import_existing_campaign(existing_campaign)
            return 
        
        self.name = name

        if additional_data:
            self.update_attributes(additional_data)

    def import_existing_campaign(self, existing_campaign: dict):
        self.name = existing_campaign.get("name", "")
        self.vesselCode = existing_campaign.get("vesselCode", "")
        self.type = existing_campaign.get("type", "")
        self.principalInvestigator = existing_campaign.get("principalInvestigator", "")
        self.launchVesselName = existing_campaign.get("launchVesselName", "")
        self.recoveryVesselName = existing_campaign.get("recoveryVesselName", "")
        self.cruiseName = existing_campaign.get("cruiseName", "")
        self.technicianName = existing_campaign.get("technicianName", "")
        self.technicianContact = existing_campaign.get("technicianContact", "")

        start = existing_campaign.get("start", "")
        if start:
            self.start = convert_to_datetime(start)
        else:
            self.start = None

        end = existing_campaign.get("end", "")
        if end:
            self.end = convert_to_datetime(end)
        else:
            self.end = None

        self.surveys = [Survey(survey_id=survey["id"], existing_survey=survey ) for survey in existing_campaign["surveys"]]


    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Campaign instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Campaign instance.
        """
        return {
            "name": self.name,
            "vesselCode": self.vesselCode,
            "type": self.type if self.type else "",
            "launchVesselName": self.launchVesselName if self.launchVesselName else "",
            "recoveryVesselName": self.recoveryVesselName if self.recoveryVesselName else "",
            "cruiseName": self.cruiseName if self.cruiseName else "",
            "technicianName": self.technicianName if self.technicianName else "",
            "technicianContact": self.technicianContact if self.technicianContact else "",
            "start": self.start.strftime('%Y-%m-%dT%H:%M:%S') if self.start else "",
            "end": self.end.strftime('%Y-%m-%dT%H:%M:%S') if self.end else "",
            "surveys": [survey.to_dict() for survey in self.surveys]    
        }

