from typing import Any, Dict, List

from es_sfgtools.utils.metadata.utils import AttributeUpdater

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
    def __init__(self, survey_id: str, additional_data: Dict[str, Any] = None):
        self.id = survey_id
        self.type: str = ""
        self.benchmarkIDs: List[str] = []
        self.start: str = ""
        self.end: str = ""
        self.notes: str = ""
        self.commands: str = ""

        if additional_data:
            self.update_attributes(additional_data)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Survey instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Survey instance.
        """
        return {
            "id": self.id,
            "type": self.type,
            "benchmarkIDs": self.benchmarkIDs,
            "start": self.start,
            "end": self.end,
            "notes": self.notes,
            "commands": self.commands
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
        self.start: str = ""
        self.end: str = ""
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
        self.start = existing_campaign.get("start", "")
        self.end = existing_campaign.get("end", "")
        self.surveys = [Survey(survey_id=survey["id"], additional_data=survey) for survey in existing_campaign["surveys"]]


    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Campaign instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Campaign instance.
        """
        return {
            "name": self.name,
            "vesselCode": self.vesselCode,
            "type": self.type,
            "launchVesselName": self.launchVesselName,
            "recoveryVesselName": self.recoveryVesselName,
            "cruiseName": self.cruiseName,
            "technicianName": self.technicianName,
            "technicianContact": self.technicianContact,
            "start": self.start,
            "end": self.end,
            "surveys": [survey.to_dict() for survey in self.surveys]    
        }

