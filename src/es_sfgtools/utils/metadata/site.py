import json
from datetime import datetime
import ipywidgets as widgets
from typing import Any, Union, Dict, List

from es_sfgtools.utils.metadata.benchmark import Benchmark, Transponder
from es_sfgtools.utils.metadata.campaign import Campaign, Survey
from es_sfgtools.utils.metadata.utils import layout, AttributeUpdater

def import_site(filepath: str):
    """Import site data from a JSON file."""
    with open(filepath, 'r') as file:
        return Site(existing_site=json.load(file))
    

top_level_groups = ["referenceFrames", "benchmarks", "campaigns", "surveyVessels"]

button_descriptions = {
    "new_ref": "Add new reference frame",
    "new_campaign": "Add new campaign",
    "new_benchmark": "Add new benchmark",
    "new_transponder": "Add new transponder",
    "new_survey": "Add new survey",
    "existing_ref": "Update existing reference frame",
    "existing_campaign": "Update existing campaign",
    "existing_benchmark": "Update existing benchmark",
    "existing_transponder": "Update existing transponder",
    "existing_survey": "Update existing survey",
    "delete_ref": "Delete reference frame",
    "delete_campaign": "Delete campaign",
    "delete_benchmark": "Delete benchmark",
    "delete_transponder": "Delete transponder",
    "delete_survey": "Delete survey",
    "add_sensor": "Add sensor to transponder",
    "add_battery": "Add battery voltage to transponder"
}

buttons = {key: widgets.Button(description=value, button_style='danger', layout=layout) for key, value in button_descriptions.items()}

class ReferenceFrame(AttributeUpdater):
    start: str = ""
    end: str = ""

    def __init__(self, name: str):
        self.name = name

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the ReferenceFrame instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the ReferenceFrame instance.
        """
        return {
            "name": self.name,
            "start": self.start,
            "end": self.end
        }


class Site:
    def __init__(self, names: List[str] = None, networks: List[str] = None, time_of_origin: str = None, 
    local_geoid_height: float = None, array_center: Dict = None, existing_site: Dict = None) -> None:
        """
        Create a new site object.

        Args:
            names: List of names of the site.
            networks: List of networks the site is part of.
            time_of_origin: Time of origin of the site.
            local_geoid_height: Local geoid height of the site.
            existing_site: Existing site data to import.
        """

        if existing_site:
            self.import_existing_site(existing_site)
            return

        if time_of_origin <= datetime(year=1990, month=1, day=1):
            print("Time of origin is not a valid date.. Not entering time of origin.")
            time_of_origin = None        

        self.names = names
        self.networks = networks if networks else []
        self.timeOrigin = time_of_origin.strftime('%Y-%m-%dT%H:%M:%S') if time_of_origin else " ",
        self.localGeoidHeight = local_geoid_height if local_geoid_height else ""
        self.arrayCenter = array_center if array_center else {'x': "", 'y': "", 'z': ""}  
        self.campaigns: List[Campaign] = []
        self.benchmarks: List[Benchmark] = []
        self.referenceFrames: List[ReferenceFrame] = []

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Site instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Site instance.
        """
        return {
            "names": self.names,
            "networks": self.networks,
            "timeOrigin": self.timeOrigin,
            "localGeoidHeight": self.localGeoidHeight,
            "arrayCenter": self.arrayCenter,
            "referenceFrames": [referenceFrame.to_dict() for referenceFrame in self.referenceFrames],
            "benchmarks": [benchmark.to_dict() for benchmark in self.benchmarks],
            "campaigns": [campaign.to_dict() for campaign in self.campaigns]
        }
    
    def import_existing_site(self, existing_site: Dict):
        self.names = existing_site.get("names", [])
        self.networks = existing_site.get("networks", [])
        self.timeOrigin = existing_site.get("timeOrigin", "")
        self.localGeoidHeight = existing_site.get("localGeoidHeight", "")
        self.arrayCenter = existing_site.get("arrayCenter", {})
        self.campaigns = [Campaign(existing_campaign=campaign) for campaign in existing_site.get("campaigns", [])]
        self.benchmarks = [Benchmark(existing_benchmark=benchmark) for benchmark in existing_site.get("benchmarks", [])]
        self.referenceFrames = [ReferenceFrame(name=rf["name"], additional_data=rf) for rf in existing_site.get("referenceFrames", [])]

    def new_benchmark(self, benchmark_name: str, benchmark_data: dict):
        """ Add a new benchmark to the site dictionary """
        for benchmark in self.benchmarks:
            if benchmark.name == benchmark_name:
                print("Benchmark already exists.. Choose to update or delete if needed")
                print(json.dumps(benchmark.to_dict(), indent=2))
                return
            
        print("Adding new benchmark..")
        new_benchmark = Benchmark(benchmark_name, additional_data=benchmark_data)
        self.benchmarks.append(new_benchmark)
        print(json.dumps(new_benchmark.to_dict(), indent=2))


    def update_existing_benchmark(self, benchmark_name: str, benchmark_data: dict):
        """ Update an existing benchmark in the site dictionary """
        print("Updating existing benchmark..")
        for benchmark in self.benchmarks:
            if benchmark.name == benchmark_name:
                benchmark.update_attributes(benchmark_data)
                print(json.dumps(benchmark.to_dict(), indent=2))
                return

    def delete_benchmark(self, benchmark_name: str):
        """ Delete a benchmark from the site dictionary """
        print("Deleting benchmark..")
        for benchmark in self.benchmarks:
            if benchmark.name == benchmark_name:
                self.benchmarks.remove(benchmark)
                print("Deleted benchmark..")
                return
            
        print("Benchmark not found..")

    def new_transponder(self, benchmark_name: str, transponder_address, transponder_data: dict, output, event=None):
        """ Add a new transponder to a benchmark in the site dictionary """
        with output:
            benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)
            
            if benchmark is None:
                print(f"Benchmark {benchmark_name} not found, ensure you have the correct benchmark name")
                return
            
            if not transponder_address:
                print("Transponder address not provided, address is required for adding..")
                return

            for transponder in benchmark.transponders:
                if transponder.address == transponder_address:
                    print("Transponder {} already exists in benchmark.. please update instead of adding".format(transponder_data["address"]))
                    return
            
            print("Adding new transponder to benchmark {}..".format(benchmark_name))
            benchmark.transponders.append(Transponder(transponder_address, additional_data=transponder_data))
            print(json.dumps(benchmark.to_dict(), indent=2))
            print("Added transponder to benchmark..")

    def add_sensor_to_transponder(self, benchmark_name: str, transponder_address: str, sensor_data: dict, output, event=None):
        with output:
            benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)
            
            if benchmark is None:
                print(f"Benchmark {benchmark_name} not found, ensure you have the correct benchmark name")
                return
            
            transponder = next((t for t in benchmark.transponders if t.address == transponder_address), None)
            
            if transponder is None:
                print(f"Transponder with address {transponder_address} not found in benchmark {benchmark_name}")
                return
            
            print("Adding sensor to transponder {} in benchmark {}..".format(transponder_address, benchmark_name))
            transponder.extraSensors.append(sensor_data)

    def add_battery_voltage_to_transponder(self, benchmark_name: str, transponder_address: str, battery_data: dict, output, event=None):
        with output:
            benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)
            
            if benchmark is None:
                print(f"Benchmark {benchmark_name} not found, ensure you have the correct benchmark name")
                return
            
            transponder = next((t for t in benchmark.transponders if t.address == transponder_address), None)
            
            if transponder is None:
                print(f"Transponder with address {transponder_address} not found in benchmark {benchmark_name}")
                return
            
            print("Adding battery voltage to transponder {} in benchmark {}..".format(transponder_address, benchmark_name))
            transponder.batteryVoltage.append(battery_data)


    def update_existing_transponder(self, benchmark_name: str, transponder_address: str, transponder_data: dict, output, event=None):
        with output:
            benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)
            
            if benchmark is None:
                print(f"Benchmark {benchmark_name} not found, ensure you have the correct benchmark name")
                return
            
            transponder = next((t for t in benchmark.transponders if t.address == transponder_address), None)
            
            if transponder is None:
                print(f"Transponder with address {transponder_address} not found in benchmark {benchmark_name}")
                return
            
            print("Updating transponder in benchmark {}..".format(benchmark_name))
            transponder.update_attributes(transponder_data)
            print(json.dumps(benchmark.to_dict(), indent=2))
            print("Updated transponder.")

    def delete_transponder(self, benchmark_name: str, transponder_address: str, output, event=None):
        with output:
            benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)
            
            if benchmark is None:
                print(f"Benchmark {benchmark_name} not found, ensure you have the correct benchmark name")
                return
            
            transponder = next((t for t in benchmark.transponders if t.address == transponder_address), None)
            
            if transponder is None:
                print(f"Transponder with address {transponder_address} not found in benchmark {benchmark_name}")
                return
            
            print("Deleting transponder in benchmark {}..".format(benchmark_name))
            for transponder in benchmark.transponders:
                if transponder.address == transponder_address:
                    benchmark.transponders.remove(transponder)
                    return

            print("Transponder not found..")
                

    def new_campaign(self, campaign_name: str, campaign_data: dict, output, event=None):
        """ Add a new campaign to the site dictionary """
        with output:
            for campaign in self.campaigns:
                if campaign.name == campaign_name:
                    print("Campaign already exists.. Choose to update or delete if needed")
                    print(json.dumps(campaign.to_dict(), indent=2))
                    return
            
            print("Adding new campaign..")
            new_campaign = Campaign(campaign_name, additional_data=campaign_data)
            self.campaigns.append(new_campaign)
            print(json.dumps(new_campaign.to_dict(), indent=2))       

    def update_existing_campaign(self, campaign_name: str, campaign_data: dict, output, event=None):
        """ Update an existing campaign in the site dictionary """
        with output:
            print("Updating existing campaign..")
            for campaign in self.campaigns:
                if campaign.name == campaign_name:
                    campaign.update_attributes(campaign_data)
                    print(json.dumps(campaign.to_dict(), indent=2))
                    return
            
            print("Campaign not found..")
                

    def delete_campaign(self, campaign_name: str, output, event=None):
        """ Delete a campaign from the site dictionary """
        with output:
            print("Deleting campaign..")
            for campaign in self.campaigns:
                if campaign.name == campaign_name:
                    self.campaigns.remove(campaign)
                    print("Deleted campaign..")
                    return
                
            print("Campaign not found..")


    def new_survey(self, campaign_name: str, survey_data: dict, output, event=None):
        """ Add a new survey to a campaign in the site dictionary """
        with output:
            campaign = next((c for c in self.campaigns if c.name == campaign_name), None)
            
            if campaign is None:
                print(f"Campaign {campaign_name} not found, ensure you have the correct campaign name")
                return
            
            survey_id = campaign_name + "_" + str(len(campaign.surveys) + 1)
            print("Generating new survey ID: " + survey_id)
            
            print("Adding survey to campaign {}..".format(campaign_name))
            campaign.surveys.append(Survey(survey_id=survey_id, additional_data=survey_data))
            print(json.dumps(campaign.to_dict(), indent=2))
            print("Added survey to campaign..")

    def update_existing_survey(self, campaign_name: str, survey_id: str, survey_data: dict, output, event=None):
        with output:
            campaign = next((c for c in self.campaigns if c.name == campaign_name), None)
            if campaign is None:
                print(f"Campaign {campaign_name} not found, ensure you have the correct campaign name")
                return

            campaign_survey = next((survey for survey in campaign.surveys if survey.id == survey_id), None)
            if campaign_survey is None:
                print(f"Survey with ID {survey_id} not found in campaign {campaign_name}")
                return
            
            print("Updating survey {} in campaign {}..".format(survey_id, campaign_name))
            campaign_survey.update_attributes(survey_data)
            print(json.dumps(campaign.to_dict(), indent=2))
            print("Updated survey {} in campaign {}..".format(survey_id, campaign_name))

    def delete_survey(self, campaign_name: str, survey_id: str, output, event=None):
        """ Delete a survey from a campaign in the site/campaign dictionary """
        with output:
            campaign = next((c for c in self.campaigns if c.name == campaign_name), None)
            if campaign is None:
                print(f"Campaign {campaign_name} not found, ensure you have the correct campaign name")
                return

            campaign_survey = next((survey for survey in campaign.surveys if survey.id == survey_id), None)
            if campaign_survey is None:
                print(f"Survey with ID {survey_id} not found in campaign {campaign_name}")
                return
            
            print("Deleting survey {} in campaign {}..".format(survey_id, campaign_name))
            for survey in campaign.surveys:
                if survey.id == survey_id:
                    campaign.surveys.remove(survey)
                    print("Deleted survey..")
                    return
                
            print("Survey not found..")
            
    def new_reference_frame(self, reference_frame_name: str, reference_frame_data: dict, output, event=None):
        """ Add a new reference frame to the site dictionary """
        with output:
            for reference_frame in self.referenceFrames:
                if reference_frame.name == reference_frame_name:
                    print("Reference frame already exists.. Choose to update or delete if needed")
                    print(json.dumps(reference_frame.to_dict(), indent=2))
                    return
            
            print("Adding new reference frame..")
            new_reference_frame = ReferenceFrame(reference_frame_name)
            new_reference_frame.update_attributes(reference_frame_data)
            self.referenceFrames.append(new_reference_frame)
            print(json.dumps(new_reference_frame.to_dict(), indent=2))

    def update_existing_reference_frame(self, reference_frame_name: str, reference_frame_data: dict, output, event=None):
        """ Update an existing reference frame in the site dictionary """
        with output:
            print("Updating existing reference frame..")
            for reference_frame in self.referenceFrames:
                if reference_frame.name == reference_frame_name:
                    reference_frame.update_attributes(reference_frame_data)
                    print(json.dumps(reference_frame.to_dict(), indent=2))
                    return
                
            print("Reference frame not found..")
        
    def delete_reference_frame(self, reference_frame_name: str, output, event=None):
        """ Delete a reference frame from the site dictionary """
        with output:
            print("Deleting reference frame..")
            for reference_frame in self.referenceFrames:
                if reference_frame.name == reference_frame_name:
                    self.referenceFrames.remove(reference_frame)
                    print("Deleted reference frame..")
                    return
                
            print("Reference frame not found..")