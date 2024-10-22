import json
from datetime import datetime
import os
import ipywidgets as widgets
from ipywidgets import Layout


style = {'description_width': 'initial'}
layout=Layout(width='30%', height='40px')

button_descriptions = {
    "new_ref": "Add new reference frame",
    "new_campaign": "Add new campaign",
    "new_benchmark": "Add new benchmark",
    "new_transponder": "Add new transponder",
    "new_survey": "Add new survey",
    "new_survey_vessel": "Add new survey vessel",
    "new_payload": "Add new INS payload",
    "new_receiver": "Add new GNSS Receiver",
    "new_antenna": "Add new GNSS Antenna",
    "new_transducer": "Add new acoustic transducer",
    "new_transceiver": "Add new acoustic transceiver",
    "new_atd": "Add new acoustic to transducer offsets",
    "existing_ref": "Update existing reference frame",
    "existing_campaign": "Update existing campaign",
    "existing_benchmark": "Update existing benchmark",
    "existing_transponder": "Update existing transponder",
    "existing_survey": "Update existing survey",
    "existing_survey_vessel": "Update existing survey vessel",
    "existing_payload": "Update existing INS payload",
    "existing_receiver": "Update existing GNSS Receiver",
    "existing_antenna": "Update existing GNSS Antenna",
    "existing_transducer": "Update existing acoustic transducer",
    "existing_transceiver": "Update existing acoustic transceiver",
    "existing_atd": "Update existing acoustic to transducer offsets"
}

buttons = {key: widgets.Button(description=value, button_style='danger', layout=layout) for key, value in button_descriptions.items()}

def start_and_end_dates(start_date: datetime, end_date: datetime, dict_to_update: dict) -> dict:
    """
    Add or update the start and end dates in the dictionary.
    """
    if start_date != datetime(year=1900, month=1, day=1):
        dict_to_update['start'] = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    else:
        print("Start Date: not entering")

    if end_date != datetime(year=1900, month=1, day=1):
        dict_to_update['end'] = end_date.strftime('%Y-%m-%dT%H:%M:%S')
    else:
        print("End Date: not entering")

    print("Adding or updating site with: \n" + json.dumps(dict_to_update, indent=2))

    return dict_to_update


top_level_groups = ["referenceFrames", "benchmarks", "campaigns", "surveyVessels"]
survey_vessels_types = ["insPayloads", "gnssReceivers", "gnssAntennas", "acousticTransducer", 
                        "acousticTransceiver", "atdOffsets"]


class Site:
    def __init__(self, names: list = None, networks: list = None, time_of_origin: datetime = None, local_geoid_height: float = None, existing_site: dict = None) -> None:
        """
        Create a new site object.
        
        :param names: List of names of the site.
        :param networks: List of networks the site is part of.
        :param time_of_origin: Time of origin of the site.
        :param local_geoid_height: Local geoid height of the site.
        :param existing_site: Existing site data to import.
        """

        if existing_site:
            self.site = existing_site
        else:
            self.site = {
                "names": names if names else [],
                "networks": networks if networks else [],
                "timeOrigin": time_of_origin.strftime('%Y-%m-%dT%H:%M:%S') if time_of_origin else None,
                "arrayCenter": {},
                "localGeoidHeight": local_geoid_height,
                "referenceFrames": [],
                "benchmarks": [],
                "campaigns": [],
                "surveyVessels": [],
            }

    def export_site(self, filepath: str):
        """Export site data to a JSON file."""
        with open(filepath, 'w') as file:
            json.dump(self.site, file, indent=4)

    def new_top_level_group(self, group_name: str, group_data: dict, output, event=None):
        """
        Add a new top level group to the site dictionary
        """
        with output:
            top_level_groups = ["referenceFrames", "benchmarks", "campaigns", "surveyVessels"]

            if group_name not in top_level_groups:
                print("Group name not found, ensure you have the correct group name")
                return
            
            # check if group already exists
            for group in self.site[group_name]:
                if group["name"] == group_data["name"]:
                    print(group_name + " already exists.. Choose to update if needed")
                    print(json.dumps(self.site[group_name], indent=2))
                    return
            
            self.site[group_name].append(group_data)

            print("Added " + group_name)
            print(json.dumps(self.site[group_name], indent=2))


    def existing_ref_frame(self, reference_frame: dict, output, event=None):
        """ Update existing reference frame using the reference frame name """
        with output:
            for i in range(len(self.site["referenceFrames"])):
                site_reference_frame_name =  self.site["referenceFrames"][i]["name"]

                if site_reference_frame_name == reference_frame["name"]:
                    print("Updating " + site_reference_frame_name)
                    # full_reference_frame = self.site["referenceFrames"][i]

                    for key in reference_frame:
                        if reference_frame[key]:
                            self.site["referenceFrames"][i][key] = reference_frame[key]

                    print("Updated reference frame " + site_reference_frame_name)
                    print(json.dumps(self.site["referenceFrames"], indent=2))
                    break
            else:
                print("Reference frame not found, ensure you have the correct reference frame name")

    def existing_campaign(self, campaign: dict, output, event=None):
        """ Update existing campaign using the campaign name """
        with output:
            for i in range(len(self.site["campaigns"])):
                site_campaign_name =  self.site["campaigns"][i]["name"]

                if site_campaign_name == campaign["name"]:
                    print("Updating " + site_campaign_name)
                    # full_campaign = self.site["campaigns"][i]

                    for key in campaign:
                        if campaign[key]:
                            self.site["campaigns"][i][key] = campaign[key]

                    print("Updated campaign " + site_campaign_name)
                    print(json.dumps(self.site["campaigns"], indent=2))
                    break
            else:
                print("Campaign not found, ensure you have the correct campaign name")


    def add_primary_survey_vessel(self, vessel_data: dict, output, event=None):
        with output:
            for vessel in self.site["surveyVessels"]:
                if vessel["name"] == vessel_data["name"]:
                    print("Survey vessel already exists..")
                    print(json.dumps(self.site["surveyVessels"], indent=2))
                    return
            else:
                self.site["surveyVessels"].append(vessel_data)
                print("Added survey vessel")
                print(json.dumps(self.site['surveyVessels'], indent=2))

    
    def existing_survey_vessel(self, survey_vessel_name: str, survey_vessel: dict, output, event=None):
        with output:
            for i in range(len(self.site["surveyVessels"])):
                site_survey_vessel_name =  self.site["surveyVessels"][i]["name"]

                if site_survey_vessel_name == survey_vessel_name:
                    print("Updating " + site_survey_vessel_name)

                    for key in survey_vessel:
                        if survey_vessel[key]:
                            self.site["surveyVessels"][i][key] = survey_vessel[key]

                    print("Updated survey vessel " + site_survey_vessel_name)
                    print(json.dumps(self.site["surveyVessels"], indent=2))
                    break
            else:
                print("Survey vessel not found, ensure you have the correct survey vessel name")

    def add_survey_vessel_equipment(self, vessel_name: str, equipment_name: str, equipment_data: dict, output, event=None):
        with output:
            if equipment_name not in survey_vessels_types:
                print("Survey vessel type not found, ensure you have the correct vessel type")
                return
            
            for vessel in range(len(self.site["surveyVessels"])):
                # IF correct vessel in survey vessel list
                if self.site["surveyVessels"][vessel]["name"] == vessel_name:
                    # IF no equipment by that name exists yet under the vessel, add it and return
                    if equipment_name not in self.site["surveyVessels"][vessel]:
                        self.site["surveyVessels"][vessel][equipment_name] = [equipment_data]
                        print("Added " + equipment_name + " to " + vessel_name)
                        print(json.dumps(self.site["surveyVessels"], indent=2))
                        return
                    
                    else:
                        # IF equipment by that name exists, check if equipment with serial number exists
                        for equipment in self.site["surveyVessels"][vessel][equipment_name]:
                            # IF equipment with serial number exists, update survey ID only
                            if equipment["serialNumber"] == equipment_data["serialNumber"]:
                                print("Equipment with serial number already exists..")
                                print(json.dumps(self.site['surveyVessels'], indent=2))
                                return

                        # IF equipment with serial number does not exist, add it
                        self.site["surveyVessels"][vessel][equipment_name].append(equipment_data)
                        print("Added " + equipment_name + " to " + vessel_name)
                        print(json.dumps(self.site["surveyVessels"], indent=2))
                        return
                    
    def existing_vessel_equipment(self, primary_vessel_name: str, equipment_name: str, equipment: dict, output, event=None):
        """ Update existing equipment on a survey vessel using the serial number """
        with output:
            if equipment_name not in survey_vessels_types:
                print("Survey vessel equipment type not found, ensure you have the correct vessel type")
                return
        
            
            for i in range(len(self.site["surveyVessels"])):
                if self.site["surveyVessels"][i]["name"] == primary_vessel_name:
                    for j in range(len(self.site["surveyVessels"][i][equipment_name])):
                        if self.site["surveyVessels"][i][equipment_name][j]["serialNumber"] == equipment["serialNumber"]:
                            for key in equipment:
                                if equipment[key]:
                                    self.site["surveyVessels"][i][equipment_name][j][key] = equipment[key]

                            print("Updated " + equipment_name + " with serial number " + equipment["serialNumber"])
                            print(json.dumps(self.site["surveyVessels"], indent=2))
                            return
                
    def new_survey(self, survey: dict, campaign_name, output, event=None):
        with output:
            for i in range(len(self.site["campaigns"])):
                if self.site["campaigns"][i]["name"] == campaign_name:
                    for survey in self.site["campaigns"][i]["surveys"]:
                        if survey["id"] == survey["id"]:
                            print("Survey already exists in campaign.. please update instead of adding")
                            return
                    if not survey["id"]:
                        survey['id'] = campaign_name + "_" + str(len(self.site["campaigns"][i]["surveys"]) + 1)
                    self.site["campaigns"][i]["surveys"].append(survey)
                    print("Added survey")
                    print(json.dumps(self.site["campaigns"], indent=2))
                    break
            else:
                print("Campaign not found, ensure you have the correct campaign name")


    def existing_survey(self, survey: dict, campaign_name, survey_id, output, event=None):

        with output:
            for i in range(len(self.site["campaigns"])):
                if self.site["campaigns"][i]["name"] == campaign_name:
                    for j in range(len(self.site["campaigns"][i]["surveys"])):
                        campaign_survey_ID = self.site["campaigns"][i]["surveys"][j]["id"]
                        if campaign_survey_ID == survey_id:
                            print("Updating survey " + campaign_survey_ID)
                            for key in survey:
                                if survey[key]:
                                    self.site["campaigns"][i]["surveys"][j][key] = survey[key]

                            print("Updated survey " + survey_id)
                            print(json.dumps(self.site["campaigns"], indent=2))
                            break


    def existing_benchmark(self, benchmark: dict, output, event=None):
        with output:
            for i in range(len(self.site["benchmarks"])):
                site_benchmark_name =  self.site["benchmarks"][i]["name"]

                if site_benchmark_name == benchmark["name"]:
                    print("Updating " + site_benchmark_name)
                    # full_benchmark = self.site["benchmarks"][i]

                    for key in benchmark:
                        if benchmark[key]:
                            self.site["benchmarks"][i][key] = benchmark[key]

                    print("Updated benchmark " + site_benchmark_name)
                    print(json.dumps(self.site["benchmarks"], indent=2))
                    break
            else:
                print("Benchmark not found, ensure you have the correct benchmark name")

    def add_transponder_to_benchmark(self, benchmark_name, transponder: dict, output, event=None):
        with output:
            for i in range(len(self.site["benchmarks"])):
                if self.site["benchmarks"][i]["name"] == benchmark_name:
                    for transponder in self.site["benchmarks"][i]["transponders"]:
                        if transponder["uid"] == transponder["uid"]:
                            print("Transponder already exists in benchmark.. please update instead of adding")
                            return
                    self.site["benchmarks"][i]["transponders"].append(transponder)
                    print("Added transponder")
                    print(json.dumps(self.site["benchmarks"], indent=2))
                    break
            else:
                print("Benchmark not found, ensure you have the correct benchmark name")


    def existing_transponder(self, benchmark_name: str, transponder: dict, output, event=None):
        with output:
            for i in range(len(self.site["benchmarks"])):
                if self.site["benchmarks"][i]["name"] == benchmark_name:
                    for j in range(len(self.site["benchmarks"][i]["transponders"])):
                        benchmark_transponder_ID = self.site["benchmarks"][i]["transponders"][j]["uid"]
                        if benchmark_transponder_ID == transponder["uid"]:
                            # site_transponder = self.site["benchmarks"][i]["transponders"][j]
                            
                            for key in transponder:
                                if transponder[key]:
                                    self.site["benchmarks"][i]["transponders"][j][key] = transponder[key]
                        
                        print("Updated transponder " + benchmark_transponder_ID)
                        print(json.dumps(self.site["benchmarks"], indent=2))
                        break

            else:
                print("Transponder not found, ensure you have the correct benchmark & transponder name")




def import_site(filepath: str):
    """Import site data from a JSON file."""
    with open(filepath, 'r') as file:
        return Site(existing_site=json.load(file))


# todo: add this functionality to the site class in the future
# SUBDUCTION_ZONE_PATH_INDEX = 1
# STATION_NAME_PATH_INDEX = 2
# YYYY_A_CAMPAIGN_PATH_INDEX = 3
# RAW_FILES_PATH_INDEX = 4

# def read_master_file(file_path: str):
#     """ 
#     Read the SITE.master file and return the contents as a dictionary. 
#     The master file contains apriori transponder positions & delay times.

#     Parameters:
#     master_file_path (str): The path to the SITE.master file.
#     """

#     # Check if the master file exists
#     if not os.path.exists(file_path):
#         print(f'The master file does not exist: {file_path}')
#         return

#     # Open the master file and read the first line
#     with open(file_path, 'r') as file:
#         lines = file.readlines()
    
#     # Extract the transponder positions and delay times
#     start_date = datetime.datetime.strptime(lines[0].strip(), '%Y-%m-%d %H:%M:%S')
#     num_of_transponders = int(lines[1].strip())

#     for transponder_index in range(num_of_transponders):
#         transponder_ID, _, latitude, longitude, height, turn_around_time, _ = lines[2 + transponder_index].strip().split()

#         transponder_name = ("{}-{}").format(self.site_name + 
#                                             transponder_ID)
        
# todo: add this functionality to the site class in the future
def read_lever_arms_file(file_path):
    """ Read the lever arms file and return the contents as a dictionary.
    (The lever arms file contains the body frame offsets between antenna and transducer)
    """

    # Check if the lever arms file exists
    if not os.path.exists(file_path):
        print(f'The lever arms file does not exist: {file_path}')
        return

    # Open the lever arms file and read the first line
    with open(file_path, 'r') as file:
        line = file.readline()

    # Extract the lever arms
    lever_arms = {}
    lever_arms['X'], lever_arms['Y'], lever_arms['Z'] = line.split()

    return lever_arms