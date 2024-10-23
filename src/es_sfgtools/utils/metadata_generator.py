import json
from datetime import datetime
import os
import ipywidgets as widgets
from ipywidgets import Layout


style = {'description_width': 'initial'}
layout=Layout(width='30%', height='40px')

top_level_groups = ["referenceFrames", "benchmarks", "campaigns", "surveyVessels"]
survey_vessels_types = ["insPayloads", "gnssReceivers", "gnssAntennas", "acousticTransducer", 
                        "acousticTransceiver", "atdOffsets", "imuSensors"]


button_descriptions = {
    "new_ref": "Add new reference frame",
    "new_campaign": "Add new campaign",
    "new_benchmark": "Add new benchmark",
    "new_transponder": "Add new transponder",
    "new_survey": "Add new survey",
    "new_survey_vessel": "Add new survey vessel",
    "new_imu_sensor": "Add new imu sensor",
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
    "existing_imu_sensor": "Update existing imu sensor",
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

def import_site(filepath: str):
    """Import site data from a JSON file."""
    with open(filepath, 'r') as file:
        return Site(existing_site=json.load(file))



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

        # todo add array center to the site class in the future

    def export_site(self, filepath: str):
        """Export site data to a JSON file."""

        print("Exporting site data to: " + filepath)
        with open(filepath, 'w') as file:
            json.dump(self.site, file, indent=4)
        print("Exported site data to: " + filepath)

    def new_top_level_group(self, group_name: str, group_data: dict, output, event=None):
        """
        Add a new top level group to the site dictionary
        """
        with output:
            top_level_groups = ["referenceFrames", "benchmarks", "campaigns", "surveyVessels"]

            if group_name not in top_level_groups:
                print("Metadata Group name not found, ensure you have the correct group name")
                return
            
            if not group_data["name"]:
                print("Name not provided, name is required for adding..")
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


    def existing_ref_frame(self, reference_frame_input: dict, output, event=None):
        """ Update existing reference frame using the reference frame name """
        with output:
            if not reference_frame_input["name"]:
                print("Reference name not provided, name is required for updating..")
                return

            site_reference_frame = next((ref for ref in self.site["referenceFrames"] if ref["name"] == reference_frame_input["name"]), None)
            
            if site_reference_frame is None:
                print("Reference frame not found, ensure you have the correct reference frame name")
                return

            print("Updating " + site_reference_frame["name"])
            for key, value in reference_frame_input.items():
                if value:
                    site_reference_frame[key] = value

            print("Updated reference frame " + site_reference_frame["name"])
            print(json.dumps(self.site["referenceFrames"], indent=2))


    def existing_campaign(self, campaign_input: dict, output, event=None):
        """ Update existing campaign using the campaign name """
        with output:
            if not campaign_input["name"]:
                print("Campaign name not provided, name is required for updating..")
                return
            
            campaign = next((camp for camp in self.site["campaigns"] if camp["name"] == campaign_input["name"]), None)

            if campaign is None:
                print("Campaign not found, ensure you have the correct campaign name")
                return
            
            print("Updating " + campaign["name"])
            for key, value in campaign_input.items():
                if value:
                    campaign[key] = value

            print("Updated campaign " + campaign["name"])
            print(json.dumps(self.site["campaigns"], indent=2))

    def add_primary_survey_vessel(self, vessel_data_input: dict, output, event=None):
        with output:
            print("Adding primary survey vessel..")
            if vessel_data_input["name"] == "":
                print("Name not provided, name is required for adding..")
                return
            
            for vessel in self.site["surveyVessels"]:
                if vessel["name"] == vessel_data_input["name"]:
                    print("Survey vessel already exists..")
                    print(json.dumps(self.site["surveyVessels"], indent=2))
                    return
            else:
                self.site["surveyVessels"].append(vessel_data_input)
                print("Added survey vessel")
                print(json.dumps(self.site['surveyVessels'], indent=2))

    
    def existing_survey_vessel(self, survey_vessel_name: str, survey_vessel_input: dict, output, event=None):
        with output:
            for i in range(len(self.site["surveyVessels"])):
                site_survey_vessel_name =  self.site["surveyVessels"][i]["name"]

                if site_survey_vessel_name == survey_vessel_name:
                    print("Updating " + site_survey_vessel_name)

                    for key, value in survey_vessel_input.items():
                        if value:
                            self.site["surveyVessels"][i][key] = value

                    print("Updated survey vessel " + site_survey_vessel_name)
                    print(json.dumps(self.site["surveyVessels"], indent=2))
                    break
            else:
                print("Survey vessel not found, ensure you have the correct survey vessel name")
                    
    def add_survey_vessel_equipment(self, primary_vessel_name: str, equipment_name: str, equipment_data_input: dict, output, event=None):
        with output:
            print(f"Adding {equipment_name} to {primary_vessel_name}..")
            if equipment_name not in survey_vessels_types:
                print("Survey vessel equipment type not found, ensure you have the correct vessel type")
                return
            
            if equipment_data_input["serialNumber"] == "":
                print("Serial number not provided, please provide a serial number..")
                return

            vessel = next((primary_vessel for primary_vessel in self.site["surveyVessels"] if primary_vessel["name"] == primary_vessel_name), None)
            if vessel is None:
                print("Primary survey vessel {} not found, ensure you have the correct vessel name".format(primary_vessel_name))
                return

            if equipment_name not in vessel:
                vessel[equipment_name] = [equipment_data_input]
                print(f"Added {equipment_name} to {primary_vessel_name}")
                print(json.dumps(self.site["surveyVessels"], indent=2))
                return

            existing_equipment = next((e for e in vessel[equipment_name] if e["serialNumber"] == equipment_data_input["serialNumber"]), None)
            if existing_equipment:
                print("Equipment {} with serial number {} already exists..".format(equipment_name, equipment_data_input["serialNumber"]))
                return

            vessel[equipment_name].append(equipment_data_input)
            print(f"Added {equipment_name} to {primary_vessel_name}")

            print(json.dumps(self.site["surveyVessels"], indent=2))

    def add_atd_offsets(self, primary_vessel_name: str, atd_data_input: dict, output, event=None):
        with output:
            print("Adding acoustic to transducer offsets to survey vessel..")
            if not atd_data_input['x'] or not atd_data_input['y'] or not atd_data_input['z']:
                print("Offsets not provided, please provide all offsets..")
                return
            
            vessel = next((primary_vessel for primary_vessel in self.site["surveyVessels"] if primary_vessel["name"] == primary_vessel_name), None)
            if vessel is None:
                print("Primary survey vessel {} not found, ensure you have the correct vessel name".format(primary_vessel_name))
                return
            
            if "atdOffsets" not in vessel:
                vessel["atdOffsets"] = [atd_data_input]
                print("Added acoustic to transducer offsets to survey vessel")
                print(json.dumps(self.site["surveyVessels"], indent=2))
                return

    def existing_vessel_equipment(self, primary_vessel_name: str, equipment_name: str, equipment: dict, output, event=None):
        """ Update existing equipment on a survey vessel using the serial number """
        with output:
            if equipment_name not in survey_vessels_types:
                print("Survey vessel equipment type not found, ensure you have the correct vessel type")
                return

            if not equipment.get("serialNumber"):
                print("Serial number not provided, serial number is required for updating..")
                return

            vessel = next((primary_vessel for primary_vessel in self.site["surveyVessels"] if primary_vessel["name"] == primary_vessel_name), None)
            if vessel is None:
                print(f"Survey vessel {primary_vessel_name} not found")
                return

            equipment_list = vessel.get(equipment_name, [])
            existing_equipment = next((equip for equip in equipment_list if equip["serialNumber"] == equipment["serialNumber"]), None)
            if existing_equipment is None:
                print(f"Equipment {equipment_name} with serial number {equipment['serialNumber']} not found on vessel {primary_vessel_name}")
                return

            for key, value in equipment.items():
                if value:
                    existing_equipment[key] = value

            print(f"Updated {equipment_name} with serial number {equipment['serialNumber']}")
            print(json.dumps(self.site["surveyVessels"], indent=2))
                

    def new_survey(self, survey_input: dict, campaign_name, output, event=None):
        with output:
            print("Adding survey to campaign {}".format(campaign_name))
            campaign = next((camp for camp in self.site["campaigns"] if camp["name"] == campaign_name), None)
            
            if campaign is None:
                print("Campaign not found, ensure you have the correct campaign name")
                return

            if any(survey["id"] == survey_input["id"] for survey in campaign["surveys"]):
                print("Survey already exists in campaign.. please update instead of adding")
                return

            if not survey_input["id"]:
                print("No survey ID provided, generating one..")
                survey_input['id'] = campaign_name + "_" + str(len(campaign["surveys"]) + 1)
                print("Generated survey ID: " + survey_input['id'])

            campaign["surveys"].append(survey_input)
            print("Added survey to site class..")
            print(json.dumps(self.site["campaigns"], indent=2))


    def existing_survey(self, survey_input: dict, campaign_name, survey_id, output, event=None):
        with output:
            print("Updating survey {} in campaign {}".format(survey_id, campaign_name))
            campaign = next((c for c in self.site["campaigns"] if c["name"] == campaign_name), None)
            if campaign is None:
                print("Campaign not found, ensure you have the correct campaign name")
                return

            campaign_survey = next((survey for survey in campaign["surveys"] if survey["id"] == survey_id), None)
            if campaign_survey is None:
                print(f"Survey with ID {survey_id} not found in campaign {campaign_name}")
                return

            print("Updating survey " + survey_id)
            for key, value in survey_input.items():
                if value:
                    campaign_survey[key] = value

            print("Updated survey " + survey_id)
            print(json.dumps(self.site["campaigns"], indent=2))


    def existing_benchmark(self, benchmark_input: dict, output, event=None):
        with output:
            site_benchmark = next((benchmark for benchmark in self.site["benchmarks"] if benchmark["name"] == benchmark_input["name"]), None)
            
            if site_benchmark is None:
                print("Benchmark {} not found, ensure you have the correct benchmark name".format(benchmark_input["name"]))
                return

            print("Updating " + site_benchmark["name"])
            for key, value in benchmark_input.items():
                if value:
                    site_benchmark[key] = value

            print("Updated benchmark " + site_benchmark["name"])
            print(json.dumps(self.site["benchmarks"], indent=2))

    def add_transponder_to_benchmark(self, benchmark_name, transponder_input: dict, output, event=None):
        with output:
            benchmark = next((b for b in self.site["benchmarks"] if b["name"] == benchmark_name), None)
            
            if benchmark is None:
                print(f"Benchmark {benchmark_name} not found, ensure you have the correct benchmark name")
                return
            
            if not transponder_input["address"]:
                print("Transponder address not provided, address is required for adding..")
                return

            existing_transponder = next((trans for trans in benchmark.get("transponders", []) if trans["address"] == transponder_input["address"]), None)
            
            if existing_transponder:
                print("Transponder {} already exists in benchmark.. please update instead of adding".format(transponder_input["address"]))
                return

            benchmark.setdefault("transponders", []).append(transponder_input)
            print("Added transponder")
            print(json.dumps(self.site["benchmarks"], indent=2))


    def existing_transponder(self, benchmark_name: str, transponder_input: dict, output, event=None):
        with output:
            benchmark = next((b for b in self.site["benchmarks"] if b["name"] == benchmark_name), None)
            
            if benchmark is None:
                print(f"Benchmark {benchmark_name} not found, ensure you have the correct benchmark name")
                return
            
            if not transponder_input["address"]:
                print("Transponder address not provided, address is required for updating..")
                return

            existing_transponder = next((trans for trans in benchmark.get("transponders", []) if trans["address"] == transponder_input["address"]), None)
            
            if existing_transponder is None:
                print(f"Transponder with address {transponder_input['address']} not found in benchmark {benchmark_name}")
                return

            for key, value in transponder_input.items():
                if value:
                    existing_transponder[key] = value

            print(f"Updated transponder {transponder_input['uid']}")
            print(json.dumps(self.site["benchmarks"], indent=2))




# todo: add this functionality to the site class in the future
# def existing_atd_offsets(self, primary_vessel_name: str, atd_data_input: dict, output, event=None):
#     with output:
#         print("Updating acoustic to transducer offsets on survey vessel..")
#         if not atd_data_input['x'] or not atd_data_input['y'] or not atd_data_input['z']:
#             print("Offsets not provided, please provide all offsets..")
#             return
        
#         vessel = next((primary_vessel for primary_vessel in self.site["surveyVessels"] if primary_vessel["name"] == primary_vessel_name), None)
#         if vessel is None:
#             print("Primary survey vessel {} not found, ensure you have the correct vessel name".format(primary_vessel_name))
#             return

#         if "atdOffsets" not in vessel:
#             print("No acoustic to transducer offsets found on survey vessel..")
#             return


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