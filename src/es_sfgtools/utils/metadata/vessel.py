import json
survey_vessels_types = ["insPayloads", "gnssReceivers", "gnssAntennas", "acousticTransducer", 
                        "acousticTransceiver", "atdOffsets", "imuSensors"]

vessel_buttons_descriptions = {
    "new_survey_vessel": "Add new survey vessel",
    "new_imu_sensor": "Add new imu sensor",
    "new_receiver": "Add new GNSS Receiver",
    "new_antenna": "Add new GNSS Antenna",
    "new_transducer": "Add new acoustic transducer",
    "new_transceiver": "Add new acoustic transceiver",
    "new_atd": "Add new acoustic to transducer offsets",
    "existing_survey_vessel": "Update existing survey vessel",
    "existing_imu_sensor": "Update existing imu sensor",
    "existing_receiver": "Update existing GNSS Receiver",
    "existing_antenna": "Update existing GNSS Antenna",
    "existing_transducer": "Update existing acoustic transducer",
    "existing_transceiver": "Update existing acoustic transceiver",
    "existing_atd": "Update existing acoustic to transducer offsets",
    "delete_survey_vessel": "Delete survey vessel",
    "delete_imu_sensor": "Delete imu sensor",
    "delete_receiver": "Delete GNSS Receiver",
    "delete_antenna": "Delete GNSS Antenna",
    "delete_transducer": "Delete acoustic transducer",
    "delete_transceiver": "Delete acoustic transceiver",
    "delete_atd": "Delete acoustic to transducer offsets",
}

def import_vessel(filepath: str):
    """Import vessel data from a JSON file."""
    with open(filepath, 'r') as file:
        return Vessel(existing_vessel=json.load(file))

class Vessel:
    def __init__(self, name, existing_vessel: dict = None) -> None:

        if existing_vessel:
            self.vessel = existing_vessel
        else:
            self.vessel = {
                "name": name,
                "insPayloads": [],
                "gnssReceivers": [],
                "gnssAntennas": [],
                "acousticTransducer": [],
                "acousticTransceiver": [],
                "atdOffsets": [],
                "imuSensors": []
            }

    def export_vesel(self, filepath: str):
        """Export site data to a JSON file."""

        print("Exporting vessel data to: " + filepath)
        with open(filepath, 'w') as file:
            json.dump(self.vessel, file, indent=4)
        print("Exported vessel data to: " + filepath)

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