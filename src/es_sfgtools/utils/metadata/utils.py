import json
from ipywidgets import Layout
from datetime import datetime
from typing import Union, Dict, Any


style = {'description_width': 'initial'}
layout=Layout(width='30%', height='40px')

def convert_to_datetime(date_str: Union[str, datetime]) -> datetime:
    """
    Convert ISO string format to datetime if a string is provided.

    Args:
        date_str (Union[str, datetime]): The date string or datetime object to convert.

    Returns:
        datetime: The converted datetime object.

    Raises:
        ValueError: If the date string is not in a valid ISO format.
    """
    if isinstance(date_str, str):
        try:
            return datetime.fromisoformat(date_str)
        except ValueError:
            print("Invalid date format, please provide a valid date in format YYYY-MM-DDTHH:MM:SS")
            raise
    return date_str

def start_and_end_dates(start_date: datetime, end_date: datetime, dict_to_update: dict) -> dict:
    """
    Add or update the start and end dates in the dictionary.
    """

    # Convert ISO string format to datetime if a string is provided
    try:
        if start_date:
            start_date = convert_to_datetime(start_date)
        if end_date:
            end_date = convert_to_datetime(end_date) 
    except ValueError:
        raise

    # Check that it is a reasonable date >1990 and not the default date
    if start_date:
        if start_date >= datetime(year=1990, month=1, day=1):
            dict_to_update['start'] = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            print("Date too old. Not entering start date.")

    if end_date:
        if end_date >= datetime(year=1990, month=1, day=1):
            dict_to_update['end'] = end_date.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            print("Date too old. Not entering end date.")

    # # Check that the start date is not greater than the end date, if so raise an error
    # if dict_to_update['end']:
    #     if start_date > end_date:
    #         print("Start date is greater than end date.. Please check the dates")
    #         raise ValueError("Start date is greater than end date.. Please check the dates entered")

    print("Check your site output to confirm.. \n" + json.dumps(dict_to_update, indent=2))
    return dict_to_update


class AttributeUpdater:
    def update_attributes(self, additional_data: Dict[str, Any]):
        """
        Update the attributes based on the provided dictionary.

        Args:
            additional_data (Dict[str, Any]): A dictionary of additional attributes to update.
        """
        for key, value in additional_data.items():
            if value:
                if hasattr(self, key):
                    print(f"Updating attribute '{key}' with value '{value}'")
                    setattr(self, key, value)
                else:
                    print(f"Unknown attribute '{key}' provided in additional data")


def only_one_is_true(*args):
    """
    Check that only one of the arguments is True.
    """
    return sum(args) == 1

# TODO: add this functionality to the site class in the future
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