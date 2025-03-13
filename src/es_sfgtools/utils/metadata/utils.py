import json
import os
from datetime import datetime
from typing import Optional, Union, Dict, Any
import copy

from pydantic import BaseModel, field_validator

def parse_datetime(cls, value):
    # Check that the date is a string and convert it to a datetime object
    if not value:
        # If an empty string, ignore it
        return None
    
    if isinstance(value, str):
        return convert_to_datetime(value)
    
    elif isinstance(value, datetime):
        return value
    
    else:
        raise ValueError('Invalid date format')

def check_dates(cls, end, values):
    start = values.data['start']

    # Check that the end date is a string and convert it to a datetime object
    if isinstance(end, str):
        end = convert_to_datetime(end)

    # Check that the start date is before the end date
    if start and end and start > end:
        raise ValueError('End date must not be before start date')
    
    return end

def if_zero_than_none(cls, value):
    # Check if the field is the number 0 and replace it with None
    if value == 0:
        return None
    return value

def check_optional_fields_for_empty_strings(cls, value):
    # Check if the field is the string but is empty and replace it with None
    if isinstance(value, str) and not value:
        return None
    return value

class AttributeUpdater:
    def update_attributes(self, additional_data: Dict[str, Any]):
        """
        Update the class attributes based on the provided dictionary. Handles nested objects with the AttributeUpdater (e.g Location) class.

        Args:
            additional_data (Dict[str, Any]): A dictionary of additional attributes to update.
        """
        for key, value in additional_data.items():
            if value:
                if hasattr(self, key):
                    if isinstance(value, dict) and isinstance(getattr(self, key), AttributeUpdater):
                        getattr(self, key).update_attributes(value)
                    else:
                        self.set_value(key, value)
                else:
                    print(f"Unknown attribute '{key}' provided in additional data")

    def set_value(self, key, value):
        """Set the value of an attribute and update the class instance. Also validates the updated instance."""
        updated_data = self.model_dump()  # Get current data
        updated_data[key] = value  # Update the field
        new_instance = self.model_validate(updated_data)  # Validate
        self.__dict__.update(new_instance.__dict__)  # Apply the update


def only_one_is_true(*args):
    """
    Check that only one of the arguments is True.
    """
    return sum(args) == 1

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


def convert_custom_objects_to_dict(d: dict) -> dict:
    """
    Recursively convert custom objects in the dictionary to their dictionary representations.

    Args:
        d (dict): The dictionary to update.

    Returns:
        dict: The updated dictionary with custom objects converted to dictionaries.
    """
    for key, value in d.items():
        if isinstance(value, BaseModel):
            d[key] = value.model_dump()
        elif isinstance(value, datetime):
            d[key] = value.isoformat()
        elif isinstance(value, dict):
            d[key] = convert_custom_objects_to_dict(value)
    return d


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

