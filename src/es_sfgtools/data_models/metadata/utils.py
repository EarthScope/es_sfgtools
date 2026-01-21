# Description: Utility functions for metadata classes.
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, field_validator


def parse_datetime(cls, value):
    """Parse a datetime string.

    Parameters
    ----------
    cls : class
        The class.
    value : str
        The datetime string.

    Returns
    -------
    datetime
        The parsed datetime.
    """
    # Check that the date is a string and convert it to a datetime object
    if not value:
        # If an empty string, ignore it
        return None

    if isinstance(value, str):
        return convert_to_datetime(value)

    elif isinstance(value, datetime):
        return value

    else:
        raise ValueError("Invalid date format")


def check_dates(cls, end, values):
    """Check that the end date is after the start date.

    Parameters
    ----------
    cls : class
        The class.
    end : datetime
        The end date.
    values : dict
        The values.

    Returns
    -------
    datetime
        The end date.
    """
    start = values.data["start"]

    # Check that the end date is a string and convert it to a datetime object
    if isinstance(end, str):
        end = convert_to_datetime(end)

    # Check that the start date is before the end date
    if start and end and start > end:
        raise ValueError("End date must not be before start date")

    return end


def if_zero_than_none(cls, value):
    """If the value is 0, return None.

    Parameters
    ----------
    cls : class
        The class.
    value : int
        The value.

    Returns
    -------
    int | None
        The value or None.
    """
    # Check if the field is the number 0 and replace it with None
    if value == 0:
        return None
    return value


def check_fields_for_empty_strings(cls, value):
    """Check if the field is an empty string and replace it with None.

    Parameters
    ----------
    cls : class
        The class.
    value : str
        The value.

    Returns
    -------
    str | None
        The value or None.
    """
    # Check if the field is the string but is empty and replace it with None
    if isinstance(value, str) and not value:
        return None
    return value


class AttributeUpdater:
    def update_attributes(self, additional_data: Dict[str, Any]):
        """Update the class attributes based on the provided dictionary.

        This handles nested objects with the AttributeUpdater (e.g Location)
        class. This class is helpful for the notebook where the user will be
        passing empty strings if they don't want to update a field. This
        function will only reset the value if not empty. If other keys are
        provided in the dictionary, it will print a warning.

        Parameters
        ----------
        additional_data : Dict[str, Any]
            A dictionary of additional attributes to update.
        """
        for key, value in additional_data.items():
            if value:
                if hasattr(self, key):
                    if isinstance(value, dict) and isinstance(
                        getattr(self, key), AttributeUpdater
                    ):
                        getattr(self, key).update_attributes(value)
                    else:
                        self.set_value(key, value)
                else:
                    print(f"Unknown attribute '{key}' provided in additional data")

    def set_value(self, key, value):
        """Set the value of an attribute and update the class instance.

        This also validates the updated instance.

        Parameters
        ----------
        key : str
            The key.
        value : Any
            The value.
        """
        updated_data = self.model_dump()  # Get current data
        updated_data[key] = value  # Update the field
        new_instance = self.model_validate(updated_data)  # Validate
        self.__dict__.update(new_instance.__dict__)  # Apply the update


def only_one_is_true(*args):
    """Check that only one of the arguments is True.

    Parameters
    ----------
    *args
        The arguments to check.

    Returns
    -------
    bool
        True if only one of the arguments is True, False otherwise.
    """
    return sum(args) == 1


def convert_to_datetime(date_str: Union[str, datetime]) -> datetime:
    """Convert ISO string format to datetime if a string is provided.

    Parameters
    ----------
    date_str : Union[str, datetime]
        The date string or datetime object to convert.

    Returns
    -------
    datetime
        The converted datetime object, always timezone-aware in UTC.

    Raises
    ------
    ValueError
        If the date string is not in a valid ISO format.
    """
    if isinstance(date_str, str):
        try:
            dt = datetime.fromisoformat(date_str)
            # If the datetime is timezone-naive, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            print(
                "Invalid date format, please provide a valid date in format YYYY-MM-DDTHH:MM:SS"
            )
            raise
    # If it's already a datetime, ensure it's timezone-aware
    if isinstance(date_str, datetime) and date_str.tzinfo is None:
        return date_str.replace(tzinfo=timezone.utc)
    return date_str


def convert_custom_objects_to_dict(d: dict) -> dict:
    """Recursively convert custom objects in a dictionary to dictionaries.

    Parameters
    ----------
    d : dict
        The dictionary to update.

    Returns
    -------
    dict
        The updated dictionary with custom objects converted to
        dictionaries.
    """
    for key, value in d.items():
        if isinstance(value, BaseModel):
            d[key] = value.model_dump()
        elif isinstance(value, datetime):
            d[key] = value.isoformat()
        elif isinstance(value, dict):
            d[key] = convert_custom_objects_to_dict(value)
    return d


class Location(AttributeUpdater, BaseModel):
    latitude: Optional[float] = Field(
        default=None, description="The latitude of the location.", ge=-90, le=90
    )
    longitude: Optional[float] = Field(
        default=None, description="The longitude of the location.", ge=-180, le=180
    )
    elevation: Optional[float] = Field(
        default=None, description="The elevation of the location."
    )

    _if_zero_than_none = field_validator("latitude", "longitude", "elevation")(
        if_zero_than_none
    )