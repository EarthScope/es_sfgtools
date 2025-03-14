from datetime import datetime
from enum import Enum
import json
from typing import Any, ClassVar, Dict, List, Optional
from pydantic import BaseModel, Field, ValidationError, field_validator

from es_sfgtools.utils.metadata.utils import (
    AttributeUpdater,
    check_fields_for_empty_strings,
    only_one_is_true,
    parse_datetime,
    check_dates,
)


class EquipmentType(str, Enum):
    IMU_SENSORS = "imuSensors"
    ATD_OFFSETS = "atdOffsets"
    GNSS_ANTENNAS = "gnssAntennas"
    GNSS_RECEIVERS = "gnssReceivers"
    ACOUSTIC_TRANSCEIVER = "acousticTransceiver"
    ACOUSTIC_TRANSDUCER = "acousticTransducer"


class AtdOffset(AttributeUpdater, BaseModel):
    transducerSerialNumber: Optional[str] = Field(default=None)
    x: float
    y: float
    z: float


class GnssAntenna(AttributeUpdater, BaseModel):
    # Required
    type: str
    serialNumber: str
    start: datetime = Field(..., gt=datetime(1901, 1, 1))

    # Optional
    order: Optional[str] = Field(default=None)
    model: Optional[str] = Field(default=None)
    radomeSerialNumber: Optional[str] = Field(default=None)
    end: Optional[datetime] = Field(default=None, gt=datetime(1901, 1, 1))

    # Validators
    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end")(check_dates)
    _check_strings = field_validator(
        "model", "type", "serialNumber", "order", "radomeSerialNumber"
    )(check_fields_for_empty_strings)


class GnssReceiver(AttributeUpdater, BaseModel):
    # Required
    type: str
    serialNumber: str
    start: datetime = Field(..., gt=datetime(1901, 1, 1))

    # Optional
    model: Optional[str] = Field(default=None)
    firmwareVersion: Optional[str] = Field(default=None)
    end: Optional[datetime] = Field(default=None, gt=datetime(1901, 1, 1))

    # Validators
    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end")(check_dates)
    _check_strings = field_validator(
        "model", "firmwareVersion", "type", "serialNumber"
    )(check_fields_for_empty_strings)


class AcousticTransducer(AttributeUpdater, BaseModel):
    # Required
    type: str
    serialNumber: str
    frequency: str
    start: datetime = Field(..., gt=datetime(1901, 1, 1))

    # Optional
    end: Optional[datetime] = Field(default=None, gt=datetime(1901, 1, 1))

    # Validators
    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end")(check_dates)
    _check_strings = field_validator("type", "serialNumber", "frequency")(
        check_fields_for_empty_strings
    )


class AcousticTransceiver(AttributeUpdater, BaseModel):
    # Required
    type: str
    serialNumber: str
    frequency: str
    start: datetime = Field(..., gt=datetime(1901, 1, 1))

    # Optional
    triggerDelay: Optional[float] = Field(default=None)
    delayIncludedInTWTT: Optional[bool] = Field(default=None)
    end: Optional[datetime] = Field(default=None, gt=datetime(1901, 1, 1))

    # Validators
    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end")(check_dates)
    _check_strings = field_validator("type", "serialNumber", "frequency")(
        check_fields_for_empty_strings
    )


class ImuSensor(AttributeUpdater, BaseModel):
    # Required
    type: str
    serialNumber: str
    start: datetime = Field(..., gt=datetime(1901, 1, 1))

    # Optional
    model: Optional[str] = Field(default=None)
    end: Optional[datetime] = Field(default=None, gt=datetime(1901, 1, 1))

    # Validators
    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end", mode="after")(check_dates)
    _check_strings = field_validator("type", "serialNumber", "model")(
        check_fields_for_empty_strings
    )


class Vessel(AttributeUpdater, BaseModel):
    # Required
    name: str = Field(..., description="The 4 digit name of the vessel")
    type: str = Field(..., description="The type of the vessel. e.g. waveglider")
    model: str = Field(..., description="The model of the vessel")

    # Optional
    serialNumber: Optional[str] = Field(default=None)
    start: Optional[datetime] = Field(default=None, gt=datetime(1901, 1, 1))
    end: Optional[datetime] = Field(default=None, gt=datetime(1901, 1, 1))
    imuSensors: List[ImuSensor] = Field(default_factory=list)
    atdOffsets: List[AtdOffset] = Field(default_factory=list)
    gnssAntennas: List[GnssAntenna] = Field(default_factory=list)
    gnssReceivers: List[GnssReceiver] = Field(default_factory=list)
    acousticTransducers: List[AcousticTransducer] = Field(default_factory=list)
    acousticTransceivers: List[AcousticTransceiver] = Field(default_factory=list)

    # Validators
    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end")(check_dates)
    _check_strings = field_validator("name", "type", "model", "serialNumber")(
        check_fields_for_empty_strings
    )

    # Map of equipment types to their respective lists and classes - used for adding, updating and deleting equipment
    equipment_map: ClassVar[Dict[str, Any]] = {
        EquipmentType.IMU_SENSORS: (lambda self: self.imuSensors, ImuSensor),
        EquipmentType.GNSS_ANTENNAS: (lambda self: self.gnssAntennas, GnssAntenna),
        EquipmentType.GNSS_RECEIVERS: (lambda self: self.gnssReceivers, GnssReceiver),
        EquipmentType.ACOUSTIC_TRANSDUCER: (
            lambda self: self.acousticTransducers,
            AcousticTransducer,
        ),
        EquipmentType.ACOUSTIC_TRANSCEIVER: (
            lambda self: self.acousticTransceivers,
            AcousticTransceiver,
        ),
        EquipmentType.ATD_OFFSETS: (lambda self: self.atdOffsets, AtdOffset),
    }

    @field_validator("name", "type", "model")
    def check_required_fields(cls, value):
        if not value:
            raise ValueError("Required field {} is empty".format(value))
        return value

    def export_vessel(self, filepath: str):
        with open(filepath, "w") as file:
            file.write(self.model_dump_json(indent=2))

    @classmethod
    def from_json(cls, filepath: str) -> "Vessel":
        with open(filepath, "r") as file:
            return cls(**json.load(file))

    def print_json(self):
        print(self.model_dump_json(indent=2))

    def run_equipment(
        self,
        serial_number: str,
        equipment_type: EquipmentType,
        equipment_metadata: dict,
        add_new: bool = False,
        update: bool = False,
        delete: bool = False,
    ):

        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one action to perform.")
            return

        if not serial_number:
            print("ERROR: Serial number is required.")
            return

        if add_new:
            self._new_equipment(serial_number, equipment_type, equipment_metadata)
        elif update:
            self._update_equipment(serial_number, equipment_type, equipment_metadata)
        elif delete:
            self._delete_equipment(serial_number, equipment_type)

    def _new_equipment(
        self,
        serial_number: str,
        equipment_type: EquipmentType,
        equipment_metadata: dict,
    ):
        """Add a new survey vessel equipment."""

        print(f"Adding new {equipment_type}..")

        if equipment_type not in self.equipment_map:
            print(f"ERROR: Invalid equipment type: {equipment_type}")
            return

        equipment_list, equipment_class = self.equipment_map[equipment_type]
        equipment_list = equipment_list(self)

        for equipment in equipment_list:
            if equipment.serialNumber == serial_number:
                print(
                    f"ERROR: {equipment_type} with serial number {serial_number} already exists, choose to update instead."
                )
                print(equipment.model_dump_json(indent=2))
                return

        try:
            equipment_metadata["serialNumber"] = serial_number
            new_equipment = equipment_class(**equipment_metadata)
        except ValidationError as e:
            print(f"Validation error for {equipment_type}: {e}")
            return

        equipment_list.append(new_equipment)
        print(new_equipment.model_dump_json(indent=2))
        print(f"New {equipment_type} added successfully.")

    def _update_equipment(
        self,
        serial_number: str,
        equipment_type: EquipmentType,
        equipment_metadata: dict,
    ):
        """Update the attributes of a survey vessel equipment."""

        print(f"Updating {equipment_type} with serial number {serial_number}..")

        if equipment_type not in self.equipment_map:
            print(f"ERROR: Invalid equipment type: {equipment_type}")
            return

        equipment_list, _ = self.equipment_map[equipment_type]
        equipment_list = equipment_list(self)

        for equipment in equipment_list:
            if equipment.serialNumber == serial_number:
                equipment_metadata["serialNumber"] = serial_number
                equipment.update_attributes(equipment_metadata)
                print(equipment.model_dump_json(indent=2))
                print(
                    f"{equipment_type} with serial number {serial_number} updated successfully."
                )
                return

        print(f"ERROR: {equipment_type} with serial number {serial_number} not found.")

    def _delete_equipment(self, serial_number: str, equipment_type: EquipmentType):
        """Delete a survey vessel equipment."""

        print(f"Deleting {equipment_type} with serial number {serial_number}..")

        if equipment_type not in self.equipment_map:
            print(f"ERROR: Invalid equipment type: {equipment_type}")
            return

        equipment_list, _ = self.equipment_map[equipment_type]
        equipment_list = equipment_list(self)

        for i, equipment in enumerate(equipment_list):
            if equipment.serialNumber == serial_number:
                equipment_list.pop(i)
                print(
                    f"{equipment_type} with serial number {serial_number} deleted successfully."
                )
                return

        print(f"ERROR: {equipment_type} with serial number {serial_number} not found.")


def import_vessel(filepath: str) -> Vessel:
    """Import vessel data from a JSON file."""
    with open(filepath, "r") as file:
        return Vessel(**json.load(file))


if __name__ == "__main__":
    vessel_json_file_path = (
        "/Users/terry/repos/seafloor_geodesy_notebooks/notebooks/1126.json"
    )
    vessel_data = json.load(open(vessel_json_file_path))

    vessel_class = import_vessel(vessel_json_file_path)
