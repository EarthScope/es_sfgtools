import json
from typing import Any, Dict, List

from es_sfgtools.utils.metadata.utils import AttributeUpdater, only_one_is_true

survey_vessels_types = ["imuSensors", "gnssReceivers", "gnssAntennas", "acousticTransducer", 
                        "acousticTransceiver", "atdOffsets", "imuSensors"]

class AtdOffset(AttributeUpdater):
    def __init__(self, transducer_serial_number, additional_data: Dict[str, Any] = None):
        self.transducerSerialNumber = transducer_serial_number
        self.x = ""
        self.y = ""
        self.z = ""

        if additional_data:
            self.update_attributes(additional_data)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transducerSerialNumber": self.transducerSerialNumber,
            "x": self.x,
            "y": self.y,
            "z": self.z
        }

class GnssAntenna(AttributeUpdater):
    def __init__(self, serial_number: str, additional_data: Dict[str, Any] = None):
        self.order = ""
        self.type = ""
        self.model = ""
        self.serialNumber = serial_number
        self.radomeSerialNumber = ""
        self.start = ""
        self.end = ""

        if additional_data:
            self.update_attributes(additional_data)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order": self.order,
            "type": self.type,
            "model": self.model,
            "serialNumber": self.serialNumber,
            "radomeSerialNumber": self.radomeSerialNumber,
            "start": self.start,
            "end": self.end
        }
    
class GnssReceiver(AttributeUpdater):
    def __init__(self, serial_number: str, additional_data: Dict[str, Any] = None):
        self.type = ""
        self.model = ""
        self.serialNumber = serial_number
        self.firmwareVersion = ""
        self.start = ""
        self.end = ""

        if additional_data:
            self.update_attributes(additional_data)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "model": self.model,
            "serialNumber": self.serialNumber,
            "firmwareVersion": self.firmwareVersion,
            "start": self.start,
            "end": self.end
        }

class AcousticTransducer(AttributeUpdater):
    def __init__(self, serial_number: str, additional_data: Dict[str, Any] = None):
        self.type = ""
        self.serialNumber = serial_number
        self.frequency = ""
        self.start = ""
        self.end = ""

        if additional_data:
            self.update_attributes(additional_data)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "serialNumber": self.serialNumber,
            "frequency": self.frequency,
            "start": self.start,
            "end": self.end
        }

class AcousticTransceiver(AttributeUpdater):
    def __init__(self, serial_number: str, additional_data: Dict[str, Any] = None):
        self.type = ""
        self.serialNumber = serial_number
        self.frequency = ""
        self.triggerDelay = ""
        self.delayIncludedInTWTT = ""
        self.start = ""
        self.end = ""

        if additional_data:
            self.update_attributes(additional_data)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "serialNumber": self.serialNumber,
            "frequency": self.frequency,
            "triggerDelay": self.triggerDelay,
            "delayIncludedInTWTT": self.delayIncludedInTWTT,
            "start": self.start,
            "end": self.end
        }
    
class ImuSensor(AttributeUpdater):
    def __init__(self, serial_number: str, additional_data: Dict[str, Any] = None):
        self.type = ""
        self.model = ""
        self.serialNumber = serial_number
        self.start = ""
        self.end = ""

        if additional_data:
            self.update_attributes(additional_data)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "model": self.model,
            "serialNumber": self.serialNumber,
            "start": self.start,
            "end": self.end
        }
    


class Vessel:
    def __init__(self, name: str = None, type: str = None, model: str = None, serial_number: str = None,
                 start: str = None, end: str = None, existing_vessel: Dict[str, Any] = None):
        if existing_vessel:
            self.import_existing_vessel(existing_vessel)
        else:
            self.name = name
            self.type = type if type else ""
            self.model = model if model else ""
            self.serial_number = serial_number if serial_number else ""
            self.start = start if start else ""
            self.end = end if end else ""
            self.atd_offsets: List[AtdOffset] = []
            self.imu_sensors: List[ImuSensor] = []
            self.gnss_antennas: List[GnssAntenna] = []
            self.gnss_receivers: List[GnssReceiver] = []
            self.acoustic_transducers: List[AcousticTransducer] = []
            self.acoustic_transceivers: List[AcousticTransceiver] = []

    def import_existing_vessel(self, existing_vessel: Dict[str, Any]):
        self.name = existing_vessel.get("name", "")
        self.type = existing_vessel.get("type", "")
        self.model = existing_vessel.get("model", "")
        self.serial_number = existing_vessel.get("serialNumber", "")
        self.start = existing_vessel.get("start", "")
        self.end = existing_vessel.get("end", "")
        self.imu_sensors = [ImuSensor(serial_number=sensor["serialNumber"], additional_data=sensor) for sensor in existing_vessel.get("imuSensors", [])]
        self.atd_offsets = [AtdOffset(**offset) for offset in existing_vessel.get("atdOffsets", [])]
        self.gnss_antennas = [GnssAntenna(serial_number=antenna["serialNumber"], additional_data=antenna) for antenna in existing_vessel.get("gnssAntennas", [])]
        self.gnss_receivers = [GnssReceiver(serial_number=receiver["serialNumber"], additional_data=receiver) for receiver in existing_vessel.get("gnssReceivers", [])]
        self.acoustic_transducers = [AcousticTransducer(serial_number=transducer["serialNumber"], additional_data=transducer) for transducer in existing_vessel.get("acousticTransducer", [])]
        self.acoustic_transceivers = [AcousticTransceiver(serial_number=transceiver["serialNumber"], additional_data=transceiver) for transceiver in existing_vessel.get("acousticTransceiver", [])]

    def export_vessel(self, filepath: str):
        with open(filepath, 'w') as file:
            json.dump(self.to_dict(), file, indent=2)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "model": self.model,
            "serialNumber": self.serial_number,
            "start": self.start,
            "end": self.end,
            "imuSensors": [sensor.to_dict() for sensor in self.imu_sensors],
            "atdOffsets": [offset.to_dict() for offset in self.atd_offsets],
            "gnssAntennas": [antenna.to_dict() for antenna in self.gnss_antennas],
            "gnssReceivers": [receiver.to_dict() for receiver in self.gnss_receivers],
            "acousticTransducer": [transducer.to_dict() for transducer in self.acoustic_transducers],
            "acousticTransceiver": [transceiver.to_dict() for transceiver in self.acoustic_transceivers]
        }


    def run_equipment(self, serial_number: str, equipment_type: str, equipment_data: dict, add_new: bool, update: bool, delete: bool):
        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one action to perform.")
            return
        
        if not serial_number:
            print("ERROR: Serial number is required.")
            return
            
        if equipment_type not in survey_vessels_types:
            print(f"ERROR: Invalid equipment type: {equipment_type}")
            return
        
        if add_new:
            self.new_equipment(serial_number, equipment_type, equipment_data)
        elif update:
            self.update_equipment(serial_number, equipment_type, equipment_data)
        elif delete:
            self.delete_equipment(serial_number, equipment_type)

    
    def new_equipment(self, serial_number: str, equipment_type: str, equipment_data: dict):
        """ Add a new survey vessel equipment. """
        
        print(f"Adding new {equipment_type}..")
        if equipment_type == "imuSensors":
            for sensor in self.imu_sensors:
                if sensor.serialNumber == serial_number:
                    print(f"ERROR: IMU sensor with serial number {serial_number} already exists, choose to update instead.")
                    print(json.dumps(sensor.to_dict(), indent=2))
                    return
                
            new_sensor = ImuSensor(serial_number=serial_number, additional_data=equipment_data)
            self.imu_sensors.append(new_sensor)
            print(json.dumps(new_sensor.to_dict(), indent=2))

        elif equipment_type == "gnssAntennas":
            for antenna in self.gnss_antennas:
                if antenna.serialNumber == serial_number:
                    print(f"ERROR: GNSS antenna with serial number {serial_number} already exists, choose to update instead.")
                    print(json.dumps(antenna.to_dict(), indent=2))
                    return
                
            new_antenna = GnssAntenna(serial_number=serial_number, additional_data=equipment_data)
            self.gnss_antennas.append(new_antenna)
            print(json.dumps(new_antenna.to_dict(), indent=2))

        elif equipment_type == "gnssReceivers":
            for receiver in self.gnss_receivers:
                if receiver.serialNumber == serial_number:
                    print(f"ERROR: GNSS receiver with serial number {serial_number} already exists, choose to update instead.")
                    print(json.dumps(receiver.to_dict(), indent=2))
                    return
                
            new_receiver = GnssReceiver(serial_number=serial_number, additional_data=equipment_data)
            self.gnss_receivers.append(new_receiver)
            print(json.dumps(new_receiver.to_dict(), indent=2))

        elif equipment_type == "acousticTransducer":
            for transducer in self.acoustic_transducers:
                if transducer.serialNumber == serial_number:
                    print(f"ERROR: Acoustic transducer with serial number {serial_number} already exists, choose to update instead.")
                    print(json.dumps(transducer.to_dict(), indent=2))
                    return
                
            new_transducer = AcousticTransducer(serial_number=serial_number, additional_data=equipment_data)
            self.acoustic_transducers.append(new_transducer)
            print(json.dumps(new_transducer.to_dict(), indent=2))

        elif equipment_type == "acousticTransceiver":
            for transceiver in self.acoustic_transceivers:
                if transceiver.serialNumber == serial_number:
                    print(f"ERROR: Acoustic transceiver with serial number {serial_number} already exists, choose to update instead.")
                    print(json.dumps(transceiver.to_dict(), indent=2))
                    return
                
            new_transceiver = AcousticTransceiver(serial_number=serial_number, additional_data=equipment_data)
            self.acoustic_transceivers.append(new_transceiver)
            print(json.dumps(new_transceiver.to_dict(), indent=2))

        elif equipment_type == "atdOffsets":
            for offset in self.atd_offsets:
                if offset.transducerSerialNumber == serial_number:
                    print(f"ERROR: Acoustic to transducer offset with serial number {serial_number} already exists, choose to update instead.")
                    print(json.dumps(offset.to_dict(), indent=2))
                    return
                
            new_offset = AtdOffset(serial_number, equipment_data)
            self.atd_offsets.append(new_offset)
            print(json.dumps(new_offset.to_dict(), indent=2))

        else:
            print(f"ERROR: Invalid equipment type: {equipment_type}")
            return

        print(f"New {equipment_type} added successfully.")

    def update_equipment(self, serial_number: str, equipment_type: str, equipment_data: dict):
        """ Update the attributes of a survey vessel equipment. """
        
        print(f"Updating {equipment_type} with serial number {serial_number}..")
        if equipment_type == "imuSensors":
            for sensor in self.imu_sensors:
                if sensor.serialNumber == serial_number:
                    sensor.update_attributes(equipment_data)
                    print(json.dumps(sensor.to_dict(), indent=2))
                    print(f"IMU sensor with serial number {serial_number} updated successfully.")
                    return
                
            print(f"ERROR: IMU sensor with serial number {serial_number} not found.")

        elif equipment_type == "gnssAntennas":
            for antenna in self.gnss_antennas:
                if antenna.serialNumber == serial_number:
                    antenna.update_attributes(equipment_data)
                    print(json.dumps(antenna.to_dict(), indent=2))
                    print(f"GNSS antenna with serial number {serial_number} updated successfully.")
                    return
                
            print(f"ERROR: GNSS antenna with serial number {serial_number} not found.")

        elif equipment_type == "gnssReceivers":
            for receiver in self.gnss_receivers:
                if receiver.serialNumber == serial_number:
                    receiver.update_attributes(equipment_data)
                    print(json.dumps(receiver.to_dict(), indent=2))
                    print(f"GNSS receiver with serial number {serial_number} updated successfully.")
                    return
                
            print(f"ERROR: GNSS receiver with serial number {serial_number} not found.")

        elif equipment_type == "acousticTransducer":
            for transducer in self.acoustic_transducers:
                if transducer.serialNumber == serial_number:
                    transducer.update_attributes(equipment_data)
                    print(json.dumps(transducer.to_dict(), indent=2))
                    print(f"Acoustic transducer with serial number {serial_number} updated successfully.")
                    return
                
            print(f"ERROR: Acoustic transducer with serial number {serial_number} not found.")

        elif equipment_type == "acousticTransceiver":
            for transceiver in self.acoustic_transceivers:
                if transceiver.serialNumber == serial_number:
                    transceiver.update_attributes(equipment_data)
                    print(json.dumps(transceiver.to_dict(), indent=2))
                    print(f"Acoustic transceiver with serial number {serial_number} updated successfully.")
                    return
                
            print(f"ERROR: Acoustic transceiver with serial number {serial_number} not found.")

        elif equipment_type == "atdOffsets":
            for offset in self.atd_offsets:
                if offset.transducerSerialNumber == serial_number:
                    offset.update_attributes(equipment_data)
                    print(json.dumps(offset.to_dict(), indent=2))
                    print(f"Acoustic to transducer offset with serial number {serial_number} updated successfully.")
                    return
                
            print(f"ERROR: Acoustic to transducer offset with serial number {serial_number} not found.")

    def delete_equipment(self, serial_number: str, equipment_type: str):
        """ Delete a survey vessel equipment. """
        
        print(f"Deleting {equipment_type} with serial number {serial_number}..")
        if equipment_type == "imuSensors":
            for i, sensor in enumerate(self.imu_sensors):
                if sensor.serialNumber == serial_number:
                    self.imu_sensors.pop(i)
                    print(f"IMU sensor with serial number {serial_number} deleted successfully.")
                    return
                
            print(f"ERROR: IMU sensor with serial number {serial_number} not found.")

        elif equipment_type == "gnssAntennas":
            for i, antenna in enumerate(self.gnss_antennas):
                if antenna.serialNumber == serial_number:
                    self.gnss_antennas.pop(i)
                    print(f"GNSS antenna with serial number {serial_number} deleted successfully.")
                    return
                
            print(f"ERROR: GNSS antenna with serial number {serial_number} not found.")

        elif equipment_type == "gnssReceivers":
            for i, receiver in enumerate(self.gnss_receivers):
                if receiver.serialNumber == serial_number:
                    self.gnss_receivers.pop(i)
                    print(f"GNSS receiver with serial number {serial_number} deleted successfully.")
                    return
                
            print(f"ERROR: GNSS receiver with serial number {serial_number} not found.")

        elif equipment_type == "acousticTransducer":
            for i, transducer in enumerate(self.acoustic_transducers):
                if transducer.serialNumber == serial_number:
                    self.acoustic_transducers.pop(i)
                    print(f"Acoustic transducer with serial number {serial_number} deleted successfully.")
                    return
                
            print(f"ERROR: Acoustic transducer with serial number {serial_number} not found.")

        elif equipment_type == "acousticTransceiver":
            for i, transceiver in enumerate(self.acoustic_transceivers):
                if transceiver.serialNumber == serial_number:
                    self.acoustic_transceivers.pop(i)
                    print(f"Acoustic transceiver with serial number {serial_number} deleted successfully.")
                    return
                
            print(f"ERROR: Acoustic transceiver with serial number {serial_number} not found.")

        elif equipment_type == "atdOffsets":
            for i, offset in enumerate(self.atd_offsets):
                if offset.transducerSerialNumber == serial_number:
                    self.atd_offsets.pop(i)
                    print(f"Acoustic to transducer offset with serial number {serial_number} deleted successfully.")
                    return
                
            print(f"ERROR: Acoustic to transducer offset with serial number {serial_number} not found.")

def import_vessel(filepath: str) -> Vessel:
    """Import vessel data from a JSON file."""
    with open(filepath, 'r') as file:
        return Vessel(existing_vessel=json.load(file))

