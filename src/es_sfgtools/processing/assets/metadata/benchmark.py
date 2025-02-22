from typing import Any, Dict, List
from datetime import datetime

from es_sfgtools.utils.metadata.utils import AttributeUpdater, Location, convert_to_datetime



class Benchmark(AttributeUpdater):
    def __init__(self, name: str = None, additional_data: Dict[str, Any] = None, existing_benchmark: Dict[str, Any] = None):

        if existing_benchmark:
            self.import_exisiting_benchmark(existing_benchmark)
            return
        
        self.name: str = name
        self.benchmarkID: str = ""  
        self.dropPointLocation = Location()
        self.aPrioriLocation = Location()
        self.start: datetime = None
        self.end: datetime = None
        self.transponders: List[Transponder] = []

        if additional_data:
            self.update_attributes(additional_data)

    def import_exisiting_benchmark(self, existing_benchmark: Dict[str, Any]):
        """
        Import an existing benchmark from a dictionary.

        Args:
            existing_benchmark (Dict[str, Any]): A dictionary containing the existing benchmark data.
        """
        self.name = existing_benchmark.get("name")  # Required
        self.benchmarkID = existing_benchmark.get("benchmarkID", "")

        drop_point_location_data = existing_benchmark.get("dropPointLocation", {})
        if isinstance(drop_point_location_data, dict):
            self.dropPointLocation = Location(additional_data=drop_point_location_data)
    
        a_priori_location_data = existing_benchmark.get("aPrioriLocation", {})
        if isinstance(a_priori_location_data, dict):
            self.aPrioriLocation = Location(additional_data=a_priori_location_data)

        start_time = existing_benchmark.get("start", "")
        if start_time:
            self.start = convert_to_datetime(start_time)
        else:
            self.start = None

        end_time = existing_benchmark.get("end", "")
        if end_time:
            self.end = convert_to_datetime(end_time)
        else:
            self.end = None

        self.transponders = [Transponder(existing_transponder=tran) for tran in existing_benchmark.get("transponders", [])]

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Benchmark instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Benchmark instance.
        """
        return {
            "name": self.name,
            "benchmarkID": self.benchmarkID if self.benchmarkID else "",
            "dropPointLocation": self.dropPointLocation.to_dict() if self.dropPointLocation else {}, 
            "aPrioriLocation": self.aPrioriLocation.to_dict() if self.aPrioriLocation else {},
            "start": self.start.strftime('%Y-%m-%dT%H:%M:%S') if self.start else "",
            "end": self.end.strftime('%Y-%m-%dT%H:%M:%S') if self.end else "",
            "transponders": [transponder.to_dict() for transponder in self.transponders]
        }
    
class Transponder(AttributeUpdater):
    def __init__(self, address: str = None, additional_data: Dict[str, Any] = None, existing_transponder: Dict[str, Any] = None):
        if existing_transponder:
            self.import_existing_transponder(existing_transponder)
            return
        
        self.address: str = address
        self.uid: str = ""
        self.model: str = ""
        self.serialNumber: str = ""
        self.batteryCapacity: str = ""
        self.tat: float = 0.0
        self.start: datetime = None
        self.end: datetime = None
        self.notes: str = ""
        self.batteryVoltage: List[Any] = []
        self.extraSensors: List[Any]  = []

        if additional_data:
            self.update_attributes(additional_data)

    def import_existing_transponder(self, existing_transponder: Dict[str, Any]):
        """
        Import an existing transponder from a dictionary.
        """
        self.address = existing_transponder.get("address", "")
        self.uid = existing_transponder.get("uid", "")
        self.model = existing_transponder.get("model", "")
        self.serialNumber = existing_transponder.get("serialNumber", "")    
        self.batteryCapacity = existing_transponder.get("batteryCapacity", "")
        self.tat = existing_transponder.get("tat", 0.0)

        start_time = existing_transponder.get("start", "")
        if start_time:
            self.start = convert_to_datetime(start_time)
        else:
            self.start = None

        end_time = existing_transponder.get("end", "")
        if end_time:
            self.end = convert_to_datetime(end_time)
        else:
            self.end = None

        self.notes = existing_transponder.get("notes", "")
        self.batteryVoltage = existing_transponder.get("batteryVoltage", [])
        self.extraSensors = existing_transponder.get("extraSensors", [])

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Transponder instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Transponder instance.
        """
        return {
            "address": str(self.address),
            "uid": str(self.uid) if self.uid else "",
            "model": str(self.model) if self.model else "",
            "serialNumber": str(self.serialNumber) if self.serialNumber else "",
            "batteryCapacity": str(self.batteryCapacity) if self.batteryCapacity is not None else "",
            "tat": float(self.tat) if self.tat is not None else 0.0,
            "start": self.start.strftime('%Y-%m-%dT%H:%M:%S') if self.start else "",
            "end": self.end.strftime('%Y-%m-%dT%H:%M:%S') if self.end else "",
            "notes": str(self.notes) if self.notes else "",
            "batteryVoltage": self.batteryVoltage,
            "extraSensors": self.extraSensors
        }

