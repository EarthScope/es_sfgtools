from typing import Any, Dict, List

from es_sfgtools.utils.metadata.utils import AttributeUpdater


class Benchmark(AttributeUpdater):
    def __init__(self, name: str = None, additional_data: Dict[str, Any] = None, existing_benchmark: Dict[str, Any] = None):

        if existing_benchmark:
            self.import_exisiting_benchmark(existing_benchmark)
            return
        
        self.name = name
        self.benchmarkID = ""  
        self.dropPointLocation= {'latitude': "", 'longitude': "", 'elevation': ""}
        self.aPrioriLocation = {'latitude': "", 'longitude': "", 'elevation': ""}
        self.start = ""
        self.end = ""
        self.transponders: List[Transponder] = []

        if additional_data:
            self.update_attributes(additional_data)

    def import_exisiting_benchmark(self, existing_benchmark: Dict[str, Any]):
        """
        Import an existing benchmark from a dictionary.

        Args:
            existing_benchmark (Dict[str, Any]): A dictionary containing the existing benchmark data.
        """
        self.name = existing_benchmark.get("name", "")
        self.benchmarkID = existing_benchmark.get("benchmarkID", "")
        self.dropPointLocation = existing_benchmark.get("dropPointLocation", {'latitude': "", 'longitude': "", 'elevation': ""})
        self.aPrioriLocation = existing_benchmark.get("aPrioriLocation", {'latitude': "", 'longitude': "", 'elevation': ""})
        self.start = existing_benchmark.get("start", "")
        self.end = existing_benchmark.get("end", "")
        self.transponders = [Transponder(address=transponder["address"], additional_data=transponder) for transponder in existing_benchmark["transponders"]]

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Benchmark instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Benchmark instance.
        """
        return {
            "name": self.name,
            "benchmarkID": self.benchmarkID,
            "dropPointLocation": self.dropPointLocation,
            "aPrioriLocation": self.aPrioriLocation,
            "start": self.start,
            "end": self.end,
            "transponders": [transponder.to_dict() for transponder in self.transponders]
        }
    
class Transponder(AttributeUpdater):
    def __init__(self, address: str, additional_data: Dict[str, Any] = None):
        self.address = address
        self.uid = ""
        self.model = ""
        self.serialNumber = ""
        self.batteryCapacity = ""
        self.tat = ""
        self.start = ""
        self.end = ""
        self.notes = ""
        self.batteryVoltage = []
        self.extraSensors = []

        if additional_data:
            self.update_attributes(additional_data)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Transponder instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Transponder instance.
        """
        return {
            "address": self.address,
            "uid": self.uid,
            "model": self.model,
            "serialNumber": self.serialNumber,
            "batteryCapacity": self.batteryCapacity,
            "tat": self.tat,
            "start": self.start,
            "end": self.end,
            "notes": self.notes,
            "batteryVoltage": self.batteryVoltage,
            "extraSensors": self.extraSensors
        }

