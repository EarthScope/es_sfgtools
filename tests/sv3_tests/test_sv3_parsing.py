import json
from pathlib import Path
from es_sfgtools.data_models.sv3_models import (
    NovatelInterrogationEvent,
    NovatelRangeEvent)
RESOURCES = Path(__file__).parent.parent / "resources"
SAMPLE_DFO = RESOURCES / "sv3/dfo_ncc1_2022_A_1065_329653_002_20220501_021315_00082_DFOP00_sample.json"

class TestSV3Parsing:

    def test_dfo_parsing(self):
        with open(SAMPLE_DFO, "r") as f:
            data = json.load(f)
        for event in data:
            if event.get("event") == "interrogation":
                interrogation = NovatelInterrogationEvent(**event)
            elif event.get("event") == "range":
                range_event = NovatelRangeEvent(**event)
            else:
                print(f"Unknown event type: {event.get('event')}")