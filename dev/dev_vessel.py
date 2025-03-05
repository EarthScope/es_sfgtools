from es_sfgtools.utils.metadata.vessel import Vessel
test_meta = {
    "type": "waveglider",
    "name": "1126",
    "model": "sv3",
    "serialNumber": "",
    "start": "2023-02-01T00:00:00",
    "atdOffsets": [{"x": "0.0053", "y": "0", "z": "0.92813"}],
    "gnssAntennas": [
        {
            "order": "primary",
            "type": "novatel",
            "model": "GNSS-802L",
            "serialNumber": "NMLM22110020V",
            "radomeSerialNumber": "",
            "start": "2023-02-28T00:00:00",
        },
        {
            "order": "secondary",
            "type": "novatel",
            "model": "GNSS-802L",
            "serialNumber": "NMLM22090012Z",
            "radomeSerialNumber": "",
            "start": "2023-02-28T00:00:00",
        },
    ],
    "acousticTransducer": [
        {
            "type": "641-0654",
            "serialNumber": "0",
            "frequency": "MF",
            "start": "2023-02-28T00:00:00",
        }
    ],
    "acousticTransceiver": [
        {
            "type": "",
            "serialNumber": "0",
            "frequency": "MF",
            "triggerDelay": "0.13",
            "delayIncludedInTWTT": "True",
            "start": "2023-02-28T00:00:00",
        }
    ],
}

instance = Vessel(existing_vessel=test_meta)