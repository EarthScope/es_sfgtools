"""
Author: Franklyn Dunbar
Date: 2024-05-01
Email: franklyn.dunbar@earthscope.org, franklyn.dunbar@umontana.edu
"""

from datetime import datetime, timezone

# Default station TAT offsets in milliseconds
STATION_OFFSETS = {
    "5209": 200,
    "5210": 320,
    "5211": 440,
    "5212": 560
}

ADJ_LEAP = 1.0  # this is the leap second adjustment TODO Ask James why this is there
TRIGGER_DELAY_SV2 = 0.1  # SV2 trigger delay in seconds
TRIGGER_DELAY_SV3 = 0.13  # SV3 trigger delay in seconds
GNSS_START_TIME = datetime(1980, 1, 6, tzinfo=timezone.utc) # GNSS start time

MASTER_STATION_ID = {
    "0": "5209",
    "1": "5210",
    "2": "5211",
    "3": "5212"
}
