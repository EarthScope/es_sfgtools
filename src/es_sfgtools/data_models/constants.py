from datetime import datetime, timedelta, timezone
from decimal import getcontext,Decimal

import julian

# Set precision for Decimal operations
getcontext().prec = 10
GNSS_START_TIME = datetime(1980, 1, 6, tzinfo=timezone.utc)  # GNSS start time
GNSS_START_TIME_JULIAN = julian.to_jd(GNSS_START_TIME.replace(tzinfo=None), "mjd")
GNSS_START_TIME_JULIAN_BOUNDS = julian.to_jd(
    GNSS_START_TIME.replace(tzinfo=None) + timedelta(days=365 * 500), "mjd"
)
TRIGGER_DELAY_SV2 = 0.1  # SV2 trigger delay in seconds
TRIGGER_DELAY_SV3 = 0.13  # SV3 trigger delay in seconds
ADJ_LEAP = 1.0  # this is the leap second adjustment TODO Ask James why this is there

STATION_OFFSETS = {"5209": 200, "5210": 320, "5211": 440, "5212": 560}
MASTER_STATION_ID = {"0": "5209", "1": "5210", "2": "5211", "3": "5212"}
LEAP_SECONDS = 18  # Leap seconds