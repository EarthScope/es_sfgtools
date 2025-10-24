"""
This module defines various constants used throughout the application.

These include time references for GNSS, default trigger delays, and station
ID mappings.
"""

from datetime import datetime, timedelta, timezone
from decimal import getcontext

import julian

# Set precision for Decimal operations, though it is not used in this file.
getcontext().prec = 10

# --- Time Constants ---
GNSS_START_TIME = datetime(1980, 1, 6, tzinfo=timezone.utc)  # GNSS start time
GNSS_START_TIME_JULIAN = julian.to_jd(GNSS_START_TIME.replace(tzinfo=None), "mjd")
GNSS_START_TIME_JULIAN_BOUNDS = julian.to_jd(
    GNSS_START_TIME.replace(tzinfo=None) + timedelta(days=365 * 500), "mjd"
)
LEAP_SECONDS = 18  # Leap seconds between GPS time and UTC as of 2024

# --- Instrument Constants ---
TRIGGER_DELAY_SV2 = 0.1  # SV2 trigger delay in seconds
TRIGGER_DELAY_SV3 = 0.13  # SV3 trigger delay in seconds

# TODO: Ask James why this is here and what it represents.
ADJ_LEAP = 1.0  # this is the leap second adjustment

# --- Station Constants ---
STATION_OFFSETS = {"5209": 200, "5210": 320, "5211": 440, "5212": 560}
MASTER_STATION_ID = {"0": "5209", "1": "5210", "2": "5211", "3": "5212"}