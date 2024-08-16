from datetime import datetime, timezone,timedelta
import julian

GNSS_START_TIME = datetime(1980, 1, 6, tzinfo=timezone.utc)  # GNSS start time
GNSS_START_TIME_JULIAN = julian.to_jd(GNSS_START_TIME.replace(tzinfo=None), "mjd")
GNSS_START_TIME_JULIAN_BOUNDS = julian.to_jd(
    GNSS_START_TIME.replace(tzinfo=None) + timedelta(days=365 * 500), "mjd"
)
TRIGGER_DELAY_SV2 = 0.1  # SV2 trigger delay in seconds
TRIGGER_DELAY_SV3 = 0.13  # SV3 trigger delay in seconds
ADJ_LEAP = 1.0  # this is the leap second adjustment TODO Ask James why this is there
