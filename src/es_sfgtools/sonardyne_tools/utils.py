from datetime import timedelta
from ..data_models.constants import GNSS_START_TIME,LEAP_SECONDS

class InstrumentTimeRectifier:
    # Convert common time to gps time in sondardyne records
    # gpsTime_i - gpsTime_0 = commonTimeFalse_i - commonTimeFalse_0
    # commonTimeFalse_i = commonTimeFalse_0 + (gpsTime_i - gpsTime_0)
    # commonTimeTrue_i = commonTimeFalse_i - commonTimeTrueOffset
    # commonTimeTrueOffset = gpsTime_0 + GNSS_START_TIME + LEAP_SECONDS - commonTimeFalse_0
    # commonTimeTrue_i = commonTimeFalse_i - commonTimeTrueOffset
    def __init__(self,gpsTime:float,instrumentTime:float,unixTime:float):
        """
        Initialize the InstrumentTimeRectifier with GPS time, instrument time, and Unix time.

        Args:
            gpsTime (float): GPS time in seconds since the GNSS epoch.
            instrumentTime (float): Instrument time in seconds since the GNSS epoch.
            unixTime (float): Corresponding Unix time in seconds.
        """
        self.gpsTime = gpsTime
        self.instrumentTime = instrumentTime
        self.unixTimeOrigin = unixTime
        self.timeOriginUTC = self._gpstime_to_unixutc(gpsTime)
        self.commonTimeTrueOffset = self.timeOriginUTC - self.unixTimeOrigin

        # TrueTime = RecordCommonTime - CommonTimeTrueOffset
    def _gpstime_to_unixutc(self,gpsTime:float) -> float:
        """
        Convert GPS time to Unix UTC time.

        Args:
            gpsTime (float): GPS time in seconds since the GNSS epoch.

        Returns:
            float: Corresponding Unix UTC time in seconds.
        """
        unix_time = (gpsTime + LEAP_SECONDS) + GNSS_START_TIME.timestamp()
        return unix_time

    def rectifyCommonTime(self,commonTime:float) -> float:
        """
        Rectify the common time using the true offset.

        Args:
            commonTime (float): Common time in seconds since the GNSS epoch.

        Returns:
            float: Rectified common time in seconds since the GNSS epoch.
        """
        return commonTime - self.commonTimeTrueOffset