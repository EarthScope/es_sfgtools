from datetime import timedelta
from ..data_models.constants import GNSS_START_TIME,LEAP_SECONDS

class InstrumentTimeRectifier:
    def __init__(self,gpsTime:float,instrumentTime:float):
        """
        Initialize the InstrumentTimeRectifier with GPS time, instrument time, and Unix time.

        Args:
            gpsTime (float): GPS time in seconds since the GNSS epoch.
            instrumentTime (float): Instrument time in seconds since the GNSS epoch.
        """
        self.gpsTime0 = gpsTime
        self.instrumentTime0 = instrumentTime

        # TrueTime = RecordCommonTime - CommonTimeTrueOffset
    def instrumentTime2GPSTime(self, instrumentTime: float) -> float:
        """
        Convert instrument time to GPS time.

        Args:
            instrumentTime (float): Instrument time in seconds since the GNSS epoch.

        Returns:
            float: Corresponding GPS time in seconds since the GNSS epoch.
        """
        return instrumentTime - self.instrumentTime0 + self.gpsTime0
    