class NoNovatelFound(Exception):
    """Custom exception raised when no Novatel files are found for processing."""


class NoRinexBuilt(Exception):
    """Custom exception raised when no RINEX files are built for processing."""

    pass


class NoRinexFound(Exception):
    """Custom exception raised when no RINEX files are found for processing."""

    pass


class NoKinFound(Exception):
    """Custom exception raised when no KIN files are found for processing."""

    pass


class NoDFOP00Found(Exception):
    """Custom exception raised when no DFOP00 files are found for processing."""

    pass


class NoSVPFound(Exception):
    """Custom exception raised when no SVP files are found for processing."""

    pass

class NoLocalData(Exception):
    """Custom exception raised when no data is ingested for processing."""

    pass