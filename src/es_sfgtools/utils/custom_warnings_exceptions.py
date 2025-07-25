import warnings
import os 
# Warning to reduce sample frequency of PRIDE-PPP
class PrideSampleFrequencyWarning(Warning):
    """Warning for when the PRIDE-PPP sample frequency should be reduced."""
    message = "PRIDE-PPP sample frequency is too high. Consider reducing the sample rate."


WARNINGS_DICT = {"input interval is shorter than observation interval": PrideSampleFrequencyWarning}


class DYLDLibraryException(Exception):
    """Exception raised when the DYLD_LIBRARY_PATH environment variable is not set."""
    def __init__(
        self,
        message="\nLibrary not loaded: @rpath/libtiledb.dylib \nDYLD_LIBRARY_PATH does not include TileDB dylib file. Hint: $ export DYLD_LIBRARY_PATH=$CONDA_PREFIX/lib:$DYLD_LIBRARY_PATH",
    ):
        super().__init__(message)


class LDLibraryException(Exception):
    """Exception raised when the LD_LIBRARY_PATH environment variable is not set."""
    def __init__(
        self,
        message="\nLibrary not loaded: @rpath/libtiledb.dylib \nLD_LIBRARY_PATH does not include TileDB tile.h file. Hint: $ export LD_LIBRARY_PATH=$CONDA_PREFIX/lib:$LD_LIBRARY_PATH",
    ):
        super().__init__(message)


EXCEPTIONS_DICT_LINUX = {"Library not loaded: @rpath/libtiledb.dylib ": LDLibraryException}
EXCEPTIONS_DICT_MACOS = {"Library not loaded: @rpath/libtiledb.dylib ": DYLDLibraryException}
