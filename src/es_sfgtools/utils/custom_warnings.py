import warnings

# Warning to reduce sample frequency of PRIDE-PPP
class PrideSampleFrequencyWarning(Warning):
    """Warning for when the PRIDE-PPP sample frequency should be reduced."""
    message = "PRIDE-PPP sample frequency is too high. Consider reducing the sample rate."


WARNINGS_DICT = {"input interval is shorter than observation interval": PrideSampleFrequencyWarning}
