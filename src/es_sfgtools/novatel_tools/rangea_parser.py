"""
RANGEA ASCII Log Parser

This module provides Python functions to parse NovAtel RANGEA ASCII log strings
into structured observation data, inspired by the Go-lang GNSS tools implementation using
novatelascii.DeserializeRANGEA and observation.Epoch.

The RANGEA log contains GNSS pseudorange, carrier phase, Doppler, and C/N0
measurements for all tracked satellites across multiple constellations.

"""

import datetime
from enum import IntEnum
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, computed_field


class GNSSSystem(IntEnum):
    """GNSS constellation identifiers from NovAtel channel tracking status."""
    GPS = 0
    GLONASS = 1
    SBAS = 2
    GALILEO = 3
    BEIDOU = 5
    QZSS = 6
    NAVIC = 7


class SignalType(IntEnum):
    """Common GNSS signal types (simplified mapping)."""
    L1CA = 0      # GPS L1 C/A
    L2P = 5       # GPS L2P
    L2C = 9       # GPS L2C
    L5Q = 14      # GPS L5Q
    L1C = 17      # GPS L1C
    E1 = 2        # Galileo E1
    E5A = 12      # Galileo E5a
    E5B = 17      # Galileo E5b
    B1I = 0       # BeiDou B1I
    B2I = 2       # BeiDou B2I
    B3I = 6       # BeiDou B3I


class Observation(BaseModel):
    """
    A single GNSS observation for one signal from one satellite.
    
    This corresponds to a single observation record within a RANGEA message,
    containing pseudorange, carrier phase, Doppler, and signal quality metrics.
    
    """
    signal_type: int = Field(..., title="Signal Type Identifier")
    pseudorange: float = Field(..., title="Pseudorange", description="Pseudorange measurement in meters")
    pseudorange_std: float = Field(..., title="Pseudorange Std", description="Pseudorange standard deviation in meters")
    carrier_phase: float = Field(..., title="Carrier Phase", description="Accumulated Doppler range (ADR) in cycles")
    carrier_phase_std: float = Field(..., title="Carrier Phase Std", description="Carrier phase standard deviation in cycles")
    doppler: float = Field(..., title="Doppler", description="Doppler frequency shift in Hz")
    cn0: float = Field(..., title="C/N0", description="Carrier-to-noise density ratio in dB-Hz")
    locktime: float = Field(..., title="Lock Time", description="Continuous tracking time in seconds")
    tracking_status: int = Field(..., title="Tracking Status", description="Raw 32-bit channel tracking status word")
    half_cycle_ambiguity: bool = Field(default=False, title="Half Cycle Ambiguity", description="True if half-cycle ambiguity is present")
    phase_lock: bool = Field(default=True, title="Phase Lock", description="True if phase is locked")
    code_lock: bool = Field(default=True, title="Code Lock", description="True if code is locked")
    parity_known: bool = Field(default=True, title="Parity Known", description="True if parity is known (for navigation data)")
    pseudorange_std: float = Field(..., title="Pseudorange Std", description="Pseudorange standard deviation in meters")
    carrier_phase: float = Field(..., title="Carrier Phase", description="Accumulated Doppler range (ADR) in cycles")
    carrier_phase_std: float = Field(..., title="Carrier Phase Std", description="Carrier phase standard deviation in cycles")
    doppler: float = Field(..., title="Doppler", description="Doppler frequency shift in Hz")
    cn0: float = Field(..., title="C/N0", description="Carrier-to-noise density ratio in dB-Hz")
    locktime: float = Field(..., title="Lock Time", description="Continuous tracking time in seconds")
    tracking_status: int = Field(..., title="Tracking Status", description="Raw 32-bit channel tracking status word")
    half_cycle_ambiguity: bool = Field(default=False, title="Half Cycle Ambiguity", description="True if half-cycle ambiguity is present")
    phase_lock: bool = Field(default=True, title="Phase Lock", description="True if phase is locked")
    code_lock: bool = Field(default=True, title="Code Lock", description="True if code is locked")
    parity_known: bool = Field(default=True, title="Parity Known", description="True if parity is known (for navigation data)")

    model_config = {"frozen": False}


class Satellite(BaseModel):
    """
    GNSS satellite with all its observations.
    
    A satellite may have multiple observations for different signals
    (e.g., GPS satellite might have L1CA, L2C, and L5 observations).
    
    Attributes:
        system: GNSS constellation (GPS, GLONASS, Galileo, etc.)
        prn: Satellite PRN number (or slot for GLONASS)
        fcn: GLONASS frequency channel number (-7 to +6), 0 for other systems
        observations: Dictionary mapping signal type to Observation
    """
    system: GNSSSystem
    prn: int
    fcn: int = 0
    observations: Dict[int, Observation] = Field(default_factory=dict)

    model_config = {"frozen": False}
    
    def add_observation(self, obs: Observation) -> None:
        """Add an observation for a specific signal type."""
        self.observations[obs.signal_type] = obs


class GNSSEpoch(BaseModel):
    """
    A GNSS observation epoch containing all satellite measurements at one time.
    
    This is the Python equivalent of Go's observation.Epoch structure.
    An epoch represents all GNSS observations recorded at a single instant,
    typically at the receiver's measurement rate (e.g., 1 Hz, 10 Hz).
    
    Attributes:
        time: UTC timestamp of the epoch
        gps_week: GPS week number
        gps_seconds: Seconds into the GPS week
        satellites: Dictionary mapping (system, prn) tuple to Satellite
        receiver_status: Raw receiver status word from header
        num_observations: Total number of observation records
    """
    time: datetime.datetime
    gps_week: int
    gps_seconds: float
    satellites: Dict[Tuple[int, int], Satellite] = Field(default_factory=dict)
    receiver_status: str = ""
    num_observations: int = 0

    model_config = {"frozen": False}
    
    def add_satellite(self, sat: Satellite) -> None:
        """Add or update a satellite in this epoch."""
        key = (int(sat.system), sat.prn)
        if key in self.satellites:
            # Merge observations
            self.satellites[key].observations.update(sat.observations)
        else:
            self.satellites[key] = sat
    
    def get_satellite(self, system: GNSSSystem, prn: int) -> Optional[Satellite]:
        """Get a satellite by system and PRN."""
        return self.satellites.get((int(system), prn))
    
    @computed_field
    @property
    def satellite_count(self) -> int:
        """Return the number of unique satellites in this epoch."""
        return len(self.satellites)
    
    def get_systems(self) -> List[GNSSSystem]:
        """Return list of GNSS systems present in this epoch."""
        systems = set(sys for sys, _ in self.satellites.keys())
        return [GNSSSystem(s) for s in systems]


# GPS epoch: January 6, 1980 00:00:00 UTC
GPS_EPOCH = datetime.datetime(1980, 1, 6, 0, 0, 0, tzinfo=datetime.timezone.utc)
GPS_LEAP_SECONDS = 18  # Current GPS-UTC leap seconds offset (as of 2017)


def _decode_channel_tracking_status(status: int) -> dict:
    """
    Decode the 32-bit NovAtel channel tracking status word.
    
    The channel tracking status encodes information about the signal being
    tracked, including the GNSS system, signal type, and lock status.
    
    Bit layout:
        0-4:   Tracking state (0=idle, 3=code lock, 4=freq lock pull-in, ...)
        5-9:   SV channel number
        10-12: Phase lock flag
        13:    Parity known flag
        14:    Code lock flag
        15:    Reserved
        16-20: Satellite system (0=GPS, 1=GLONASS, 2=SBAS, 3=Galileo, 5=BeiDou, 6=QZSS)
        21-25: Signal type
        26-27: Reserved
        28:    Half-cycle added flag
        29:    Reserved
        30:    Digital filtering
        31:    PRN lock flag
    
    Args:
        status: 32-bit channel tracking status integer
        
    Returns:
        Dictionary with decoded fields
    """
    return {
        'tracking_state': status & 0x1F,
        'sv_channel': (status >> 5) & 0x1F,
        'phase_lock_flag': (status >> 10) & 0x07,
        'parity_known': bool((status >> 13) & 0x01),
        'code_lock': bool((status >> 14) & 0x01),
        'system': (status >> 16) & 0x1F,
        'signal_type': (status >> 21) & 0x1F,
        'half_cycle_added': bool((status >> 28) & 0x01),
        'prn_lock': bool((status >> 31) & 0x01),
    }


def _parse_header(header_str: str) -> Tuple[int, float, str]:
    """
    Parse the RANGEA message header to extract GPS time.
    
    Header format (long message):
        #RANGEA,port,sequence,idle_time,time_status,gps_week,gps_seconds,
        receiver_status,reserved,sw_version
    
    Args:
        header_str: The header portion of the RANGEA message (before semicolon)
        
    Returns:
        Tuple of (gps_week, gps_seconds, receiver_status)
        
    Raises:
        ValueError: If header cannot be parsed
    """
    fields = header_str.split(',')
    
    # Find GPS week and seconds - they're after time_status field
    # The time_status is typically a string like "FINESTEERING" or "COARSESTEERING"
    gps_week = None
    gps_seconds = None
    receiver_status = ""
    
    for i, f in enumerate(fields):
        # GPS week is typically in the range 2000-3000
        if f.isdigit() and 2000 <= int(f) <= 3000:
            gps_week = int(f)
            # Next field should be GPS seconds
            if i + 1 < len(fields):
                try:
                    gps_seconds = float(fields[i + 1])
                    # Receiver status is typically 2 fields after
                    if i + 2 < len(fields):
                        receiver_status = fields[i + 2]
                    break
                except ValueError:
                    continue
    
    if gps_week is None or gps_seconds is None:
        raise ValueError(f"Could not parse GPS time from header: {header_str}")
    
    return gps_week, gps_seconds, receiver_status


def _gps_to_utc(gps_week: int, gps_seconds: float, leap_seconds: int = GPS_LEAP_SECONDS) -> datetime.datetime:
    """
    Convert GPS week and seconds to UTC datetime.
    
    Args:
        gps_week: GPS week number
        gps_seconds: Seconds into the GPS week
        leap_seconds: GPS-UTC leap second offset
        
    Returns:
        UTC datetime with timezone info
    """
    total_seconds = gps_week * 604800 + gps_seconds - leap_seconds
    return GPS_EPOCH + datetime.timedelta(seconds=total_seconds)


def deserialize_rangea(rangea_string: str) -> GNSSEpoch:
    """
    Parse a NovAtel RANGEA ASCII log string into an Epoch object.
    
    This function is the Python equivalent of the Go code:
        rangea, err := novatelascii.DeserializeRANGEA(m.Data)
        epoch, err := rangea.SerializeGNSSEpoch(m.Time())
    
    RANGEA Format:
        #RANGEA,<header>;num_obs,<obs1>,...,<obsN>*checksum
        
    Each observation has 10 fields:
        prn, glo_freq, psr, psr_std, adr, adr_std, dopp, cn0, locktime, ch_tr_status
    
    Args:
        rangea_string: Complete RANGEA ASCII log string including header and checksum
        
    Returns:
        Epoch object containing all parsed satellite observations
        
    Raises:
        ValueError: If the string cannot be parsed as a valid RANGEA message
        
    Example:
        >>> rangea = "#RANGEA,USB2,0,73.5,FINESTEERING,2379,414835.000,..."
        >>> epoch = deserialize_rangea(rangea)
        >>> print(f"Epoch time: {epoch.time}, satellites: {epoch.satellite_count}")
    """
    if not rangea_string or "#RANGEA" not in rangea_string:
        raise ValueError("Invalid RANGEA string: missing #RANGEA header")
    
    # Remove checksum if present
    if '*' in rangea_string:
        rangea_string = rangea_string.split('*')[0]
    
    # Split header and data at semicolon
    parts = rangea_string.split(';')
    if len(parts) != 2:
        raise ValueError("Invalid RANGEA string: missing semicolon separator")
    
    header_part = parts[0]
    data_part = parts[1]
    
    # Parse header
    gps_week, gps_seconds, receiver_status = _parse_header(header_part)
    utc_time = _gps_to_utc(gps_week, gps_seconds)
    
    # Create epoch
    epoch = GNSSEpoch(
        time=utc_time,
        gps_week=gps_week,
        gps_seconds=gps_seconds,
        receiver_status=receiver_status,
    )
    
    # Parse observation data
    data_fields = data_part.split(',')
    num_obs = int(data_fields[0])
    epoch.num_observations = num_obs
    
    idx = 1  # Start after num_obs field
    fields_per_obs = 10
    
    for obs_idx in range(num_obs):
        if idx + fields_per_obs > len(data_fields):
            break
        
        try:
            prn = int(data_fields[idx])
            glo_freq = int(data_fields[idx + 1])
            psr = float(data_fields[idx + 2])
            psr_std = float(data_fields[idx + 3])
            adr = float(data_fields[idx + 4])
            adr_std = float(data_fields[idx + 5])
            doppler = float(data_fields[idx + 6])
            cn0 = float(data_fields[idx + 7])
            locktime = float(data_fields[idx + 8])
            ch_tr_status = int(data_fields[idx + 9], 16)
            
            # Decode channel tracking status
            status = _decode_channel_tracking_status(ch_tr_status)
            
            # Get system and signal type
            system = GNSSSystem(status['system']) if status['system'] in [e.value for e in GNSSSystem] else GNSSSystem.GPS
            signal_type = status['signal_type']
            
            # Create observation
            obs = Observation(
                signal_type=signal_type,
                pseudorange=psr,
                pseudorange_std=psr_std,
                carrier_phase=adr,
                carrier_phase_std=adr_std,
                doppler=doppler,
                cn0=cn0,
                locktime=locktime,
                tracking_status=ch_tr_status,
                half_cycle_ambiguity=status['half_cycle_added'],
                phase_lock=status['phase_lock_flag'] >= 3,
                code_lock=status['code_lock'],
                parity_known=status['parity_known'],
            )
            
            # Get or create satellite
            sat_key = (int(system), prn)
            if sat_key not in epoch.satellites:
                fcn = glo_freq if system == GNSSSystem.GLONASS else 0
                sat = Satellite(system=system, prn=prn, fcn=fcn)
                epoch.satellites[sat_key] = sat
            
            # Add observation to satellite
            epoch.satellites[sat_key].add_observation(obs)
            
        except (ValueError, IndexError) as e:
            # Skip malformed observations but continue parsing
            pass
        
        idx += fields_per_obs
    
    return epoch


def epoch_to_dict(epoch: GNSSEpoch) -> dict:
    """
    Convert an Epoch object to a dictionary for serialization.
    
    Args:
        epoch: Epoch object to convert
        
    Returns:
        Dictionary representation suitable for JSON serialization
    """
    return {
        'time': epoch.time.isoformat(),
        'gps_week': epoch.gps_week,
        'gps_seconds': epoch.gps_seconds,
        'receiver_status': epoch.receiver_status,
        'num_observations': epoch.num_observations,
        'satellite_count': epoch.satellite_count,
        'satellites': [
            {
                'system': sat.system.name,
                'prn': sat.prn,
                'fcn': sat.fcn,
                'observations': [
                    {
                        'signal_type': obs.signal_type,
                        'pseudorange': obs.pseudorange,
                        'carrier_phase': obs.carrier_phase,
                        'doppler': obs.doppler,
                        'cn0': obs.cn0,
                        'locktime': obs.locktime,
                    }
                    for obs in sat.observations.values()
                ]
            }
            for sat in epoch.satellites.values()
        ]
    }
