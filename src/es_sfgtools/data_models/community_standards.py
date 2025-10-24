"""
This module defines data schemas that conform to the GNSS-A Community Data
Standard, as described at https://hal.science/hal-04319233.

It provides Pydantic and Pandera models for representing seafloor acoustic data
and site metadata in a standardized format.
"""

import datetime
from typing import Optional

import pandera as pa
import pymap3d as pm
from pandera.typing import Series
from pydantic import BaseModel

from .metadata import Site, Vessel


class SFGDSTFSeafloorAcousticData(pa.DataFrameModel):
    """
    Pandera model for seafloor acoustic data, as defined by the Seafloor
    Geodesy Data Standardization Task Force (SFGDSTF).
    """

    # Essential pa.Fields
    MT_ID: Series[str] = pa.Field(description="ID of mirror transponder")
    TravelTime: Series[float] = pa.Field(
        description="Observed travel time (net value) [sec.]", ge=0
    )
    T_transmit: Series[float] = pa.Field(
        description="Transmission time of acoustic signal [sec. from origin]", ge=0
    )
    X_transmit: Series[float] = pa.Field(
        description="Transducer position at T_transmit in ECEF [m]"
    )
    Y_transmit: Series[float] = pa.Field(
        description="Transducer position at T_transmit in ECEF [m]"
    )
    Z_transmit: Series[float] = pa.Field(
        description="Transducer position at T_transmit in ECEF [m]"
    )
    T_receive: Series[float] = pa.Field(
        description="Reception time of acoustic signal [sec. from origin]", ge=0
    )
    X_receive: Series[float] = pa.Field(
        description="Transducer position at T_receive in ECEF [m]"
    )
    Y_receive: Series[float] = pa.Field(
        description="Transducer position at T_receive in ECEF [m]"
    )
    Z_receive: Series[float] = pa.Field(
        description="Transducer position at T_receive in ECEF [m]"
    )

    # Optional pa.Fields
    TDC_ID: Optional[Series[str]] = pa.Field(
        default=None, description="ID of the reception transducer"
    )
    aSNR: Optional[Series[float]] = pa.Field(
        default=None, description="Signal/Noise Ratio of the acoustic ping"
    )
    acc: Optional[Series[int]] = pa.Field(
        default=None, description="acoustic Cross Correlation of the acoustic ping"
    )
    dbV: Optional[Series[float]] = pa.Field(
        default=None, description="acoustic Decibel Volt voltage of the acoustic ping"
    )
    quality_flag: Optional[Series[str]] = pa.Field(
        default=None,
        description="String defining the quality of the record",
    )
    trans_sigX0: Optional[Series[float]] = pa.Field(
        default=None, description="Transducer position std at transmit"
    )
    trans_sigY0: Optional[Series[float]] = pa.Field(
        default=None, description="Transducer position std at transmit"
    )
    trans_sigZ0: Optional[Series[float]] = pa.Field(
        default=None, description="Transducer position std at transmit"
    )
    trans_sigX1: Optional[Series[float]] = pa.Field(
        default=None, description="Transducer position std at receive"
    )
    trans_sigY1: Optional[Series[float]] = pa.Field(
        default=None, description="Transducer position std at receive"
    )
    trans_sigZ1: Optional[Series[float]] = pa.Field(
        default=None, description="Transducer position std at receive"
    )

    # GNSS antenna positions and uncertainties
    ant_X0: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS position at T_transmit in ECEF [m]"
    )
    ant_Y0: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS position at T_transmit in ECEF [m]"
    )
    ant_Z0: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS position at T_transmit in ECEF [m]"
    )
    ant_sigX0: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS antenna std at transmit"
    )
    ant_sigY0: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS antenna std at transmit"
    )
    ant_sigZ0: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS antenna std at transmit"
    )
    ant_cov_XY0: Optional[Series[float]] = pa.Field(
        default=None, description="Covariance matrix"
    )
    ant_X1: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS position at T_receive in ECEF [m]"
    )
    ant_Y1: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS position at T_receive in ECEF [m]"
    )
    ant_Z1: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS position at T_receive in ECEF [m]"
    )
    ant_sigX1: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS antenna std at transmit"
    )
    ant_sigY1: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS antenna std at transmit"
    )
    ant_sigZ1: Optional[Series[float]] = pa.Field(
        default=None, description="GNSS antenna std at transmit"
    )

    # Attitude information
    heading0: Optional[Series[float]] = pa.Field(
        default=None,
        description="Heading at T_transmit (in degree from north)",
        ge=0,
        le=360,
    )
    pitch0: Optional[Series[float]] = pa.Field(
        default=None, description="Pitch at T_transmit (in degree)", ge=-90, le=90
    )
    roll0: Optional[Series[float]] = pa.Field(
        default=None, description="Roll at T_transmit (in degree)", ge=-180, le=180
    )
    roll1: Optional[Series[float]] = pa.Field(
        default=None, description="Roll at T_receive (in degree)", ge=-180, le=180
    )

    # Direction of arrival vectors
    doa_R: Optional[Series[float]] = pa.Field(
        default=None,
        description="direction of arrival vector wrt Roll vector direction",
    )
    doa_P: Optional[Series[float]] = pa.Field(
        default=None,
        description="direction of arrival vector wrt Pitch vector direction",
    )
    doa_H: Optional[Series[float]] = pa.Field(
        default=None,
        description="direction of arrival vector wrt Heading vector direction",
    )


class SFGDTSFSite(BaseModel):
    """Pydantic model for site metadata, based on SFGDSTF standards."""

    Site_name: str  # GNSS-A site name or code
    Campaign: str  # Observation campaign name
    TimeOrigin: datetime.datetime  # Origin of time used in the file [UTC]
    RefFrame: str = "ITRF"  # Reference frame used in the file
    MTlist: list[str] = []  # List of ID of mirror transponders
    MT_appPos: dict[str, list[float]] = {}  # Approximate positions of transponders in ECEF[m]
    ATDoffset: list[float] = [
        0.0, 0.0, 0.0
    ]  # Antenna to transponder offset [m] with [forward,rightward,downward]

    @classmethod
    def from_site_vessel(cls, site: Site, vessel: Vessel, campaign_id:str) -> "SFGDTSFSite":
        """
        Create a SFGDTSFSite object from internal Site and Vessel objects.

        Parameters
        ----------
        site : Site
            The Site object containing site metadata.
        vessel : Vessel
            The Vessel object containing vessel metadata.
        campaign_id : str
            The campaign identifier to select the appropriate campaign data.

        Notes
        -----
            This constructor makes several assumptions:
            - The first name in `site.names` is the primary site name.
            - The campaign matching campaign_id is used.
            - The first reference frame is used.
            - The first ATD offset from the vessel is used.

        """
        mt_app_pos = {}
        for benchmark in site.benchmarks:
            east, north, up = pm.geodetic2ecef(
                lat=benchmark.aPrioriLocation.latitude,
                lon=benchmark.aPrioriLocation.longitude,
                alt=benchmark.aPrioriLocation.elevation,
            )
            mt_app_pos[benchmark.benchmarkID] = [east, north, up]

        found_campaign = None
        for camp in site.campaigns:
            if camp.name == campaign_id:
                found_campaign = camp
                break

        if found_campaign is None:
            raise ValueError(f"Campaign ID {campaign_id} not found in site campaigns.")
        
        return cls(
            Site_name=site.names[0],
            Campaign=found_campaign.name,
            TimeOrigin=site.timeOrigin,
            RefFrame=(site.referenceFrames[0].name if site.referenceFrames else "ITRF"),
            MTlist=[b.benchmarkID for b in site.benchmarks],
            MT_appPos=mt_app_pos,
            ATDoffset=[
                vessel.atdOffsets[0].x,
                vessel.atdOffsets[0].y,
                vessel.atdOffsets[0].z,
            ],
        )