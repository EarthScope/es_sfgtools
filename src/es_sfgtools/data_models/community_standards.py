import pandera as pa
from pandera.typing import Series
from typing import Optional


class SeafloorAcousticData(pa.DataFrameModel):
    """
    Data frame model of seafloor acoustic data defined by the Seafloor Geodesy Data Standardization Task Force (SFGDSTF)
    """
    # Essential pa.Fields
    MT_ID: Series[str] = pa.Field(description="ID of mirror transponder", max_length=50)
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
        default=None, description="ID of the reception transducer", max_length=50
    )
    aSNR: Optional[Series[float]] = pa.Field(
        default=None, description="Signal/Noise Ratio of the acoustic ping"
    )
    acc: Optional[Series[float]] = pa.Field(
        default=None, description="acoustic Cross Correlation of the acoustic ping"
    )
    dbV: Optional[Series[float]] = pa.Field(
        default=None, description="acoustic Decibel Volt voltage of the acoustic ping"
    )
    quality_flag: Optional[Series[str]] = pa.Field(
        default=None,
        description="Series[str]ing defining the quality of the record",
        max_length=10,
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

    # Additional optional pa.Fields can be added as needed for [other] category
