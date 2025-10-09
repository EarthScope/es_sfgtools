"""
Author: Franklyn Dunbar
Date: 2024-03-07
Email: franklyn.dunbar@earthscope.org
"""

from decimal import Decimal, getcontext
from typing import Optional

from pydantic import BaseModel

# Set precision for Decimal operations
getcontext().prec = 10



class SV3InterrogationData(BaseModel):
    head0: Decimal
    pitch0: Decimal
    roll0: Decimal
    east0: Decimal
    north0: Decimal
    up0: Decimal
    east_std0: Optional[Decimal] = None
    north_std0: Optional[Decimal] = None
    up_std0: Optional[Decimal] = None
    pingTime: Decimal

class SV3ReplyData(BaseModel):
    head1: Decimal
    pitch1: Decimal
    roll1: Decimal
    east1: Decimal
    north1: Decimal
    up1: Decimal
    transponderID: str
    dbv: Decimal
    snr: Decimal
    xc: Decimal
    tt: Decimal
    tat: Decimal
    returnTime: Decimal
    east_std1: Optional[Decimal] = None
    north_std1: Optional[Decimal] = None
    up_std1: Optional[Decimal] = None